# Decisions, Learnings & Anti-Patterns

A living record of **why this project is built the way it is**, **what we tried and
rejected**, and **the gotchas we hit** — so we never re-walk a path we already
redirected from, and never re-make a mistake we already paid for.

> If you're about to change something here, read the relevant section first.
> Most of these were learned the hard way (and verified live against REW).

---

## 1. Goal

Expose **three** SPL values from REW (Room EQ Wizard) to **Bitfocus Companion**
over HTTP/JSON:

| Value | Source |
|---|---|
| **SPL A Slow** | REW native (`spl`) |
| **2-min Leq** | **Computed** by the bridge (REW has no native 2-min) |
| **15-min Leq** | REW native rolling Leq (`leq`, `rollingLeqMinutes: 15`) |

Companion runs on a **separate PC**, so the data must be reachable across the LAN.

---

## 2. Architecture decisions (and the paths we rejected)

### ❌ Open Sound Meter (OSM) instead of REW — REJECTED
Investigated thoroughly. OSM's broadcast/API exposes **only Fast/Slow instantaneous
SPL** — **no Leq of any window**. Proven from OSM's own source: the meter is built
only for `{Fast, Slow}` and the shared `LevelsData` struct is keyed `{weighting,
Fast|Slow}` with no Leq dimension. The bundled Companion OSM module has zero Leq
variables. **OSM cannot deliver 2-min or 15-min Leq. Do not revisit OSM for this.**

### ✅ REW + a bridge — CHOSEN
REW natively does Leq with a configurable rolling window, and has an HTTP API.
A bridge is required because:
- REW's API is intended for **localhost**; Companion is on another PC.
- REW has **no native 2-min** Leq (its rolling window is single — we spend it on 15-min).
- We want auto-launch/supervision of REW and a clean status surface.

### ✅ Polling, not subscriptions — CHOSEN
REW supports subscriptions (it POSTs to a callback URL), but:
- The Subscriber object's field is **`url`**, not `callbackUrl` (we had this wrong).
- Subscriptions **self-cancel** if REW ever misses a 200 from our callback — fragile.
- Polling `GET /spl-meter/1/levels` (~5 Hz) is simpler, has no inbound server, and
  no cancellation failure mode. **Do not go back to subscriptions.**

### ✅ 2-min reconstructed from REW's native 1-min Leq — CHOSEN
The 2-min Leq is the **energy average of two contiguous 1-min Leqs**: the current
`leq1m` and the `leq1m` from ~60 s ago. This is exact and **independent of the poll
rate**.

### ❌ 2-min by energy-averaging buffered "Slow" SPL — REJECTED
The original approach buffered Slow SPL into a fixed **1200-sample** deque assuming
10 Hz. REW actually updates at **~5.5 Hz**, so 1200 samples was **~3.6 min**, not 2.
Re-averaging already-smoothed Slow SPL is also less accurate. **Do not reintroduce a
sample-count buffer; never assume a fixed update rate.**

### ✅ REW GUI shown by default — CHOSEN
`rew_gui: true` by default so volunteers can see input levels / device selection /
errors. Headless is still available via the tray toggle or config.

---

## 3. REW API — verified facts & gotchas

Source of truth: REW's OpenAPI spec at `http://localhost:4735/doc.json` (Swagger 2.0,
"REW REST API"), cross-checked against a live instance.

- **The REST API is BETA-only (as of 2026-06-04).** The API is only present in REW
  **beta** builds, not the current stable release. The venue PC must run a REW beta
  for any of this to work. Re-check when REW promotes the API to stable — at that
  point this note (and the README prerequisite) can be relaxed.

- **Readiness:** `GET /application` returns **404** — it is not a valid endpoint.
  Use `GET /spl-meter/1/levels` (200) to detect the API is up. (Old code polled
  `/application` and never became "ready".)
- **Commands are capitalised:** SPL meter `Start` / `Stop` (also `Reset`, `Open`,
  `Close`, `Calibrate`); application **`Shutdown`** (not `shutdown`). Read the exact
  lists from `GET /spl-meter/commands` and `GET /application/commands`.
- **`rollingLeqMinutes` type differs by direction:** it is an **integer** in the
  config request body, but a **float/double** in the `/levels` response. Our
  `SPLValues` model types it as `float`.
- **POST may answer 200 or 202** — accept both as success.
- **Subscriber field is `url`** (not `callbackUrl`).
- **`NaN` levels:** REW returns `NaN` for `spl`/`leq`/… when there is **no audio
  capture at all** (no samples). `NaN` is invalid JSON — the bridge sanitises any
  non-finite value to `null` before serving. (Silence on a *real* input reads as a
  low dB like `-180`, not `NaN`.)
- **Weightings are `A`, `C`, `Z`** (no `B`).
- **`Start` semantics (observed, not formally confirmed):** `Start` **revives a
  stopped/stalled meter and resets it to elapsed 0**; on an **already-running,
  healthy** meter it appears to be a **no-op** (does not reset). Good for us:
  reconnecting won't wipe a running measurement, and a dead meter still gets revived.
  *TODO: confirm exact semantics if it ever matters.*

---

## 4. REW's SPL meter STOPS on its own — the big operational learning

**REW's SPL meter clock only advances while audio frames arrive.** If the input
stops delivering samples, the meter **freezes** (`elapsedTime` stops, last values
stick) — with **no error logged** by REW.

- Confirmed live: a meter ran fine for ~12 min, then froze at `-180` after the input
  (a **VB-Audio Virtual Cable**) went idle when its source stopped. REW's
  `/application/errors` and `/warnings` were **empty** — no limit, no crash.
- A **virtual audio cable** stops delivering frames when the app feeding it
  stops/pauses. A **real audio interface** delivers a continuous stream even during
  silence (silence = low dB, clock keeps ticking), so it won't freeze the same way.
- **For testing:** keep source audio playing continuously, or use a real input.
- **For production:** use a continuous live input (measurement mic on an interface,
  or a console/matrix feed).

### Auto-recovery — removed once, then re-added (do NOT remove again)
We first added auto-recovery, then **removed** it on the assumption the early stop
was a one-off audio-config issue. **That assumption was wrong** — the meter
demonstrably stops mid-run during normal operation. Auto-recovery is now back and is
**required**: when the meter *should* be running (`meter_should_run`) but
`elapsedTime` has gone stale, the poll loop re-issues `Start` (throttled by
`METER_RESTART_RETRY`, gated so a deliberate Companion `stop` is respected).
Validated live: a forced stall recovered automatically in **~2 s**.
**Lesson: a silently-frozen meter showing a falsely-low value is dangerous for live
SPL monitoring. Never ship without auto-recovery.**

---

## 5. Packaging / installer gotchas

- **🔴 Windowed (`--noconsole`) builds have `sys.stdout`/`sys.stderr == None` — this
  silently broke server startup (v0.4.0).** uvicorn's log formatter does
  `sys.stdout.isatty()`, so **`uvicorn.Config(...)` raised `AttributeError`** during
  `start_server()`. It ran in pystray's setup thread, which **swallows exceptions**, so
  the installed exe just sat there: tray icon up, no server on :8080, nothing past
  `Starting ... tray application` in the log. It only ever showed up in the *installed*
  exe (source runs have a real console), which is why it slipped through. **Fix
  (v0.4.1):** at the top of `rew_bridge.py`, redirect `sys.stdout`/`sys.stderr` to
  `os.devnull` when they're `None`, before anything touches them. Also wrapped
  `on_setup()` in try/except that `logger.exception(...)`s, so a startup crash can never
  again be invisible. **Reproduce without building:** run `pythonw tray_app.py` (pythonw
  also has `stdout=None`). Don't remove the devnull guard.
- **Quit must wait for graceful shutdown, or REW is orphaned.** The tray runs uvicorn
  in a **daemon** thread; the lifespan shutdown (which closes REW) runs there. If the
  main thread exits immediately after `icon.stop()`, that daemon thread is killed
  mid-shutdown and **REW keeps running**. Fix: store the server thread and **join it**
  before releasing the icon. (Found live; the log showed Quit with REW still alive.)
- **The installer's "Launch at end" hangs.** Launching the tray app from the
  **elevated** installer via `runasoriginaluser` hangs pystray's tray-icon init (no
  server, no REW). The app is fine when launched **normally** (Start Menu / desktop
  shortcut / autostart-on-login). The post-install auto-launch was **removed**; users
  launch from the shortcut. (Do not re-add a naive `[Run]` launch of the exe.)
- **Installs to `Program Files (x86)`** because the installer isn't marked 64-bit.
  Cosmetic (the 64-bit exe runs fine there). *Deferred fix:* add
  `ArchitecturesInstallIn64BitMode` — left out for now to avoid build risk.
- **Config lives in `%LOCALAPPDATA%\REW SPL Bridge\`** for frozen builds (not Program
  Files — avoids permission issues). `config.json` is gitignored; `config.example.json`
  is the tracked template. The installer's `CreateDefaultConfig` only writes if the
  file is absent, so it never clobbers an existing user config.
- **Autostart is OFF by default** (deliberate, per request). It's a checkbox in the
  installer (`Start automatically when Windows starts` → HKCU `Run` entry). It runs
  **at login** (the tray app needs a user session), not as a pre-login service.

---

## 6. Logging (don't let it churn or fill disk)

- Log is a **`RotatingFileHandler`, 1 MB × 3 backups (~4 MB max)** — bounded forever,
  cannot fill the disk over weeks/months.
- **`httpx`/`httpcore` are silenced to WARNING** and uvicorn `access_log=False`.
  Without this, polling at ~5 Hz logged a line per request and churned the bounded log
  to ~2 hours of useful history. **Keep these quiet.**

---

## 7. Multi-day / multi-week operation

**The bridge is built for continuous operation:** bounded log + bounded memory
(fixed ~130 s Leq history, reused HTTP client, steady poll loop) + auto-recovery +
auto-reconnect (loses REW → re-establishes + reconfigures).

**Self-healing failure tiers (all validated live):**
1. **Meter stall** (REW alive, clock frozen) → re-issue `Start` (~2 s).
2. **API blip** (briefly unreachable) → auto-reconnect + reconfigure.
3. **Full REW crash** (process dies) → **relaunch a fresh REW** after ~20 s of being
   unreachable, throttled and process-death guarded (v0.3.2). Validated by killing
   `roomeqwizard.exe`: recovered in ~28 s (20 s grace + REW boot).

So a 24/7 deployment self-heals from a stalled input, a flaky API, and an outright
crash, with no human intervention.

**OS / deployment (not code):** enable Windows **auto-login** (tray app needs a
session after reboot), **defer Windows Update reboots** (active hours), **disable
sleep/hibernate**, prefer a **dedicated machine**.

**Safety net (recommended):** have Companion watch `measurement_active` and
`seconds_since_update` (from `/health` or `/api/spl`) and visibly alert if the data
goes stale — a human backstop over auto-recovery.

---

## 7d. Status window (v0.4.3)

- A **Companion-style status window** (`status_window.py`) **opens at launch** and on
  every restart: REW status dot, the dashboard address, a **live log tail**, and action
  buttons. Closing it (X) **hides to tray**; **Quit** stops the whole app. Born from the
  v0.4.0 silent-failure incident — a visible log pane means a startup problem is *seen*,
  not guessed.
- **Threading (do not "simplify" this):** pystray owns the **main thread**, so the window
  runs tkinter's `mainloop()` in **its own thread**. **All Tk calls happen in that
  thread.** Other threads never touch Tk widgets — they set a `threading.Event`
  (`_show_requested`) that the window's `after()`-driven refresh reads. Status + log
  updates are pulled by that 1 s refresh (reads `tray.connected`, tails `LOG_FILE`
  incrementally by byte offset, handles rotation). Cross-thread Tk calls = random
  crashes; keep the boundary.
- The window is **non-essential**: it's opened inside a `try/except` in `on_setup`, and
  its thread wraps everything in `try/except` logging. If tkinter ever fails (e.g. a
  frozen-bundle issue), the **server still runs** — the window can't take the app down.
- `tkinter` (and `scrolledtext`/`messagebox`/`simpledialog`) are listed in
  `rew_bridge.spec` `hiddenimports` so PyInstaller reliably bundles the tcl/tk runtime
  (the imports are function-level). Widget `padx`/`pady` take a **single** screen
  distance — a `(a, b)` tuple is only valid in `.pack()`/`.grid()`, not the widget
  constructor (this bit us once: `TclError: bad screen distance "0 12"`).

---

## 7b. Web dashboard, thresholds & max (v0.3.3)

- The bridge serves a **responsive web dashboard at `GET /`** (`dashboard.py`). HTML is
  an **embedded string** served via `HTMLResponse` — deliberately *not* a bundled file,
  to avoid any PyInstaller frozen-path resolution risk. Vanilla HTML/CSS/JS, **no CDN**
  (works on an isolated LAN). Responsive via CSS Grid `auto-fit`/`minmax` + `clamp()`
  fonts (1 column on mobile → multi-column on desktop). Polls `/api/spl` same-origin
  (no CORS).
- **Three panels only** (SPL A Slow, 2-min Leq, 15-min Leq) — the values we actually
  produce. No SPL A Fast (a REW meter is Slow *or* Fast; simultaneous needs REW Pro).
- **Thresholds are 3-level (green/orange/red), per panel, and editable from the web UI**,
  persisted to `config.json` via `POST /api/config` → `save_config()` (survives reboot).
  Validated server-side (`validate_thresholds`: numeric, in range, `orange ≤ red`,
  `null` allowed). `GET /api/config` returns them. Defaults: Slow 98/100, 15-min 94/95,
  2-min none.
- **Max is bridge-tracked** per panel (`SPLState.maxes`), updated every poll, exposed as
  `max_*` in `/api/spl`. **Not** reset by auto-recovery (true session-max); reset via
  `POST /api/control {"action":"reset_max"}` (and on shutdown).
- `POST /api/config` and `/api/control` are **unauthenticated** (same trust model as the
  rest — fine on an isolated show LAN).

---

## 7c. Companion integration (Route A) — v0.4.0

Full operator guide lives in **`COMPANION_SETUP.md`**. Key design decisions:

- **The bridge computes the colour, not Companion.** `/api/spl` now returns
  `spl_a_slow_color` / `leq_2min_color` / `leq_15min_color`, each one of
  `green` / `orange` / `red` / `neutral` / `stale`, from the same `panel_color()`
  logic the dashboard uses. **Why:** keeps the thresholds a *single source of truth*
  (editable only in the web UI, persisted on the bridge) and makes Companion trivial —
  a plain "Variable: Check value == red" feedback, no dB math duplicated in Companion.
  Change a limit in the web UI → both dashboard *and* Companion buttons follow.
- **`data_stale` + `seconds_since_update`** added to `/api/spl` so a Companion alert
  button can light up on a stalled meter without a second `/health` poll.
- **Panel values rounded to 0.1 dB in `/api/spl`** (`spl_a_slow`, `leq_15min`; 2-min
  was already rounded) so Companion can display them directly — no expression-side
  formatting. Dashboard already did `toFixed(1)`, so no visible change there.
- **Per-panel max reset.** `POST /api/control` `reset_max` now takes an optional
  `"panel"` (one of `PANEL_KEYS`); omit it to reset all. Dashboard's button still
  sends no panel (= reset all); Companion gets one reset button per panel.
- **Companion mechanics that actually work (verified against module source + docs):**
  - Generic HTTP **GET** action stores the body via the **"JSON Response Data Variable"**
    option (with **"JSON Stringify Result" ON**) — the module does *not* do per-field
    JSONPath itself.
  - Split fields with Companion's built-in **`jsonpath($(custom:rew_raw), '$.field')`**
    expression inside **"Set custom variable value to expression"** internal actions.
    (Avoids the flaky generic-http "set from stored JSONresult via JSONpath" action that
    has come and gone across versions — GitHub issues #56/#2388.)
  - Colour via the **"Variable: Check value"** internal feedback (reliable). Prefer it
    over "Check boolean expression" which had an update-lag bug in ≤3.5 (issue #3386).
  - Poll via a **Time interval trigger** (1 s; 0.5 s fine). No subscriptions needed.
- **Still unauthenticated** — same accepted isolated-LAN trust model.

---

## 8. Build & release

- **PyInstaller** (`rew_bridge.spec`, one-folder, entry `tray_app.py`) → **Inno Setup**
  (`installer.iss`) → published as a **GitHub Release** by CI on a `vX.Y.Z` tag push.
- Tagging is the "build": `git tag vX.Y.Z && git push origin vX.Y.Z`.
- **`app_icon.ico` is generated** (`generate_icon.py`) at build time; gitignored.

### Release checklist — TEST LOCALLY BEFORE TAGGING
Live testing caught the **Quit-orphans-REW** and **installer-launch-hang** bugs that a
green build would have shipped. Before each tag, run the **built exe** and confirm:
1. Build succeeds (no missing hidden imports).
2. Frozen exe launches (no import crash) — launch it **normally**, not from an
   elevated context.
3. REW launches (GUI by default), connects, **auto-starts** metering.
4. `/api/spl` returns valid JSON with real audio; **`leq_2min` populates** after
   ~2 min (`valid_2min` flips true).
5. **Quit closes REW** too (check `roomeqwizard` is gone).
6. **Stall → auto-recovery:** stop REW's meter (`POST /spl-meter/1/command
   {"command":"Stop"}`) and confirm the bridge restarts it within a few seconds.

---

## 9. Tooling notes

- **Pushing needs no `gh` CLI** — Windows Credential Manager is configured; plain
  `git push` authenticates. Commit identity: `albin-user` /
  `<id>+albin-user@users.noreply.github.com` (no-reply, set locally per-repo).
- Verify write auth non-destructively with
  `GIT_TERMINAL_PROMPT=0 git -c credential.interactive=false push --dry-run origin main`
  (don't confuse a public-repo **read** succeeding with **write** working).
