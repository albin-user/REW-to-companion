# Bitfocus Companion Setup — Step by Step

This guide builds a Companion page with:

- **3 live value buttons** — SPL A Slow, 2‑min Leq, 15‑min Leq
- **Green / orange / red** button colours that **exactly match the web dashboard**
  (the limits live only on the bridge — change them in the web UI and Companion
  follows automatically, nothing to edit here)
- **Max** readout per panel + **Reset Max** buttons (per panel and "reset all")
- A **NO‑SIGNAL alert** button that lights up red when the meter stalls or the
  bridge goes offline

It works on Companion **3.x and 4.x**. No coding — you paste a few short
expressions that are provided verbatim below.

---

## 0. Before you start

1. The bridge must be running on the venue PC (tray icon green). Note its
   **IP and port** — they're shown right in the tray menu:
   `Open Dashboard  (10.99.50.92:8080)`. In this guide we use
   **`10.99.50.92:8080`** — replace it with yours everywhere.
2. From the Companion PC, open `http://10.99.50.92:8080/` in a browser once to
   confirm the dashboard loads. If it does, Companion can reach it too.
3. Open the Companion admin GUI (usually `http://<companion-ip>:8000`).

> The bridge must be **v0.4.0 or newer** for the colour/stale fields used here.
> Check `http://10.99.50.92:8080/api/spl` in a browser — you should see
> `spl_a_slow_color`, `leq_15min_color`, `data_stale`, etc. in the JSON.

### What the bridge gives us (`GET /api/spl`)

```json
{
  "spl_a_slow": 99.3,            "spl_a_slow_color": "orange",
  "leq_2min": 96.1,             "leq_2min_color": "neutral",
  "leq_15min": 95.4,            "leq_15min_color": "red",
  "max_spl_a_slow": 101.2,
  "max_leq_2min": 97.0,
  "max_leq_15min": 96.0,
  "measurement_active": true,
  "data_stale": false,
  "seconds_since_update": 0.2
}
```

`*_color` is already computed by the bridge from your dashboard limits and is one
of: `green`, `orange`, `red`, `neutral` (no limit set, e.g. 2‑min), or `stale`
(no signal). Companion just reads that string — it never needs to know the dB
limits.

---

## 1. Add the bridge as a connection

1. Top menu → **Connections** → **Add connection**.
2. Search for **`Generic HTTP`** (module *“Generic: HTTP requests”*). Click **Add**.
3. Give it a label, e.g. **`rew`**.
4. In **Base URL**, enter:
   ```
   http://10.99.50.92:8080
   ```
5. Leave the rest default. **Save**. The connection should show **OK / Connected**.

> Every action below uses a path like `/api/spl`; the Base URL is prepended
> automatically.

---

## 2. Create the custom variables

These hold the split‑out values so buttons can show them with no formatting.

1. Top menu → **Variables** → **Custom Variables**.
2. Create each of these (button **+ Add**). Default value can be blank:

   | Variable name | Holds |
   |---|---|
   | `rew_raw`     | the whole JSON response (filled by the poll) |
   | `spl_now`     | SPL A Slow value |
   | `spl_color`   | SPL colour (`green`/`orange`/`red`/…) |
   | `spl_max`     | SPL max |
   | `leq2_now`    | 2‑min Leq value |
   | `leq2_color`  | 2‑min colour |
   | `leq2_max`    | 2‑min max |
   | `leq15_now`   | 15‑min Leq value |
   | `leq15_color` | 15‑min colour |
   | `leq15_max`   | 15‑min max |
   | `rew_stale`   | `true` when there's no signal |

> Names are case‑sensitive. You reference them later as `$(custom:spl_now)` etc.

---

## 3. Create the polling trigger

This runs ~once a second: fetch `/api/spl`, then split the fields into the
variables above.

1. Top menu → **Triggers** → **Add trigger**. Name it **`REW poll`**.
2. **Event** → **Add event** → **Time interval**. Set **every `1` second**
   (0.5 s if you want snappier buttons — the bridge handles either easily).
3. **Actions** → **Add action**:
   - Connection **`rew`** → **GET**.
   - **URL**: `/api/spl`
   - **JSON Response Data Variable**: `rew_raw`
   - Turn **JSON Stringify Result** **ON**.
4. Add the split actions. For each one: **Add action** → connection **Internal**
   → search **`custom variable expression`** → **“Set custom variable value to
   expression”** (wording varies slightly by version). Pick the target variable
   and paste the expression:

   | Target variable | Expression |
   |---|---|
   | `spl_now`     | `jsonpath($(custom:rew_raw), '$.spl_a_slow') == null ? '--' : jsonpath($(custom:rew_raw), '$.spl_a_slow')` |
   | `spl_color`   | `jsonpath($(custom:rew_raw), '$.spl_a_slow_color')` |
   | `spl_max`     | `jsonpath($(custom:rew_raw), '$.max_spl_a_slow') == null ? '--' : jsonpath($(custom:rew_raw), '$.max_spl_a_slow')` |
   | `leq2_now`    | `jsonpath($(custom:rew_raw), '$.leq_2min') == null ? '--' : jsonpath($(custom:rew_raw), '$.leq_2min')` |
   | `leq2_color`  | `jsonpath($(custom:rew_raw), '$.leq_2min_color')` |
   | `leq2_max`    | `jsonpath($(custom:rew_raw), '$.max_leq_2min') == null ? '--' : jsonpath($(custom:rew_raw), '$.max_leq_2min')` |
   | `leq15_now`   | `jsonpath($(custom:rew_raw), '$.leq_15min') == null ? '--' : jsonpath($(custom:rew_raw), '$.leq_15min')` |
   | `leq15_color` | `jsonpath($(custom:rew_raw), '$.leq_15min_color')` |
   | `leq15_max`   | `jsonpath($(custom:rew_raw), '$.max_leq_15min') == null ? '--' : jsonpath($(custom:rew_raw), '$.max_leq_15min')` |
   | `rew_stale`   | `jsonpath($(custom:rew_raw), '$.data_stale')` |

5. **Enable** the trigger (toggle at the top). Within a second the variables
   start updating — watch them live on the **Variables** page.

> **Tip:** the GET action must be the **first** action so the others read the
> fresh `rew_raw`. Companion runs them top‑to‑bottom.

---

## 4. The three value buttons

Do this once for **SPL A Slow**, then repeat for the other two (only the
variable names change).

### 4a. Button text
1. Click an empty button on a page → it opens the button editor.
2. Set **Text** to (typed literally — Companion expands the `$(...)` parts):
   ```
   SPL A Slow
   $(custom:spl_now)
   Max $(custom:spl_max)
   ```
3. Set a comfortable **Font size** (e.g. 18) and alignment.

### 4b. Colour feedbacks (the green/orange/red)
Add four feedbacks. Each: **Add feedback** → connection **Internal** → search
**`check value`** → **“Variable: Check value”**.

For each feedback set **Variable** = `$(custom:spl_color)`, **Comparison** =
`Equal`, and:

| Value | Background style |
|---|---|
| `green`  | green  (e.g. #00B000) |
| `orange` | orange (e.g. #FFAE00) |
| `red`    | red    (e.g. #FF3B30) + (optional) tick "use a different text colour" white |
| `stale`  | dark grey (e.g. #333333) — shows when there's no signal |

That's it — the button now mirrors the dashboard. When you change a limit in the
web UI, the colour here changes too, with **nothing to edit in Companion**.

### 4c. Repeat for the other two panels
Duplicate the button (right‑click → **Copy**, paste onto another button) and
swap the variable names:

| Panel | Text value var | Max var | Colour var |
|---|---|---|---|
| **2‑min Leq**  | `leq2_now`  | `leq2_max`  | `leq2_color`  |
| **15‑min Leq** | `leq15_now` | `leq15_max` | `leq15_color` |

> The 2‑min panel has no limit set (by your choice), so its colour stays
> `neutral` — it simply won't get a green/orange/red feedback match, which is
> exactly the dashboard behaviour. (Add a 2‑min limit later in the web UI and it
> starts colouring automatically.)

---

## 5. Reset Max buttons

Each reset is a single HTTP POST.

1. New button → **Text**: `Reset SPL Max`.
2. **Add action** → connection **`rew`** → **POST**.
   - **URL**: `/api/control`
   - **Content Type**: `application/json`
   - **Body**:
     ```json
     {"action":"reset_max","panel":"spl_a_slow"}
     ```
3. Repeat for the other panels / an "all" button:

   | Button | Body |
   |---|---|
   | Reset SPL Max    | `{"action":"reset_max","panel":"spl_a_slow"}` |
   | Reset 2‑min Max  | `{"action":"reset_max","panel":"leq_2min"}` |
   | Reset 15‑min Max | `{"action":"reset_max","panel":"leq_15min"}` |
   | Reset ALL Max    | `{"action":"reset_max"}` |

The max readout on the value buttons drops back to `--` immediately.

---

## 6. NO‑SIGNAL alert button

Lights up when the meter stalls (no audio / REW restarting) **or** the bridge is
unreachable.

1. New button → **Text**:
   ```
   REW
   $(custom:rew_stale)
   ```
   (or just a fixed label like `METER`).
2. **Add feedback** → **Internal** → **“Variable: Check value”**:
   - **Variable**: `$(custom:rew_stale)`, **Comparison** `Equal`, **Value** `true`
   - **Style**: red background, text `NO SIGNAL`.
3. *(Optional but recommended — also catch a fully offline bridge.)*
   - Add a custom variable `rew_status` and, in the **GET** action (Step 3.3),
     set **Response Status Code Variable** = `rew_status`.
   - Add a second feedback on this button: **“Variable: Check value”**,
     **Variable** `$(custom:rew_status)`, **Comparison** `Not equal`, **Value**
     `200`, style red text `BRIDGE OFFLINE`.

Now the button is your single at‑a‑glance health indicator.

---

## 7. Suggested page layout

```
┌───────────┬───────────┬───────────┐
│ SPL A     │ 2‑min Leq │ 15‑min Leq│   ← the 3 value buttons (colour‑coded)
│ Slow      │           │           │
├───────────┼───────────┼───────────┤
│ Reset SPL │ Reset 2m  │ Reset 15m │   ← per‑panel resets
├───────────┴─────┬─────┴───────────┤
│  Reset ALL Max  │   NO‑SIGNAL     │
└─────────────────┴─────────────────┘
```

---

## 8. Does this meet the requirements?

| Requirement | Met? | How |
|---|---|---|
| Show **SPL A Slow, 2‑min Leq, 15‑min Leq** | ✅ | three value buttons (Step 4) |
| **Green/orange/red** thresholds | ✅ | per‑panel `*_color`, Check‑value feedbacks |
| Thresholds **editable in web UI, persistent on the bridge** | ✅ | bridge is the single source of truth; Companion reads the colour, never the limits → change once in the web UI |
| Thresholds **match** between dashboard and Companion | ✅ | same `panel_color()` drives both |
| **Bridge‑tracked Max** shown | ✅ | `max_*` on each value button |
| **Reset Max** — per panel | ✅ | `{"action":"reset_max","panel":"…"}` (Step 5) |
| **Stale / no‑signal alert** | ✅ | `data_stale` (+ optional HTTP status) → alert button (Step 6) |
| Works alongside the **web dashboard** (Route B) | ✅ | both read the same API; run simultaneously |
| 24/7 self‑healing unaffected | ✅ | bridge auto‑recovers; Companion just keeps polling |

### Notes / honest caveats
- **API is unauthenticated** on the LAN (by design for the isolated show
  network). Anyone on the network who can reach `:8080` can read values and POST
  controls. Accepted trust model for this venue.
- The **NO‑SIGNAL** button only catches a fully offline bridge if you add the
  optional HTTP‑status feedback (Step 6.3) — otherwise `rew_stale` simply stops
  updating and holds its last value.
- Poll interval 1 s is plenty for SPL/Leq (Leq numbers move slowly). 0.5 s is
  fine too; don't go below ~0.25 s — there's no benefit.

---

## 9. Troubleshooting

| Symptom | Fix |
|---|---|
| Variables stay blank | Trigger disabled, or GET not first action. Check **Variables** page for `rew_raw` filling. |
| `rew_raw` fills but split vars don't | The `jsonpath(...)` expressions need `rew_raw` to be **stringified** — confirm **JSON Stringify Result** is **ON** in the GET action. |
| Buttons show `null` | You're on bridge < v0.4.0, or skipped the `== null ? '--'` part of the expression. |
| Colours never change | Feedback variable should be `$(custom:spl_color)` (the colour var), **not** the value var. Comparison must be **Equal**, value lowercase `green`/`orange`/`red`/`stale`. |
| Nothing connects | Open `http://10.99.50.92:8080/api/spl` in a browser from the Companion PC. If that fails, it's the network/firewall, not Companion. |
| 2‑min button never colours | Expected — no limit set for 2‑min. Add one in the web UI (⚙ Limits) and it starts. |
```
