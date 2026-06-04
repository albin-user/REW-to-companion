# REW SPL Meter Bridge

A bridge that launches REW (Room EQ Wizard), reads SPL values from REW's API, and exposes them via HTTP for Bitfocus Companion integration. On Windows, it runs as a system tray application with an installer. REW's GUI is shown by default (easier to troubleshoot); it can be run headless via the tray menu or config.

## Architecture

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│      REW        │◄───────►│  Python Bridge  │◄───────►│    Companion    │
│  (localhost)    │  :4735  │   (FastAPI)     │  :8080  │   (remote PC)   │
└─────────────────┘         └─────────────────┘         └─────────────────┘
```

## Windows Installation

1. Download `REW-Bridge-Setup-X.Y.Z.exe` from [GitHub Releases](../../releases)
2. Run the installer — it will:
   - Install the app to Program Files
   - Create a desktop shortcut and Start Menu entry
   - Optionally add a Windows Firewall rule
   - Optionally set it to start automatically on boot
3. Launch from the desktop shortcut — a system tray icon appears near the clock

**Prerequisite:** [REW (Room EQ Wizard)](https://www.roomeqwizard.com/) must be installed separately.

> ⚠️ **REW API requires a beta build (as of 2026-06-04).** The REST API this
> bridge depends on is only available in REW **beta** releases — it is **not** in
> the current stable release. Install the latest REW **beta** from the
> [REW downloads page](https://www.roomeqwizard.com/) (look for the beta build).
> This requirement will go away once the API ships in a stable REW release.

### System Tray Icon

- **Red circle** — REW is not connected
- **Green circle** — REW is connected and running
- **Right-click menu:**
  - Status and port display
  - Open Dashboard — opens the web dashboard in your browser; the menu label shows the LAN address (`IP:port`)
  - Show REW GUI — toggle headless vs. GUI mode (takes effect on next REW launch)
  - Change Port — opens a dialog to set a new port (restart required)
  - Open Log / Open Log Folder — access log files for troubleshooting
  - Quit — cleanly shuts down REW and the bridge

### Configuration

The app stores its settings in `config.json`. On Windows, this is located in `%LOCALAPPDATA%\REW SPL Bridge\` (along with log files). When running from source or on macOS, files are stored in the script directory.

| Setting | Default | Description |
|---------|---------|-------------|
| `rew_path` | `null` | Path to REW executable. `null` = auto-detect from Program Files |
| `bridge_port` | `8080` | HTTP port for the bridge server |
| `rew_api_port` | `4735` | REW API port |
| `log_level` | `"INFO"` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `rew_gui` | `true` | Show REW GUI when running. `true` = GUI shown; `false` = headless (`-nogui`) |
| `thresholds` | per-panel limits | Green/orange/red dB limits per panel for the dashboard. Editable live from the web UI (⚙ Limits) and persisted here. `null` for a panel = no colour |

On first run, the app auto-selects a free port starting at 8080 and saves it to `config.json`.

### Troubleshooting

| Problem | Solution |
|---------|----------|
| Bridge won't start | Check `rew_bridge.log` for errors (right-click tray → Open Log) |
| Companion can't connect | Verify the firewall rule exists, check the port in the tray menu |
| REW not found | Set `rew_path` in `config.json` to the full path of `roomeqwizard.exe` |
| SPL values are null | The meter auto-starts on connect; if values stay null, check the log and confirm REW's audio input is selected. You can also force a start (POST `/api/control` with `{"action":"start"}`) |
| Tray icon stays red | REW may still be starting — wait 30 seconds. Check log for API errors |

## Features

- Launches REW automatically with API mode enabled (`-api`; GUI shown by default, headless optional)
- Polls REW's SPL meter levels and auto-starts metering on connect (values flow with no manual start)
- Reconstructs an exact 2-minute rolling Leq from REW's native 1-minute Leq (not natively available in REW)
- Exposes values via simple HTTP API for Bitfocus Companion
- Built-in **responsive web dashboard** at `/` — live SPL/Leq readouts, per-panel Max, and editable green/orange/red thresholds (saved on the server)
- **Bitfocus Companion** ready — per-panel colour state + stale flag in the API so buttons mirror the dashboard with no threshold logic in Companion (see [COMPANION_SETUP.md](COMPANION_SETUP.md))
- **Self-healing:** auto-recovers a stalled meter (~2 s) and relaunches REW if its process crashes (~28 s)
- Control commands: start, stop, restart, shutdown, reset_max (per-panel or all)
- File logging with rotation (1 MB, 3 backups)

## Available Values

| Value | Source |
|-------|--------|
| SPL A Slow | Direct from REW API |
| 1-min Leq | Direct from REW API |
| 2-min Leq | Computed: energy average of REW's current and 60 s-ago 1-min Leq |
| 10-min Leq | Direct from REW API |
| 15-min Leq | Direct from REW API (rolling Leq) |

## API Endpoints

### GET /api/spl

Returns current SPL values:

```json
{
  "spl_a_slow": 75.2,
  "leq_1min": 74.5,
  "leq_2min": 74.8,
  "leq_10min": 73.5,
  "leq_15min": 73.1,
  "max_spl_a_slow": 88.1,
  "max_leq_2min": 80.3,
  "max_leq_15min": 79.0,
  "spl_a_slow_color": "green",
  "leq_2min_color": "neutral",
  "leq_15min_color": "green",
  "elapsed_time": 125.5,
  "valid_2min": true,
  "rew_running": true,
  "measurement_active": true,
  "data_stale": false,
  "seconds_since_update": 0.2,
  "buffer_samples": 1200,
  "buffer_seconds": 120.0
}
```

`*_color` is computed on the bridge from the dashboard thresholds (one of
`green` / `orange` / `red` / `neutral` / `stale`) so Companion can mirror the
dashboard colours with a plain string compare — no threshold logic in Companion.
`data_stale` is `true` when the meter isn't producing fresh values.

### POST /api/control

Control the SPL meter:

```bash
# Start measurement
curl -X POST http://localhost:8080/api/control \
  -H "Content-Type: application/json" \
  -d '{"action":"start"}'

# Stop measurement
curl -X POST http://localhost:8080/api/control \
  -H "Content-Type: application/json" \
  -d '{"action":"stop"}'

# Restart REW
curl -X POST http://localhost:8080/api/control \
  -H "Content-Type: application/json" \
  -d '{"action":"restart"}'

# Shutdown REW
curl -X POST http://localhost:8080/api/control \
  -H "Content-Type: application/json" \
  -d '{"action":"shutdown"}'

# Reset the tracked Max for one panel (spl_a_slow | leq_2min | leq_15min)
curl -X POST http://localhost:8080/api/control \
  -H "Content-Type: application/json" \
  -d '{"action":"reset_max","panel":"spl_a_slow"}'

# Reset the Max for all panels (omit "panel")
curl -X POST http://localhost:8080/api/control \
  -H "Content-Type: application/json" \
  -d '{"action":"reset_max"}'
```

### GET /health

Health check endpoint:

```json
{
  "status": "healthy",
  "rew_running": true,
  "last_update": 1234567890.123,
  "seconds_since_update": 0.5
}
```

## Bitfocus Companion Integration

**See [COMPANION_SETUP.md](COMPANION_SETUP.md) for a full click‑by‑click guide** —
value buttons, dashboard‑matched green/orange/red colours, per‑panel Max + Reset,
and a no‑signal alert button.

In short: use the Generic HTTP module to poll `/api/spl` and split fields with the
built‑in `jsonpath()` expression. Useful paths:
- `$.spl_a_slow`, `$.leq_2min`, `$.leq_15min` — the three values
- `$.spl_a_slow_color`, `$.leq_2min_color`, `$.leq_15min_color` — colour state
- `$.max_spl_a_slow` … — bridge‑tracked maxes
- `$.data_stale` — no‑signal flag

Reset a max with `POST /api/control` body `{"action":"reset_max","panel":"spl_a_slow"}`
(omit `panel` to reset all).

## macOS

On macOS, run the bridge from source (see Development section below). The tray app and installer are Windows-only. Allow incoming connections when prompted by the firewall.

## Development

### Run from source

```bash
pip install -r requirements.txt

# Bridge only (no tray UI)
python rew_bridge.py

# System tray app (Windows)
python tray_app.py
```

### Build locally

```bash
# Install build deps
pip install -r requirements-dev.txt

# Generate icon
python generate_icon.py

# Build with PyInstaller (one-folder mode)
pyinstaller --clean rew_bridge.spec

# Build installer (requires Inno Setup on Windows)
iscc /DMyAppVersion=0.4.0 installer.iss
```

### Releases

Releases are built automatically by GitHub Actions when a version tag is pushed:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

This triggers the CI pipeline which builds the PyInstaller bundle, creates the Inno Setup installer, and publishes it as a GitHub Release.

## License

MIT
