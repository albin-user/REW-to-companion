# REW SPL Meter Bridge

A bridge that launches REW (Room EQ Wizard) headlessly, reads SPL values from REW's API, and exposes them via HTTP for Bitfocus Companion integration. On Windows, it runs as a system tray application with an installer.

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

### System Tray Icon

- **Red circle** — REW is not connected
- **Green circle** — REW is connected and running
- **Right-click menu:**
  - Status and port display
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
| `rew_gui` | `false` | Show REW GUI when running. `false` = headless (`-nogui`) |

On first run, the app auto-selects a free port starting at 8080 and saves it to `config.json`.

### Troubleshooting

| Problem | Solution |
|---------|----------|
| Bridge won't start | Check `rew_bridge.log` for errors (right-click tray → Open Log) |
| Companion can't connect | Verify the firewall rule exists, check the port in the tray menu |
| REW not found | Set `rew_path` in `config.json` to the full path of `roomeqwizard.exe` |
| SPL values are null | Ensure REW measurement is started (POST `/api/control` with `{"action":"start"}`) |
| Tray icon stays red | REW may still be starting — wait 30 seconds. Check log for API errors |

## Features

- Launches REW automatically with API mode enabled (`-api -nogui`)
- Subscribes to real-time SPL meter updates
- Computes 2-minute rolling Leq (not natively available in REW)
- Exposes values via simple HTTP API for Bitfocus Companion
- Control commands: start, stop, restart, shutdown
- File logging with rotation (1 MB, 3 backups)

## Available Values

| Value | Source |
|-------|--------|
| SPL A Slow | Direct from REW API |
| 2-min Leq | Computed from buffered SPL readings |
| 15-min Leq | Direct from REW API (rolling Leq) |

## API Endpoints

### GET /api/spl

Returns current SPL values:

```json
{
  "spl_a_slow": 75.2,
  "leq_2min": 74.8,
  "leq_15min": 73.1,
  "elapsed_time": 125.5,
  "valid_2min": true,
  "rew_running": true,
  "measurement_active": true,
  "buffer_samples": 1200,
  "buffer_seconds": 120.0
}
```

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

Use the Generic HTTP module in Companion to poll `/api/spl` and display values.

Example variable parsing:
- `$.spl_a_slow` — Current SPL
- `$.leq_2min` — 2-minute Leq
- `$.leq_15min` — 15-minute Leq

## macOS

On macOS, run the bridge from source (see Development section below). The tray app and installer are Windows-only. Allow incoming connections when prompted by the firewall.

## Development

### Run from source

```bash
pip install -r requirements.txt

# Bridge only (headless)
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
iscc /DMyAppVersion=0.1.0 installer.iss
```

### Releases

Releases are built automatically by GitHub Actions when a version tag is pushed:

```bash
git tag v1.0.0
git push origin v1.0.0
```

This triggers the CI pipeline which builds the PyInstaller bundle, creates the Inno Setup installer, and publishes it as a GitHub Release.

## License

MIT
