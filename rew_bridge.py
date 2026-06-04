#!/usr/bin/env python3
"""
REW SPL Meter Bridge

Launches REW (with GUI by default), reads SPL values from REW's API,
exposes them via HTTP for Bitfocus Companion, and accepts control commands.
"""

import asyncio
import json
import logging
import math
import os
import pathlib
import platform
import shutil
import socket
import subprocess
import sys
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

__version__ = "0.3.0"

# App directory (read-only bundled assets like app_icon.ico)
APP_DIR = pathlib.Path(__file__).parent

# Data directory for writable files (config, logs)
# On Windows frozen builds, use %LOCALAPPDATA% to avoid Program Files permission issues
if getattr(sys, "frozen", False) and platform.system() == "Windows":
    DATA_DIR = pathlib.Path(os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")) / "REW SPL Bridge"
else:
    DATA_DIR = APP_DIR
DATA_DIR.mkdir(parents=True, exist_ok=True)

# One-time migration: copy config.json from install dir to DATA_DIR on upgrade
if getattr(sys, "frozen", False) and platform.system() == "Windows":
    _old_config = APP_DIR.parent / "config.json"
    _new_config = DATA_DIR / "config.json"
    if _old_config.exists() and not _new_config.exists():
        shutil.copy2(_old_config, _new_config)

LOG_FILE = DATA_DIR / "rew_bridge.log"

# Default configuration
DEFAULTS = {
    "rew_path": None,
    "bridge_port": 8080,
    "rew_api_port": 4735,
    "log_level": "INFO",
    "rew_gui": True,
}


def load_config() -> dict:
    """Load configuration from config.json, falling back to defaults."""
    config = dict(DEFAULTS)
    config_path = DATA_DIR / "config.json"

    if config_path.exists():
        try:
            with open(config_path, "r") as f:
                user_config = json.load(f)
            for key in DEFAULTS:
                if key in user_config:
                    config[key] = user_config[key]
        except (json.JSONDecodeError, OSError) as e:
            # Will be logged once logging is set up; use defaults
            pass

    # Validate types and ranges
    if not isinstance(config["bridge_port"], int) or not (1024 <= config["bridge_port"] <= 65535):
        print(f"WARNING: Invalid bridge_port {config['bridge_port']!r}, using default {DEFAULTS['bridge_port']}", file=sys.stderr)
        config["bridge_port"] = DEFAULTS["bridge_port"]
    if not isinstance(config["rew_api_port"], int) or not (1 <= config["rew_api_port"] <= 65535):
        print(f"WARNING: Invalid rew_api_port {config['rew_api_port']!r}, using default {DEFAULTS['rew_api_port']}", file=sys.stderr)
        config["rew_api_port"] = DEFAULTS["rew_api_port"]
    config["log_level"] = str(config.get("log_level", "INFO"))
    if config["log_level"].upper() not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        print(f"WARNING: Invalid log_level {config['log_level']!r}, using default {DEFAULTS['log_level']}", file=sys.stderr)
        config["log_level"] = DEFAULTS["log_level"]
    if not isinstance(config.get("rew_gui"), bool):
        print(f"WARNING: Invalid rew_gui {config.get('rew_gui')!r}, coercing to bool", file=sys.stderr)
        config["rew_gui"] = bool(config.get("rew_gui", False))

    return config


def save_config(config: dict):
    """Save configuration to config.json (atomic write via temp file + rename)."""
    import tempfile

    config_path = DATA_DIR / "config.json"
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(dir=DATA_DIR, suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(config, f, indent=4)
        # On Windows, os.replace is atomic if on the same volume
        os.replace(tmp_path, config_path)
    except OSError:
        # Clean up temp file if it was created
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        # Fall back to direct write if atomic write fails
        with open(config_path, "w") as f:
            json.dump(config, f, indent=4)


def find_free_port(start: int = 8080) -> int:
    """Scan for a free port starting from the given port number."""
    for port in range(start, start + 100):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", port))
                return port
        except OSError:
            continue
    raise OSError(f"No free port found in range {start}-{start + 99}")


def setup_logging(log_level: str = "INFO"):
    """Configure logging with RotatingFileHandler and console output."""
    level = getattr(logging, str(log_level).upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear existing handlers
    root_logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # File handler: 1 MB max, 3 backups
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Quiet noisy third-party loggers. At ~5 polls/sec, httpx/httpcore would
    # otherwise log a line per request and churn the rotating log within hours,
    # discarding the events that actually matter for troubleshooting.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# Load config and set up logging
config = load_config()

# On first run (no config.json), find a free port and save config
config_path = DATA_DIR / "config.json"
if not config_path.exists():
    config["bridge_port"] = find_free_port(config["bridge_port"])
    save_config(config)

setup_logging(config["log_level"])
logger = logging.getLogger(__name__)

logger.info("Configuration: bridge_port=%s, rew_api_port=%s, log_level=%s, rew_path=%s",
            config["bridge_port"], config["rew_api_port"], config["log_level"], config["rew_path"])

# Derived configuration
REW_API_PORT = config["rew_api_port"]
BRIDGE_PORT = config["bridge_port"]
REW_API_BASE = f"http://localhost:{REW_API_PORT}"

# SPL levels are read by polling REW's GET /spl-meter/1/levels rather than by
# subscribing. Polling avoids the inbound callback server, the re-subscribe
# bookkeeping, and REW's "cancel subscription on any missed 200" failure mode.
POLL_INTERVAL = 0.2              # how often to poll REW for levels (s) ~5 Hz
ELAPSED_STALE_SECONDS = 2.0      # if elapsedTime hasn't moved in this long, meter is idle

# 2-minute Leq is reconstructed from REW's own rolling 1-minute Leq engine.
# A 2-min window is the energy average of two contiguous, non-overlapping 1-min
# Leqs: the current leq1m (covering [t-60, t]) and the leq1m from ~60 s ago
# (covering [t-120, t-60]). This is exact and far more accurate than energy-
# averaging buffered Slow SPL, and it does not depend on the subscription rate.
LEQ_2MIN_WINDOW = 120.0          # total averaging window (s)
LEQ_2MIN_SUBWINDOW = 60.0        # spacing between the two 1-min Leq samples (s)
LEQ1M_HISTORY_SECONDS = 130.0    # how much leq1m history to retain (s)
LEQ_2MIN_MATCH_TOLERANCE = 5.0   # max |dt| from the 60 s-ago target to accept (s)


@dataclass
class SPLState:
    """Current SPL meter state."""
    spl_a_slow: Optional[float] = None
    leq_15min: Optional[float] = None
    leq_1min: Optional[float] = None
    leq_10min: Optional[float] = None
    elapsed_time: float = 0.0
    last_update: float = 0.0
    # Wall-clock time elapsedTime last changed; used to detect an idle meter.
    last_elapsed_change: float = 0.0
    # History of (timestamp, leq1m) used to reconstruct the 2-min Leq.
    leq1m_history: deque = field(default_factory=deque)
    rew_running: bool = False
    measurement_active: bool = False

    def record_leq1m(self, timestamp: float, leq1m: float) -> None:
        """Append a 1-min Leq sample and drop history older than the retain window."""
        self.leq1m_history.append((timestamp, leq1m))
        cutoff = timestamp - LEQ1M_HISTORY_SECONDS
        while self.leq1m_history and self.leq1m_history[0][0] < cutoff:
            self.leq1m_history.popleft()

    def compute_leq_2min(self) -> Optional[float]:
        """Reconstruct the rolling 2-min Leq from REW's own 1-min Leq engine.

        The 2-min window is the energy average of two contiguous, non-overlapping
        1-min windows: the current leq1m (covering [t-60, t]) and the leq1m from
        ~60 s ago (covering [t-120, t-60]). Both must be complete minutes, so the
        measurement must have been running for at least 2 minutes.
        """
        if self.leq_1min is None or self.last_update == 0.0:
            return None
        # Require a full 2 minutes so both 1-min Leqs are complete windows.
        if self.elapsed_time < LEQ_2MIN_WINDOW:
            return None

        # Find the leq1m sample closest to 60 s ago.
        target = self.last_update - LEQ_2MIN_SUBWINDOW
        older = None
        best_dt = None
        for ts, value in self.leq1m_history:
            dt = abs(ts - target)
            if best_dt is None or dt < best_dt:
                best_dt, older = dt, value
        if older is None or best_dt is None or best_dt > LEQ_2MIN_MATCH_TOLERANCE:
            return None

        try:
            current_linear = 10 ** (self.leq_1min / 10)
            older_linear = 10 ** (older / 10)
            return 10 * math.log10((current_linear + older_linear) / 2)
        except (ValueError, ZeroDivisionError):
            return None


# Global state
state = SPLState()
rew_process: Optional[subprocess.Popen] = None
http_client: Optional[httpx.AsyncClient] = None


class ControlRequest(BaseModel):
    """Control command request."""
    action: str  # start, stop, restart, shutdown


class SPLValues(BaseModel):
    """SPL values received from REW subscription."""
    meterNumber: int = 1
    weighting: str = "A"
    filter: str = "Slow"
    spl: float
    leq: float
    isRollingLeq: bool = False
    rollingLeqMinutes: float = 0.0
    leq1m: float = 0.0
    leq10m: float = 0.0
    sel: float = 0.0
    elapsedTime: float


def find_rew_executable() -> Optional[str]:
    """Find the REW executable based on platform."""
    # Check config for custom path first
    if config.get("rew_path"):
        custom_path = config["rew_path"]
        if os.path.exists(custom_path):
            return custom_path
        logger.warning("Configured rew_path does not exist: %s", custom_path)

    system = platform.system()

    if system == "Windows":
        paths = [
            r"C:\Program Files\REW\roomeqwizard.exe",
            r"C:\Program Files (x86)\REW\roomeqwizard.exe",
        ]
        for path in paths:
            if os.path.exists(path):
                return path
        return None

    elif system == "Darwin":  # macOS
        if os.path.exists("/Applications/REW.app"):
            return "/Applications/REW.app"
        return None

    return None


def launch_rew() -> Optional[subprocess.Popen]:
    """Launch REW with API enabled and no GUI."""
    global rew_process

    system = platform.system()

    try:
        if system == "Windows":
            rew_path = find_rew_executable()
            if not rew_path:
                logger.error("REW executable not found on Windows")
                return None

            logger.info(f"Launching REW from: {rew_path}")
            args = [rew_path, "-api"]
            if not config.get("rew_gui"):
                args.append("-nogui")
            rew_process = subprocess.Popen(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

        elif system == "Darwin":  # macOS
            rew_path = find_rew_executable()
            if not rew_path:
                logger.error("REW.app not found in /Applications")
                return None

            logger.info("Launching REW on macOS")
            args = ["open", "-a", "REW.app", "--args", "-api"]
            if not config.get("rew_gui"):
                args.append("-nogui")
            rew_process = subprocess.Popen(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

        else:
            logger.error(f"Unsupported platform: {system}")
            return None

        return rew_process

    except Exception as e:
        logger.error(f"Failed to launch REW: {e}")
        return None


async def wait_for_rew_api(timeout: float = 30.0) -> bool:
    """Wait for REW API to become available."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = await http_client.get(f"{REW_API_BASE}/spl-meter/1/levels")
            if response.status_code == 200:
                logger.info("REW API is ready")
                return True
        except httpx.RequestError:
            pass

        await asyncio.sleep(0.5)

    logger.error(f"REW API did not become available within {timeout} seconds")
    return False


async def configure_spl_meter():
    """Configure the SPL meter for our needs."""
    meter_config = {
        "mode": "SPL",
        "weighting": "A",
        "filter": "Slow",
        "rollingLeqActive": True,
        "rollingLeqMinutes": 15
    }

    try:
        response = await http_client.post(
            f"{REW_API_BASE}/spl-meter/1/configuration",
            json=meter_config
        )
        if response.status_code == 200:
            logger.info("SPL meter configured successfully")
            return True
        else:
            logger.error(f"Failed to configure SPL meter: {response.status_code}")
            return False
    except httpx.RequestError as e:
        logger.error(f"Error configuring SPL meter: {e}")
        return False


async def start_meter() -> bool:
    """Start the SPL meter so values flow (used on connect and for the start command)."""
    success = await send_spl_command("Start")
    if success:
        state.measurement_active = True
        state.last_elapsed_change = time.time()
    return success


def _finite_or_none(x: Optional[float]) -> Optional[float]:
    """Map NaN/inf to None. REW reports NaN for levels when there is no signal."""
    if x is None or not math.isfinite(x):
        return None
    return x


def update_state_from_levels(data: dict) -> None:
    """Update shared state from a polled /spl-meter/1/levels response."""
    try:
        values = SPLValues(**data)
    except Exception:
        logger.debug("Could not parse SPL levels payload: %r", data)
        return

    now = time.time()
    leq1m = _finite_or_none(values.leq1m)
    state.spl_a_slow = _finite_or_none(values.spl)
    if values.isRollingLeq and values.rollingLeqMinutes == 15:
        state.leq_15min = _finite_or_none(values.leq)
    state.leq_1min = leq1m
    state.leq_10min = _finite_or_none(values.leq10m)

    # The meter is "active" while elapsedTime keeps advancing. Tracking the time
    # of the last change (rather than comparing magnitudes) is robust to polling
    # faster than REW updates and to elapsedTime resetting on a new measurement.
    if values.elapsedTime != state.elapsed_time:
        state.elapsed_time = values.elapsedTime
        state.last_elapsed_change = now
    state.measurement_active = (now - state.last_elapsed_change) < ELAPSED_STALE_SECONDS

    state.last_update = now
    # Only record real 1-min Leq values for the 2-min reconstruction.
    if leq1m is not None:
        state.record_leq1m(now, leq1m)


async def send_spl_command(command: str) -> bool:
    """Send a command to the SPL meter."""
    try:
        response = await http_client.post(
            f"{REW_API_BASE}/spl-meter/1/command",
            json={"command": command}
        )
        if response.status_code == 200:
            logger.info(f"SPL meter command '{command}' sent successfully")
            return True
        else:
            logger.error(f"Failed to send SPL meter command: {response.status_code}")
            return False
    except httpx.RequestError as e:
        logger.error(f"Error sending SPL meter command: {e}")
        return False


async def shutdown_rew():
    """Shutdown REW gracefully."""
    global rew_process

    if not state.rew_running and rew_process is None:
        return

    try:
        response = await http_client.post(
            f"{REW_API_BASE}/application/command",
            json={"command": "Shutdown"}
        )
        logger.info("REW shutdown command sent")
    except httpx.RequestError:
        pass  # REW may already be shutting down

    if rew_process:
        try:
            rew_process.terminate()
            await asyncio.wait_for(
                asyncio.to_thread(rew_process.wait), timeout=5
            )
        except (asyncio.TimeoutError, OSError):
            try:
                rew_process.kill()
            except OSError:
                pass
        rew_process = None

    state.rew_running = False
    state.measurement_active = False
    state.spl_a_slow = None
    state.leq_15min = None
    state.leq_1min = None
    state.leq_10min = None
    state.elapsed_time = 0.0
    state.last_update = 0.0
    state.leq1m_history.clear()


_restart_lock = asyncio.Lock()


async def restart_rew():
    """Restart REW."""
    if _restart_lock.locked():
        logger.warning("Restart already in progress, skipping")
        return False

    async with _restart_lock:
        logger.info("Restarting REW...")
        await shutdown_rew()
        await asyncio.sleep(2)  # Give time for cleanup

        if launch_rew():
            if await wait_for_rew_api():
                state.rew_running = True
                await configure_spl_meter()
                await start_meter()
                logger.info("REW restarted successfully")
                return True

        logger.error("Failed to restart REW")
        return False


async def poll_levels_loop():
    """Poll REW for SPL levels, update state, and reconnect/reconfigure as needed."""
    failures = 0
    while True:
        try:
            response = await http_client.get(
                f"{REW_API_BASE}/spl-meter/1/levels",
                timeout=5.0
            )
            if response.status_code == 200:
                if not state.rew_running:
                    # (Re)connected to REW: configure the meter and auto-start it.
                    logger.info("REW API connection established")
                    state.rew_running = True
                    await configure_spl_meter()
                    await start_meter()
                update_state_from_levels(response.json())
                failures = 0
            else:
                logger.warning("REW levels returned status %s", response.status_code)
                failures += 1
                if state.rew_running and failures >= 3:
                    state.rew_running = False
        except httpx.RequestError:
            failures += 1
            if state.rew_running and failures >= 3:
                logger.warning("Lost connection to REW API")
                state.rew_running = False
        except Exception:
            logger.exception("Unexpected error in poll loop")
        await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global http_client

    # Startup
    http_client = httpx.AsyncClient(timeout=10.0)

    logger.info("Starting REW SPL Meter Bridge")

    # Check if REW is already running before trying to launch
    if await wait_for_rew_api(timeout=3.0):
        logger.info("REW is already running, connecting to existing instance")
        state.rew_running = True
        await configure_spl_meter()
        await start_meter()
    elif launch_rew():
        if await wait_for_rew_api():
            state.rew_running = True
            await configure_spl_meter()
            await start_meter()
        else:
            logger.error("REW launched but API did not become available")

    # Start polling task (also handles reconnect/reconfigure)
    poll_task = asyncio.create_task(poll_levels_loop())

    yield

    # Shutdown
    poll_task.cancel()
    try:
        await poll_task
    except asyncio.CancelledError:
        pass

    await shutdown_rew()
    await http_client.aclose()
    logger.info("REW SPL Meter Bridge stopped")


# FastAPI app
app = FastAPI(
    title="REW SPL Meter Bridge",
    description="Bridge between REW SPL meter and Bitfocus Companion",
    version=__version__,
    lifespan=lifespan
)


@app.get("/api/spl")
async def get_spl():
    """Get current SPL values."""
    leq_2min = state.compute_leq_2min()
    history = state.leq1m_history
    history_seconds = (history[-1][0] - history[0][0]) if len(history) >= 2 else 0.0

    return {
        "spl_a_slow": state.spl_a_slow,
        "leq_1min": state.leq_1min,
        "leq_2min": round(leq_2min, 1) if leq_2min is not None else None,
        "leq_10min": state.leq_10min,
        "leq_15min": state.leq_15min,
        "elapsed_time": state.elapsed_time,
        "valid_2min": leq_2min is not None,
        "rew_running": state.rew_running,
        "measurement_active": state.measurement_active,
        "buffer_samples": len(history),
        "buffer_seconds": round(history_seconds, 1)
    }


@app.post("/api/control")
async def control(request: ControlRequest):
    """Handle control commands."""
    action = request.action.lower()

    if action == "start":
        if not state.rew_running:
            raise HTTPException(status_code=503, detail="REW is not running")

        # Clear history when starting a new measurement
        state.leq1m_history.clear()
        success = await start_meter()
        return {"status": "ok" if success else "error", "action": action}

    elif action == "stop":
        if not state.rew_running:
            raise HTTPException(status_code=503, detail="REW is not running")

        success = await send_spl_command("Stop")
        if success:
            state.measurement_active = False
            state.leq1m_history.clear()
        return {"status": "ok" if success else "error", "action": action}

    elif action == "restart":
        success = await restart_rew()
        state.leq1m_history.clear()
        state.measurement_active = False
        return {"status": "ok" if success else "error", "action": action}

    elif action == "shutdown":
        await shutdown_rew()
        state.measurement_active = False
        return {"status": "ok", "action": action}

    else:
        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "rew_running": state.rew_running,
        "last_update": state.last_update,
        "seconds_since_update": time.time() - state.last_update if state.last_update > 0 else None
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=BRIDGE_PORT, log_level="info", access_log=False)
