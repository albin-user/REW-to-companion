#!/usr/bin/env python3
"""
REW SPL Meter Bridge

Launches REW headlessly, reads SPL values from REW's API,
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

__version__ = "0.2.3"

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
    "rew_gui": False,
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
SUBSCRIPTION_CALLBACK_URL = f"http://localhost:{BRIDGE_PORT}/rew-callback"

# 2-minute Leq buffer: 2 minutes * 60 seconds * 10 Hz = 1200 samples
LEQ_2MIN_BUFFER_SIZE = 1200
LEQ_2MIN_MIN_SAMPLES = 1200  # Require full 2 minutes for valid calculation


@dataclass
class SPLState:
    """Current SPL meter state."""
    spl_a_slow: Optional[float] = None
    leq_15min: Optional[float] = None
    leq_1min: Optional[float] = None
    leq_10min: Optional[float] = None
    elapsed_time: float = 0.0
    last_update: float = 0.0
    spl_buffer: deque = field(default_factory=lambda: deque(maxlen=LEQ_2MIN_BUFFER_SIZE))
    rew_running: bool = False
    measurement_active: bool = False

    def compute_leq_2min(self) -> Optional[float]:
        """Compute 2-minute Leq from buffered SPL values."""
        if len(self.spl_buffer) < LEQ_2MIN_MIN_SAMPLES:
            return None

        # Leq = 10 * log10(mean(10^(spl_i/10)))
        try:
            linear_values = [10 ** (spl / 10) for spl in self.spl_buffer]
            mean_linear = sum(linear_values) / len(linear_values)
            return 10 * math.log10(mean_linear)
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
    rollingLeqMinutes: int = 0
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
            response = await http_client.get(f"{REW_API_BASE}/application")
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


async def subscribe_to_spl_meter():
    """Subscribe to SPL meter updates."""
    try:
        response = await http_client.post(
            f"{REW_API_BASE}/spl-meter/1/subscribe",
            json={"callbackUrl": SUBSCRIPTION_CALLBACK_URL}
        )
        if response.status_code == 200:
            logger.info(f"Subscribed to SPL meter updates (callback: {SUBSCRIPTION_CALLBACK_URL})")
            return True
        else:
            logger.error(f"Failed to subscribe to SPL meter: {response.status_code}")
            return False
    except httpx.RequestError as e:
        logger.error(f"Error subscribing to SPL meter: {e}")
        return False


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
            json={"command": "shutdown"}
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
    state.spl_buffer.clear()


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
                await subscribe_to_spl_meter()
                logger.info("REW restarted successfully")
                return True

        logger.error("Failed to restart REW")
        return False


async def subscription_keepalive():
    """Periodically check REW connection and re-subscribe if needed."""
    while True:
        try:
            response = await http_client.get(
                f"{REW_API_BASE}/application",
                timeout=5.0
            )
            if response.status_code == 200:
                if not state.rew_running:
                    logger.info("REW API connection restored")
                    state.rew_running = True
                    await configure_spl_meter()
                    await subscribe_to_spl_meter()
                elif state.last_update > 0 and (time.time() - state.last_update) > 30:
                    logger.info("No SPL data for 30s, re-subscribing")
                    await configure_spl_meter()
                    await subscribe_to_spl_meter()
                elif state.last_update == 0 and state.rew_running:
                    logger.info("No SPL data received yet, re-subscribing")
                    await configure_spl_meter()
                    await subscribe_to_spl_meter()
            else:
                logger.warning("REW API returned status %s", response.status_code)
                if state.rew_running:
                    state.rew_running = False
        except httpx.RequestError:
            if state.rew_running:
                logger.warning("Lost connection to REW API")
                state.rew_running = False
        except Exception:
            logger.exception("Unexpected error in keepalive loop")
        await asyncio.sleep(10)


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
        await subscribe_to_spl_meter()
    elif launch_rew():
        if await wait_for_rew_api():
            state.rew_running = True
            await configure_spl_meter()
            await subscribe_to_spl_meter()
        else:
            logger.error("REW launched but API did not become available")

    # Start keepalive task
    keepalive_task = asyncio.create_task(subscription_keepalive())

    yield

    # Shutdown
    keepalive_task.cancel()
    try:
        await keepalive_task
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
        "buffer_samples": len(state.spl_buffer),
        "buffer_seconds": len(state.spl_buffer) / 10.0  # Assuming 10 Hz
    }


@app.post("/api/control")
async def control(request: ControlRequest):
    """Handle control commands."""
    action = request.action.lower()

    if action == "start":
        if not state.rew_running:
            raise HTTPException(status_code=503, detail="REW is not running")

        # Clear buffer when starting new measurement
        state.spl_buffer.clear()
        success = await send_spl_command("Start")
        if success:
            state.measurement_active = True
        return {"status": "ok" if success else "error", "action": action}

    elif action == "stop":
        if not state.rew_running:
            raise HTTPException(status_code=503, detail="REW is not running")

        success = await send_spl_command("Stop")
        if success:
            state.measurement_active = False
            state.spl_buffer.clear()
        return {"status": "ok" if success else "error", "action": action}

    elif action == "restart":
        success = await restart_rew()
        state.spl_buffer.clear()
        state.measurement_active = False
        return {"status": "ok" if success else "error", "action": action}

    elif action == "shutdown":
        await shutdown_rew()
        state.measurement_active = False
        return {"status": "ok", "action": action}

    else:
        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")


@app.post("/rew-callback")
async def rew_callback(values: SPLValues):
    """Receive SPL updates from REW subscription."""
    state.spl_a_slow = values.spl
    state.leq_15min = values.leq if values.isRollingLeq and values.rollingLeqMinutes == 15 else state.leq_15min
    state.leq_1min = values.leq1m
    state.leq_10min = values.leq10m
    # Detect measurement state from elapsed time progression
    if values.elapsedTime > state.elapsed_time:
        state.measurement_active = True
    else:
        state.measurement_active = False
    state.elapsed_time = values.elapsedTime
    state.last_update = time.time()

    # Add to buffer for 2-min Leq calculation
    state.spl_buffer.append(values.spl)

    return {"status": "ok"}


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
    uvicorn.run(app, host="0.0.0.0", port=BRIDGE_PORT, log_level="info")
