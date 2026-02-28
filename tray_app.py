#!/usr/bin/env python3
"""
REW SPL Meter Bridge — System Tray Application

Main entry point for the packaged Windows app. Runs the FastAPI bridge
server in a background thread and provides a system tray icon with
status, configuration, and log access.
"""

import ctypes
import json
import logging
import os
import platform
import socket
import subprocess
import sys
import threading
import time

import httpx
import PIL.Image
import PIL.ImageDraw
import pystray
import uvicorn

import rew_bridge

logger = logging.getLogger(__name__)


class REWBridgeTray:
    def __init__(self):
        self.config = rew_bridge.load_config()
        self.connected = False
        self.server = None
        self.icon = None
        self._stop_event = threading.Event()

    def create_status_icon(self, connected: bool) -> PIL.Image.Image:
        """Create a 64x64 tray icon: green circle if connected, red if not."""
        size = 64
        img = PIL.Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = PIL.ImageDraw.Draw(img)

        # Dark background circle
        draw.ellipse([2, 2, size - 3, size - 3], fill=(40, 40, 40, 255))

        # Status circle (green=connected, red=disconnected)
        color = (0, 180, 0, 255) if connected else (200, 0, 0, 255)
        margin = 14
        draw.ellipse(
            [margin, margin, size - margin - 1, size - margin - 1],
            fill=color,
        )

        return img

    def build_menu(self) -> pystray.Menu:
        """Build the right-click tray menu."""
        return pystray.Menu(
            pystray.MenuItem(
                lambda item: f"Status: {'Connected' if self.connected else 'Disconnected'}",
                None,
                enabled=False,
            ),
            pystray.MenuItem(
                lambda item: f"Port: {self.config.get('bridge_port', 8080)}",
                None,
                enabled=False,
            ),
            pystray.MenuItem(
                "Show REW GUI",
                self.toggle_rew_gui,
                checked=lambda item: self.config.get("rew_gui", False),
            ),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Change Port...", self.change_port),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Open Log", self.open_log),
            pystray.MenuItem("Open Log Folder", self.open_log_folder),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit", self.quit),
        )

    def start_server(self):
        """Start the uvicorn server in a daemon thread."""
        port = self.config.get("bridge_port", 8080)
        uv_config = uvicorn.Config(
            rew_bridge.app,
            host="0.0.0.0",
            port=port,
            log_level="info",
        )
        self.server = uvicorn.Server(uv_config)

        thread = threading.Thread(target=self.server.run, daemon=True)
        thread.start()
        logger.info("Uvicorn server started on port %s", port)

    def health_check_loop(self):
        """Periodically check if the bridge + REW are healthy. Runs in daemon thread."""
        port = self.config.get("bridge_port", 8080)
        url = f"http://localhost:{port}/health"

        with httpx.Client(timeout=3.0) as client:
            while not self._stop_event.is_set():
                try:
                    resp = client.get(url)
                    if resp.status_code == 200:
                        data = resp.json()
                        new_status = data.get("rew_running", False)
                    else:
                        new_status = False
                except Exception:
                    new_status = False

                if new_status != self.connected:
                    self.connected = new_status
                    if self.icon:
                        self.icon.icon = self.create_status_icon(self.connected)
                        self.icon.title = (
                            "REW SPL Bridge — Connected"
                            if self.connected
                            else "REW SPL Bridge — Disconnected"
                        )
                        self.icon.update_menu()

                self._stop_event.wait(5)

    def toggle_rew_gui(self, icon=None, item=None):
        """Toggle the Show REW GUI setting."""
        thread = threading.Thread(target=self._toggle_rew_gui_action, daemon=True)
        thread.start()

    def _toggle_rew_gui_action(self):
        """Flip rew_gui and persist to disk (runs in its own thread for tkinter safety)."""
        import tkinter as tk
        from tkinter import messagebox

        new_value = not self.config.get("rew_gui", False)
        self.config["rew_gui"] = new_value
        rew_bridge.config["rew_gui"] = new_value
        rew_bridge.save_config(self.config)

        root = tk.Tk()
        root.withdraw()
        messagebox.showinfo(
            "REW GUI Setting Changed",
            "REW GUI setting changed. Takes effect next time REW is launched.",
        )
        root.destroy()

    def change_port(self, icon=None, item=None):
        """Show a dialog to change the bridge port."""
        # tkinter dialog must run from a thread, not the tray callback thread on all OSes
        thread = threading.Thread(target=self._change_port_dialog, daemon=True)
        thread.start()

    def _change_port_dialog(self):
        """Actual port-change dialog (runs in its own thread for tkinter safety)."""
        import tkinter as tk
        from tkinter import messagebox, simpledialog

        root = tk.Tk()
        root.withdraw()

        current = self.config.get("bridge_port", 8080)
        new_port = simpledialog.askinteger(
            "Change Port",
            f"Current port: {current}\nEnter new port (1024-65535):",
            minvalue=1024,
            maxvalue=65535,
            parent=root,
        )
        if new_port is None or new_port == current:
            root.destroy()
            return

        # Check if new port is available
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", new_port))
        except OSError:
            messagebox.showwarning(
                "Port Unavailable",
                f"Port {new_port} is already in use. Choose a different port.",
            )
            root.destroy()
            return

        # Update config
        self.config["bridge_port"] = new_port
        rew_bridge.save_config(self.config)

        # Update firewall rule on Windows
        if platform.system() == "Windows":
            self._update_firewall_rule(current, new_port)

        messagebox.showinfo(
            "Port Changed",
            f"Port changed to {new_port}.\nRestart the app for this to take effect.",
        )
        root.destroy()

    def _update_firewall_rule(self, old_port: int, new_port: int):
        """Update the Windows firewall rule via UAC-elevated netsh commands."""
        try:
            # Build a combined command: delete old rule then add new one
            cmd = (
                'netsh advfirewall firewall delete rule name="REW SPL Bridge" & '
                f'netsh advfirewall firewall add rule name="REW SPL Bridge" '
                f"dir=in action=allow protocol=tcp localport={new_port}"
            )
            # Request UAC elevation via ShellExecuteW
            ctypes.windll.shell32.ShellExecuteW(
                None, "runas", "cmd.exe", f"/c {cmd}", None, 0
            )
        except Exception as e:
            logger.warning("Could not update firewall rule: %s", e)

    def open_log(self, icon=None, item=None):
        """Open the log file in the default text editor."""
        log_path = str(rew_bridge.LOG_FILE)
        if platform.system() == "Windows":
            os.startfile(log_path)
        elif platform.system() == "Darwin":
            subprocess.Popen(["open", log_path])
        else:
            subprocess.Popen(["xdg-open", log_path])

    def open_log_folder(self, icon=None, item=None):
        """Open the folder containing the log file."""
        folder = str(rew_bridge.DATA_DIR)
        if platform.system() == "Windows":
            os.startfile(folder)
        elif platform.system() == "Darwin":
            subprocess.Popen(["open", folder])
        else:
            subprocess.Popen(["xdg-open", folder])

    def quit(self, icon=None, item=None):
        """Clean shutdown: stop server, shutdown REW, release tray icon."""
        logger.info("Quit requested — shutting down")
        self._stop_event.set()

        # Force-exit safety net — start BEFORE icon.stop() in case it blocks
        def _force_exit():
            time.sleep(8)
            logger.warning("Graceful shutdown timed out, forcing exit")
            os._exit(0)

        threading.Thread(target=_force_exit, daemon=True).start()

        # Stop uvicorn — triggers lifespan shutdown which handles REW cleanup
        if self.server:
            self.server.should_exit = True

        # Release tray icon (unblocks main thread)
        if self.icon:
            self.icon.stop()

    def on_setup(self, icon):
        """Called by pystray after the icon is visible. Runs in a separate thread."""
        icon.visible = True
        self.start_server()

        health_thread = threading.Thread(target=self.health_check_loop, daemon=True)
        health_thread.start()

    def run(self):
        """Run the tray application (blocks main thread)."""
        # Load .ico file for Windows taskbar if available
        ico_path = rew_bridge.APP_DIR / "app_icon.ico"

        self.icon = pystray.Icon(
            name="REW SPL Bridge",
            icon=self.create_status_icon(False),
            title="REW SPL Bridge — Starting...",
            menu=self.build_menu(),
        )
        self.icon.run(setup=self.on_setup)


def main():
    logger.info("Starting REW SPL Meter Bridge tray application")
    tray = REWBridgeTray()
    tray.run()


if __name__ == "__main__":
    main()
