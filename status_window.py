"""Companion-style status window for the REW SPL Bridge.

Opens at launch and shows REW connection status, the dashboard address, and a
live tail of the log file, with buttons for the common actions. Closing the
window (X) hides it to the tray; Quit shuts the whole app down.

pystray owns the main thread, so this window runs tkinter in its own thread.
ALL Tk calls happen in that thread — other threads only set a flag (an Event)
that the window's periodic refresh reads. Never touch Tk objects from outside.
"""

import logging
import os
import sys
import threading

import rew_bridge

logger = logging.getLogger(__name__)

REFRESH_MS = 1000          # status + log poll cadence
INITIAL_LOG_LINES = 300    # how much existing log to show on open
MAX_LOG_LINES = 2500       # cap the text widget so it can't grow unbounded


class StatusWindow:
    """A small always-available status window, shown in its own Tk thread."""

    def __init__(self, tray):
        self.tray = tray
        self.root = None
        self._thread = None
        self._show_requested = threading.Event()
        self._log_pos = 0
        # Tk widgets (created in the window thread)
        self._log_widget = None
        self._status_var = None
        self._addr_var = None
        self._status_dot = None
        self._dot_id = None

    def start(self):
        """Open the window, or raise it to the front if already open."""
        if self._thread and self._thread.is_alive():
            self.request_show()
            return
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="status-window"
        )
        self._thread.start()

    def request_show(self):
        """Ask the window (from any thread) to un-hide and come to front."""
        self._show_requested.set()

    # ---------------------------------------------------------------------
    # Everything below runs in the window thread only.
    # ---------------------------------------------------------------------
    def _run(self):
        try:
            import tkinter as tk
            from tkinter import scrolledtext

            root = tk.Tk()
            self.root = root
            root.title(f"REW SPL Bridge v{rew_bridge.__version__}")
            root.geometry("640x470")
            root.minsize(540, 380)
            self._set_icon(root)

            # --- Header: status dot + text, dashboard address ---
            header = tk.Frame(root, padx=12, pady=10)
            header.pack(fill="x")

            self._status_dot = tk.Canvas(
                header, width=16, height=16, highlightthickness=0
            )
            self._dot_id = self._status_dot.create_oval(
                2, 2, 14, 14, fill="#888888", outline=""
            )
            self._status_dot.grid(row=0, column=0, padx=(0, 8))

            self._status_var = tk.StringVar(value="REW: starting…")
            tk.Label(
                header, textvariable=self._status_var,
                font=("Segoe UI", 12, "bold"),
            ).grid(row=0, column=1, sticky="w")

            self._addr_var = tk.StringVar(value="Dashboard: …")
            tk.Label(
                header, textvariable=self._addr_var, font=("Segoe UI", 10),
                fg="#1a5fb4", cursor="hand2",
            ).grid(row=1, column=1, sticky="w", pady=(4, 0))

            # --- Buttons ---
            btns = tk.Frame(root, padx=12, pady=4)
            btns.pack(fill="x")
            tk.Button(btns, text="Open Dashboard",
                      command=self.tray.open_dashboard).pack(side="left")
            tk.Button(btns, text="Open Log",
                      command=self.tray.open_log).pack(side="left", padx=6)
            tk.Button(btns, text="Log Folder",
                      command=self.tray.open_log_folder).pack(side="left")
            tk.Button(btns, text="Change Port…",
                      command=self.tray.change_port).pack(side="left", padx=6)
            tk.Button(btns, text="Quit", fg="#b00020",
                      command=self._quit).pack(side="right")
            tk.Button(btns, text="Hide to Tray",
                      command=self._hide).pack(side="right", padx=6)

            # --- Live log pane ---
            tk.Label(root, text="Log", anchor="w", padx=12,
                     fg="#666666").pack(fill="x")
            logframe = tk.Frame(root, padx=12)
            logframe.pack(fill="both", expand=True, pady=(0, 12))
            self._log_widget = scrolledtext.ScrolledText(
                logframe, wrap="word", state="disabled",
                font=("Consolas", 9), bg="#0e0e11", fg="#d8d8de",
                insertbackground="#d8d8de",
            )
            self._log_widget.pack(fill="both", expand=True)

            root.protocol("WM_DELETE_WINDOW", self._hide)

            self._load_log_initial()
            self._refresh()
            root.mainloop()
        except Exception:
            # The window is a convenience; never let it take the app down.
            logger.exception("Status window thread crashed")

    def _set_icon(self, root):
        try:
            base = getattr(sys, "_MEIPASS", None) or str(rew_bridge.APP_DIR)
            ico = os.path.join(base, "app_icon.ico")
            if os.path.exists(ico):
                root.iconbitmap(ico)
        except Exception:
            pass  # icon is cosmetic

    def _load_log_initial(self):
        """Show the tail of the existing log and remember the byte position."""
        try:
            with open(rew_bridge.LOG_FILE, "rb") as f:
                data = f.read()
            self._log_pos = len(data)
            text = data.decode("utf-8", "replace")
            tail = "\n".join(text.splitlines()[-INITIAL_LOG_LINES:])
            self._append(tail + ("\n" if tail else ""))
        except OSError:
            self._log_pos = 0

    def _poll_log(self):
        """Append any bytes written to the log since last poll (handles rotation)."""
        try:
            size = os.path.getsize(rew_bridge.LOG_FILE)
            if size < self._log_pos:  # file rotated/truncated
                self._log_pos = 0
                self._append("\n--- log rotated ---\n")
            if size > self._log_pos:
                with open(rew_bridge.LOG_FILE, "rb") as f:
                    f.seek(self._log_pos)
                    chunk = f.read()
                self._log_pos += len(chunk)
                self._append(chunk.decode("utf-8", "replace"))
        except OSError:
            pass

    def _append(self, text):
        if not text:
            return
        w = self._log_widget
        at_bottom = w.yview()[1] >= 0.999
        w.configure(state="normal")
        w.insert("end", text)
        line_count = int(w.index("end-1c").split(".")[0])
        if line_count > MAX_LOG_LINES:
            w.delete("1.0", f"{line_count - MAX_LOG_LINES + 1}.0")
        w.configure(state="disabled")
        if at_bottom:
            w.see("end")

    def _refresh(self):
        connected = bool(getattr(self.tray, "connected", False))
        if connected:
            self._status_dot.itemconfig(self._dot_id, fill="#16c060")
            self._status_var.set("REW: Connected")
        else:
            self._status_dot.itemconfig(self._dot_id, fill="#ff3b30")
            self._status_var.set("REW: Disconnected — starting or not reachable")

        try:
            import tray_app  # lazy import avoids a circular import at module load
            ip = tray_app.get_local_ip()
        except Exception:
            ip = "localhost"
        port = self.tray.config.get("bridge_port", 8080)
        self._addr_var.set(f"Dashboard:  http://{ip}:{port}/")

        self._poll_log()

        if self._show_requested.is_set():
            self._show_requested.clear()
            try:
                self.root.deiconify()
                self.root.lift()
                self.root.focus_force()
            except Exception:
                pass

        self.root.after(REFRESH_MS, self._refresh)

    def _hide(self):
        try:
            self.root.withdraw()
        except Exception:
            pass

    def _quit(self):
        # Shut down the whole app (server + REW + tray) off the UI thread,
        # then tear down the window so its mainloop returns.
        threading.Thread(target=self.tray.quit, daemon=True).start()
        try:
            self.root.destroy()
        except Exception:
            pass
