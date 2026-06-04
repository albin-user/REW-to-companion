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
LOG_VISIBLE_LINES = 8      # how many log lines are visible at once

# Palette — matches the web dashboard (dashboard.py)
BG = "#0a0a0c"          # window background
CARD = "#141417"        # raised panel
EDGE = "#26262b"        # hairline borders
MUTED = "#8a8a93"       # secondary text
TEXT = "#f2f2f5"        # primary text
GREEN = "#16c060"
RED = "#ff3b30"
ACCENT = "#1f6f3f"      # primary button
ACCENT_HOVER = "#27814b"
BTN = "#1d1d22"         # secondary button
BTN_HOVER = "#26262d"
LOG_BG = "#08080a"
LOG_FG = "#cfcfd6"


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
            root.title("REW SPL Bridge")
            root.configure(bg=BG)
            root.geometry("560x430")
            root.minsize(480, 380)
            self._set_icon(root)

            outer = tk.Frame(root, bg=BG, padx=16, pady=14)
            outer.pack(fill="both", expand=True)

            # --- Title row ---
            titlerow = tk.Frame(outer, bg=BG)
            titlerow.pack(fill="x")
            tk.Label(titlerow, text="REW SPL Bridge", bg=BG, fg=TEXT,
                     font=("Segoe UI Semibold", 15)).pack(side="left")
            tk.Label(titlerow, text=f"v{rew_bridge.__version__}", bg=BG,
                     fg=MUTED, font=("Segoe UI", 10)).pack(side="left",
                                                           padx=(8, 0), pady=(5, 0))

            # --- Status card ---
            card = tk.Frame(outer, bg=CARD, highlightbackground=EDGE,
                            highlightthickness=1, padx=14, pady=12)
            card.pack(fill="x", pady=(12, 14))

            statusrow = tk.Frame(card, bg=CARD)
            statusrow.pack(fill="x")
            self._status_dot = tk.Canvas(statusrow, width=14, height=14,
                                         bg=CARD, highlightthickness=0)
            self._dot_id = self._status_dot.create_oval(
                1, 1, 13, 13, fill=MUTED, outline="")
            self._status_dot.pack(side="left", pady=(2, 0))
            self._status_var = tk.StringVar(value="Starting…")
            tk.Label(statusrow, textvariable=self._status_var, bg=CARD, fg=TEXT,
                     font=("Segoe UI Semibold", 12)).pack(side="left", padx=(9, 0))

            self._addr_var = tk.StringVar(value="http://…")
            tk.Label(card, text="DASHBOARD", bg=CARD, fg=MUTED,
                     font=("Segoe UI", 8)).pack(anchor="w", pady=(11, 0))
            tk.Label(card, textvariable=self._addr_var, bg=CARD, fg="#7db1ff",
                     font=("Consolas", 11)).pack(anchor="w")

            primary = self._make_button(card, "Open Dashboard",
                                        self.tray.open_dashboard, primary=True)
            primary.pack(anchor="w", pady=(12, 0))

            # --- Secondary actions ---
            actions = tk.Frame(outer, bg=BG)
            actions.pack(fill="x", pady=(0, 12))
            for label, cmd in (
                ("Open Log", self.tray.open_log),
                ("Log Folder", self.tray.open_log_folder),
                ("Change Port", self.tray.change_port),
            ):
                self._make_button(actions, label, cmd).pack(side="left",
                                                            padx=(0, 8))

            # --- Live log pane (≈8 lines) ---
            tk.Label(outer, text="LOG", bg=BG, fg=MUTED,
                     font=("Segoe UI", 8)).pack(anchor="w")
            logframe = tk.Frame(outer, bg=EDGE, highlightbackground=EDGE,
                                highlightthickness=1)
            logframe.pack(fill="x", pady=(4, 12))
            self._log_widget = scrolledtext.ScrolledText(
                logframe, wrap="none", state="disabled", height=LOG_VISIBLE_LINES,
                font=("Consolas", 9), bg=LOG_BG, fg=LOG_FG, bd=0,
                relief="flat", padx=8, pady=6, insertbackground=LOG_FG,
            )
            self._log_widget.pack(fill="both", expand=True)

            # --- Footer: hide / quit ---
            footer = tk.Frame(outer, bg=BG)
            footer.pack(fill="x")
            self._make_button(footer, "Quit", self._quit,
                              danger=True).pack(side="right")
            self._make_button(footer, "Hide to Tray",
                              self._hide).pack(side="right", padx=(0, 8))

            root.protocol("WM_DELETE_WINDOW", self._hide)

            self._load_log_initial()
            self._refresh()

            # Size the window to fit its content exactly, then lock vertical
            # resizing so the log pane stays at LOG_VISIBLE_LINES lines.
            root.update_idletasks()
            root.geometry(f"560x{root.winfo_reqheight()}")
            root.resizable(True, False)

            root.mainloop()
        except Exception:
            # The window is a convenience; never let it take the app down.
            logger.exception("Status window thread crashed")

    def _make_button(self, parent, text, command, primary=False, danger=False):
        """A flat, themed button with a hover state."""
        import tkinter as tk

        bg, hover = (ACCENT, ACCENT_HOVER) if primary else (BTN, BTN_HOVER)
        fg = "#ff7a72" if danger else "#ffffff"
        btn = tk.Button(
            parent, text=text, command=command, relief="flat",
            bg=bg, fg=fg, activebackground=hover, activeforeground=fg,
            font=("Segoe UI", 9), bd=0, padx=14, pady=7,
            cursor="hand2", highlightthickness=0,
        )
        btn.bind("<Enter>", lambda e: btn.configure(bg=hover))
        btn.bind("<Leave>", lambda e: btn.configure(bg=bg))
        return btn

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
            self._status_dot.itemconfig(self._dot_id, fill=GREEN)
            self._status_var.set("Connected to REW")
        else:
            self._status_dot.itemconfig(self._dot_id, fill=RED)
            self._status_var.set("Not connected — starting or unreachable")

        try:
            import tray_app  # lazy import avoids a circular import at module load
            ip = tray_app.get_local_ip()
        except Exception:
            ip = "localhost"
        port = self.tray.config.get("bridge_port", 8080)
        self._addr_var.set(f"http://{ip}:{port}/")

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
