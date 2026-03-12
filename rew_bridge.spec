# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec file for REW SPL Meter Bridge."""

from PyInstaller.utils.hooks import collect_submodules

# Collect all uvicorn submodules (it uses dynamic imports)
uvicorn_imports = collect_submodules("uvicorn")

a = Analysis(
    ["tray_app.py"],
    pathex=[],
    binaries=[],
    datas=[
        ("app_icon.ico", "."),
        ("config.example.json", "."),
    ],
    hiddenimports=[
        *uvicorn_imports,
        "pystray._win32",
        "httpcore",
        "anyio",
        "anyio._backends",
        "anyio._backends._asyncio",
        "h11",
        "sniffio",
        "email.mime.multipart",
        "email.mime.text",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="REW SPL Bridge",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    icon="app_icon.ico",
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="REW SPL Bridge",
)
