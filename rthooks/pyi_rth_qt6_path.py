# Ensures Qt6 DLL and plugin directories are discoverable at runtime
import os
import sys


def _resolve_base_dir() -> str:
    """Return base directory where bundled resources live.

    - onefile: sys._MEIPASS points to extraction dir.
    - onedir: prefer <exe_dir>\\_internal if present; else <exe_dir>.
    """
    if hasattr(sys, "_MEIPASS") and sys._MEIPASS:
        return sys._MEIPASS
    exe_dir = os.path.abspath(os.path.dirname(sys.executable))
    internal_dir = os.path.join(exe_dir, "_internal")
    return internal_dir if os.path.isdir(internal_dir) else exe_dir


def _add_dll_dir(path: str) -> None:
    try:
        os.add_dll_directory(path)
    except Exception:
        os.environ["PATH"] = path + os.pathsep + os.environ.get("PATH", "")


def _add_qt_paths():
    base = _resolve_base_dir()

    # Candidate directories for Qt6 DLLs (prefer PyQt6/Qt6/bin as per spec)
    bin_candidates = [
        os.path.join(base, "PyQt6", "Qt6", "bin"),
        os.path.join(base, "Qt6", "bin"),
        os.path.join(base, "Qt6"),
        base,
    ]

    for p in bin_candidates:
        if os.path.isdir(p):
            _add_dll_dir(p)

    # Candidate plugin directories (prefer ones under our base)
    plugin_candidates = [
        os.path.join(base, "PyQt6", "Qt6", "plugins"),
        os.path.join(base, "Qt6", "plugins"),
        os.path.join(base, "plugins"),
    ]

    for q in plugin_candidates:
        if os.path.isdir(q):
            os.environ["QT_PLUGIN_PATH"] = q
            plat = os.path.join(q, "platforms")
            if os.path.isdir(plat):
                os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = plat
            break


_add_qt_paths()
