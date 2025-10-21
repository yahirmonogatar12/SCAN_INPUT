import os
import sys
import gc
import logging
from contextlib import contextmanager
from typing import Optional

# Configurar logging temprano
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class PerformanceOptimizer:
    """Centralizador de optimizaciones"""
    
    @staticmethod
    def optimize_for_production():
        """Optimizaciones para PCs lentas en producción"""
        
        # 1. Optimización del Garbage Collector
        gc.disable()
        
        # 2. Variables de entorno Qt optimizadas
        qt_optimizations = {
            'QT_ENABLE_HIGHDPI_SCALING': '0',
            'QT_AUTO_SCREEN_SCALE_FACTOR': '0', 
            'QT_SCALE_FACTOR': '1',
            'QT_QUICK_BACKEND': 'software',  # Evita aceleración GPU
            'QT_OPENGL': 'software',
            'QT_LOGGING_RULES': '*.debug=false;qt.qpa.gl=false'  # Reduce logs Qt
        }
        
        # 3. Optimización Python
        python_optimizations = {
            'PYTHONOPTIMIZE': '2',
            'PYTHONDONTWRITEBYTECODE': '1',  # No crear .pyc
            'PYTHONUNBUFFERED': '1'
        }
        
        # 4. SQLite optimizado para dual DB
        sqlite_optimizations = {
            'SQLITE_THREADSAFE': '2',
            'SQLITE_DEFAULT_CACHE_SIZE': '-20000',  # 20MB cache
            'SQLITE_DEFAULT_PAGE_SIZE': '8192',
            'SQLITE_DEFAULT_SYNCHRONOUS': 'NORMAL',
            'SQLITE_DEFAULT_JOURNAL_MODE': 'WAL',
            'SQLITE_DEFAULT_TEMP_STORE': 'MEMORY',
            'SQLITE_DEFAULT_MMAP_SIZE': '268435456'  # 256MB mmap
        }
        
        # 5. Configuración de aplicación
        app_optimizations = {
            'APP_PERFORMANCE_MODE': 'OPTIMIZED',
            'DISABLE_ANIMATIONS': '1',
            'REDUCE_UI_UPDATES': '1',
            'BATCH_DB_OPERATIONS': '1',
            'USE_CONNECTION_POOL': '1',
            'MAX_RETRY_ATTEMPTS': '3'
        }
        
        # Aplicar todas las optimizaciones
        for configs in [qt_optimizations, python_optimizations, 
                       sqlite_optimizations, app_optimizations]:
            os.environ.update(configs)
        
        # Re-configurar GC optimizado
        gc.enable()
        gc.set_threshold(1000, 15, 15)  # Ajustado para mejor balance


def _resolve_base_dir() -> str:
    """Resuelve directorio base con manejo de errores"""
    try:
        if hasattr(sys, "_MEIPASS") and sys._MEIPASS:
            base = sys._MEIPASS
            exe_dir = os.path.dirname(sys.executable)
            internal_dir = os.path.join(exe_dir, "_internal")
            return internal_dir if os.path.isdir(internal_dir) else base
        return os.path.abspath(os.path.dirname(__file__))
    except Exception as e:
        logger.error(f"Error resolviendo directorio base: {e}")
        return os.getcwd()


def _add_dll_dir(path: str) -> None:
    """Agrega directorio DLL con fallback"""
    if not os.path.isdir(path):
        return
        
    try:
        if hasattr(os, 'add_dll_directory'):
            os.add_dll_directory(path)
        else:
            os.environ["PATH"] = path + os.pathsep + os.environ.get("PATH", "")
    except Exception as e:
        logger.debug(f"No se pudo agregar DLL dir {path}: {e}")


def _bootstrap_qt_paths() -> None:
    """Configura paths Qt con validación"""
    base = _resolve_base_dir()
    
    # Buscar binarios Qt
    bin_candidates = [
        os.path.join(base, "PyQt6", "Qt6", "bin"),
        os.path.join(base, "Qt6", "bin"),
        os.path.join(base, "Qt6"),
        base,
    ]
    
    for p in bin_candidates:
        if os.path.isdir(p):
            _add_dll_dir(p)
            break
    
    # Buscar plugins Qt
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


@contextmanager
def startup_optimization():
    """Context manager para optimización durante startup"""
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


if __name__ == "__main__":
    try:
        # Configurar AppUserModelID para Windows
        if sys.platform == 'win32':
            try:
                import ctypes
                myappid = 'IMD.InputScan.Production.1.0'
                ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
            except Exception:
                pass
        
        # Aplicar optimizaciones
        optimizer = PerformanceOptimizer()
        optimizer.optimize_for_production()
        
        # Bootstrap Qt
        _bootstrap_qt_paths()
        
        # Iniciar aplicación con optimización
        with startup_optimization():
            from app.main import main
            sys.exit(main())
            
    except KeyboardInterrupt:
        print("\nAplicación cerrada por usuario")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Error fatal: {e}", exc_info=True)
        sys.exit(1)

