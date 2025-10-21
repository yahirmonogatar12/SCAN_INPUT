import os
import sys
from pathlib import Path


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        # Quita comillas simples/dobles alrededor si las hay
        val = val.strip().strip("\"'")
        os.environ.setdefault(key, val)


# Detect base directory depending on whether the app is frozen by PyInstaller.
# In onedir/onefile, sys._MEIPASS points to the internal temp/base dir that contains our bundled data.
FROZEN_BASE = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parents[1]))
ROOT_DIR = FROZEN_BASE

# ✅ DIRECTORIO DE INSTALACIÓN (donde se guarda .env persistente)
# Si está congelado (PyInstaller), el ejecutable está en el directorio de instalación
if getattr(sys, 'frozen', False):
    # Congelado: sys.executable apunta al .exe en el directorio de instalación
    INSTALL_DIR = Path(sys.executable).parent
else:
    # Desarrollo: usar el directorio del proyecto
    INSTALL_DIR = Path(__file__).resolve().parents[1]


def _ensure_env_file() -> None:
    """Asegura que el archivo .env exista. Si no existe, lo crea desde .env.example.
    ⚠️ DEBE LLAMARSE ANTES DE load_env_file() para que Settings se inicialice correctamente.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    env_path = INSTALL_DIR / ".env"
    env_example = INSTALL_DIR / ".env.example"
    
    if not env_path.exists():
        logger.warning(f"⚠️ .env no existe en {env_path}")
        
        # Intentar copiar desde .env.example
        if env_example.exists():
            try:
                import shutil
                shutil.copy2(env_example, env_path)
                logger.info(f"✅ .env creado desde .env.example")
            except Exception as e:
                logger.error(f"❌ Error copiando .env.example: {e}")
        else:
            # Crear .env básico si no existe example
            logger.info(f"📝 Creando .env básico...")
            default_content = """# CONFIGURACIÓN DE INPUT SCAN
APP_MODE=ASSY
DEFAULT_LINE=M1
SOLO_QR_MODE=0
SUB_ASSY_MODE=0
NUM_PERSONAS_LINEA=14
AUTO_CHECK_UPDATES=true
AUTO_INSTALL_UPDATES=true
SILENT_UPDATE_INSTALL=false
"""
            try:
                env_path.write_text(default_content, encoding="utf-8")
                logger.info(f"✅ .env básico creado")
            except Exception as e:
                logger.error(f"❌ Error creando .env: {e}")
    else:
        logger.debug(f"✅ .env existe en {env_path}")


# ✅ ASEGURAR que .env exista ANTES de cargarlo
_ensure_env_file()

# ✅ Cargar .env desde el directorio de instalación (persistente)
load_env_file(INSTALL_DIR / ".env")

# 🔍 DEBUG: Log de directorios importantes al inicio
import logging
_logger = logging.getLogger(__name__)
_logger.info(f"📁 ROOT_DIR (recursos): {ROOT_DIR}")
_logger.info(f"📁 INSTALL_DIR (config): {INSTALL_DIR}")
_logger.info(f"📄 .env path: {INSTALL_DIR / '.env'}")
_logger.info(f"📄 .env exists: {(INSTALL_DIR / '.env').exists()}")


class Settings:
    APP_ENV: str = os.getenv("APP_ENV", "development")
    TZ: str = os.getenv("TZ", "America/Monterrey")

    # Directorio de logs en Documentos del usuario para evitar problemas de permisos
    DOCUMENTS_DIR: Path = Path.home() / "Documents" / "SMD_Scanner_Logs"
    LOG_DIR: Path = Path(os.getenv("LOG_DIR", str(DOCUMENTS_DIR)))
    
    # Directorio de datos - usar AppData si está instalado en Program Files
    if "Program Files" in str(ROOT_DIR):
        # Si está instalado, usar AppData para datos persistentes
        DATA_DIR: Path = Path.home() / "AppData" / "Local" / "SMD_Scanner" / "data"
    else:
        # Si está en desarrollo, usar el directorio local
        DATA_DIR: Path = ROOT_DIR / "data"

    DB_ENGINE: str = os.getenv("DB_ENGINE", "mysql").lower()
    SQLITE_PATH: Path = Path(os.getenv("SQLITE_PATH", str(DATA_DIR / "app.db")))
    # Ruta de la base de datos local usada por el sistema dual (SQLite cache)
    LOCAL_SQLITE_PATH: Path = Path(os.getenv("LOCAL_SQLITE_PATH", str(DATA_DIR / "local_cache.db")))

    # Configuración MySQL (ahora principal)
    MYSQL_HOST: str = os.getenv("MYSQL_HOST", "up-de-fra1-mysql-1.db.run-on-seenode.com")
    MYSQL_PORT: int = int(os.getenv("MYSQL_PORT", "11550"))
    MYSQL_DB: str = os.getenv("MYSQL_DB", "db_rrpq0erbdujn")
    MYSQL_USER: str = os.getenv("MYSQL_USER", "db_rrpq0erbdujn")
    MYSQL_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "5fUNbSRcPP3LN9K2I33Pr0ge")

    # Modo Solo QR (no requiere BARCODE; completa par con placeholder)
    SOLO_QR_MODE: bool = os.getenv("SOLO_QR_MODE", "0").strip() in ("1", "true", "True")

    # Modo SUB ASSY (validación BARCODE con QR usando tabla raw)
    SUB_ASSY_MODE: bool = os.getenv("SUB_ASSY_MODE", "0").strip() in ("1", "true", "True")

    # Línea predeterminada para UI y registros
    _default_line_raw = os.getenv("DEFAULT_LINE", "M1")
    DEFAULT_LINE: str = _default_line_raw.strip()
    
    # 🔍 DEBUG CRÍTICO: Log del valor cargado
    import logging
    _settings_logger = logging.getLogger(__name__)
    _settings_logger.info(f"🎯 DEFAULT_LINE cargado desde .env: '{DEFAULT_LINE}' (raw: '{_default_line_raw}')")

    # Modo de operación: ASSY (actual) o IMD (nuevo)
    APP_MODE: str = os.getenv("APP_MODE", "ASSY").strip().upper()
    
    # ⚡ Número de personas en línea (para calcular UPPH = UPH / Personas)
    NUM_PERSONAS_LINEA: int = int(os.getenv("NUM_PERSONAS_LINEA", "14"))

    # ===================================================================================================
    # CONFIGURACIÓN DE AUTO-ACTUALIZACIÓN
    # ===================================================================================================
    # Versión actual del programa - lee de version.txt si existe
    def _get_version() -> str:
        """Lee la versión del archivo version.txt con logging detallado."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Intentar múltiples ubicaciones para version.txt
        possible_paths = [
            ROOT_DIR / "version.txt",  # En _MEIPASS (directorio temporal de PyInstaller)
            Path(sys.executable).parent / "version.txt",  # Junto al .exe (directorio de instalación)
            Path(sys.executable).parent / "_internal" / "version.txt",  # En _internal (PyInstaller ONEDIR)
            Path(__file__).resolve().parent.parent / "version.txt",  # Directorio raíz del proyecto (desarrollo)
        ]
        
        logger.debug("🔍 Buscando version.txt...")
        logger.debug(f"  ROOT_DIR: {ROOT_DIR}")
        logger.debug(f"  sys.executable: {sys.executable}")
        logger.debug(f"  __file__: {__file__}")
        
        for version_file in possible_paths:
            try:
                if version_file.exists():
                    # Usar 'utf-8-sig' para eliminar automáticamente el BOM
                    version = version_file.read_text(encoding='utf-8-sig').strip()
                    logger.info(f"✅ Versión leída de {version_file}: {version}")
                    return version
                else:
                    logger.debug(f"  ❌ No existe: {version_file}")
            except Exception as e:
                logger.error(f"  ❌ Error leyendo {version_file}: {e}")
        
        logger.warning("⚠️ No se encontró version.txt en ninguna ubicación, usando versión por defecto 1.0.0")
        logger.warning(f"  Ubicaciones buscadas: {[str(p) for p in possible_paths]}")
        return "1.0.0"
    
    APP_VERSION: str = _get_version()
    
    # Ruta de red donde se encuentran las actualizaciones
    # Ejemplo: \\SERVER\SharedFolder\InputScan_Updates
    # O en formato Windows: \\192.168.1.100\Updates\InputScan
    UPDATE_NETWORK_PATH: str = os.getenv("UPDATE_NETWORK_PATH", r"\\192.168.1.230\develop\MES\PRODUCCION ASSY\input_scan")
    
    # Credenciales de red para acceder a la carpeta compartida
    UPDATE_NETWORK_USER: str = os.getenv("UPDATE_NETWORK_USER", "isemm06")
    UPDATE_NETWORK_PASSWORD: str = os.getenv("UPDATE_NETWORK_PASSWORD", "roqkf06!")
    
    # Habilitar verificación automática de actualizaciones al iniciar
    AUTO_CHECK_UPDATES: bool = os.getenv("AUTO_CHECK_UPDATES", "true").lower() == "true"
    
    # Instalar actualizaciones automáticamente sin preguntar (requiere AUTO_CHECK_UPDATES=true)
    AUTO_INSTALL_UPDATES: bool = os.getenv("AUTO_INSTALL_UPDATES", "true").lower() == "true"
    
    # Instalación silenciosa (sin mostrar ventanas del instalador)
    SILENT_UPDATE_INSTALL: bool = os.getenv("SILENT_UPDATE_INSTALL", "false").lower() == "true"


settings = Settings()
settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
if settings.DB_ENGINE == "sqlite":
    settings.SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)

# Asegurar carpeta para DB local del sistema dual
settings.LOCAL_SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)


def update_env_var(key: str, value: str) -> None:
    """Actualiza o agrega una variable en el archivo .env en la raíz.

    Conserva líneas no relacionadas y comentarios. Si el archivo no existe,
    lo crea. No hace validación de tipos; guarda texto literal.
    """
    # ✅ Usar INSTALL_DIR para persistencia (no ROOT_DIR que es temporal)
    env_path = INSTALL_DIR / ".env"
    
    # 🔍 DEBUG: Log para rastrear escrituras
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"💾 Guardando {key}={value} en {env_path}")
    
    lines = []
    if env_path.exists():
        try:
            content = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            lines = content
            logger.info(f"📖 .env existe con {len(lines)} líneas")
        except Exception as e:
            # Si por alguna razón no se puede leer, continuamos con lista vacía
            logger.warning(f"⚠️ Error leyendo .env: {e}")
            lines = []
    else:
        logger.info(f"📄 .env no existe, se creará en {env_path}")

    key_eq = f"{key}="
    updated = False
    new_lines = []
    for line in lines:
        if line.strip().startswith("#") or "=" not in line:
            new_lines.append(line)
            continue
        k, _sep, _v = line.partition("=")
        if k.strip() == key:
            new_lines.append(f"{key}={value}")
            updated = True
            logger.info(f"✏️ Actualizada línea existente: {key}={value}")
        else:
            new_lines.append(line)

    if not updated:
        new_lines.append(f"{key}={value}")
        logger.info(f"➕ Agregada nueva línea: {key}={value}")

    try:
        env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        logger.info(f"✅ .env guardado exitosamente con {len(new_lines)} líneas")
    except Exception as e:
        logger.error(f"❌ Error guardando .env: {e}")
