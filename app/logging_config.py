import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import os
from .config import settings


def setup_logging() -> None:
    log_file: Path = settings.LOG_DIR / "app.log"
    
    # Crear directorio de logs si no existe
    settings.LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    
    # Modo optimizado: solo WARNING+ en PCs lentas
    is_optimized = os.environ.get('APP_PERFORMANCE_MODE') == 'OPTIMIZED'
    log_level = logging.WARNING if is_optimized else logging.INFO
    logger.setLevel(log_level)

    # Console handler (ajustado según modo)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch_formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s" if is_optimized else "%(asctime)s %(levelname)s %(name)s: %(message)s")
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)

    # Rotating file handler (reducido en modo optimizado)
    max_bytes = 500_000 if is_optimized else 1_000_000  # 500KB vs 1MB
    backup_count = 3 if is_optimized else 5
    
    fh = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    fh.setLevel(log_level)
    
    # Formato simplificado en modo optimizado
    if is_optimized:
        fh_formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    else:
        fh_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s [%(filename)s:%(lineno)d]: %(message)s")
    
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)

