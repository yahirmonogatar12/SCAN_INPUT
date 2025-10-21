from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List

from ..config import settings
from ..db import get_db
from ..models import ResumenProduccion, ScanRecord
from .dual_db import get_dual_db
import logging

logger = logging.getLogger(__name__)


def get_last_scans(limit: int = 100) -> List[ScanRecord]:
    """Obtiene los últimos escaneos desde SQLite local (ultra-rápido)"""
    try:
        dual_db = get_dual_db()
        return dual_db.get_last_scans_local(limit)
    except Exception as e:
        logger.error(f"Error obteniendo últimos escaneos desde SQLite: {e}")
        # Fallback a MySQL si falla SQLite
        try:
            db = get_db()
            return db.get_last_scans(limit)
        except:
            return []


def get_today_totals() -> List[ResumenProduccion]:
    """Obtiene totales del día desde SQLite local (ultra-rápido)"""
    try:
        dual_db = get_dual_db()
        return dual_db.get_today_totals_local()
    except Exception as e:
        logger.error(f"Error obteniendo totales desde SQLite: {e}")
        # Fallback a MySQL si falla SQLite
        try:
            db = get_db()
            return db.get_today_totales()
        except:
            return []

