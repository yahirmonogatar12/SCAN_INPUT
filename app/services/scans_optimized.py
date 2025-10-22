"""
Servicio de escaneo OPTIMIZADO - Directo a MySQL sin parseo local
"""
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Tuple
import logging

from ..config import settings
from .direct_mysql import get_direct_mysql

logger = logging.getLogger(__name__)


def process_scan_direct(raw: str, linea: str = "M1") -> int:
    """
    Procesamiento usando dual_db (SQLite local + MySQL sync)
    
    Args:
        raw: Código escaneado (QR o BARCODE)
        linea: Línea de producción
    
    Returns:
        > 0: ID del escaneo insertado (éxito)
        -1: Error general o formato inválido
        -2: Escaneo duplicado
        -3: N° parte no está en el plan del día
        -4: N° parte no está EN PROGRESO
        -10: Modelo diferente al plan EN PROGRESO
    """
    try:
        # Validación básica
        if not raw or not raw.strip():
            return -1
        
        raw = raw.strip()
        
        # ✅ USAR DUAL_DB para que se incremente produced_count
        from .dual_db import get_dual_db
        dual_db = get_dual_db()
        
        # Insertar usando dual_db (maneja incremento automático)
        scan_id = dual_db.add_scan_fast(raw, linea)
        
        if scan_id > 0:
            logger.info(f"✅ Escaneo procesado: ID {scan_id}")
        elif scan_id == 0:
            logger.debug(f"� Duplicado ignorado silenciosamente: {raw[:30]}...")
        elif scan_id == -3:
            logger.warning(f"📋 Fuera de plan: {raw[:30]}...")
        elif scan_id == -4:
            logger.warning(f"⏸️ No está EN PROGRESO: {raw[:30]}...")
        elif scan_id == -7:
            logger.warning(f"🚫 SUB ASSY validation failed: {raw[:30]}...")
        elif scan_id == -8:
            logger.warning(f"🚫 QR duplicado rechazado: {raw[:30]}...")
        elif scan_id == -10:
            logger.warning(f"🚫 Modelo diferente al plan EN PROGRESO: {raw[:30]}...")
        else:
            # Mostrar código de error específico para debugging
            logger.error(f"❌ Error procesando (código {scan_id}): {raw[:30]}...")
        
        return scan_id
        
    except Exception as e:
        logger.error(f"Error en process_scan_direct: {e}")
        return -1


def get_production_counts_cached(linea: str = None) -> Dict[str, int]:
    """
    Obtiene conteos de producción desde cache (actualizado cada 15 seg)
    
    Args:
        linea: Línea específica o None para todas
    
    Returns:
        {nparte: cantidad, ...}
    """
    try:
        direct_mysql = get_direct_mysql()
        return direct_mysql.get_cached_counts(linea)
    except Exception as e:
        logger.error(f"Error obteniendo conteos: {e}")
        return {}


def get_production_summary(linea: str) -> Dict:
    """
    Obtiene resumen de producción para una línea
    Incluye plan vs producido, avance, UPH, etc.
    
    Returns:
        {
            'total_plan': int,
            'total_producido': int,
            'avance_pct': float,
            'parts': [...]
        }
    """
    try:
        direct_mysql = get_direct_mysql()
        plan_progress = direct_mysql.get_plan_progress(linea)
        
        total_plan = sum(p['plan'] for p in plan_progress)
        total_producido = sum(p['producido'] for p in plan_progress)
        avance_pct = (total_producido / total_plan * 100) if total_plan > 0 else 0
        
        return {
            'total_plan': total_plan,
            'total_producido': total_producido,
            'avance_pct': round(avance_pct, 1),
            'parts': plan_progress
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo resumen: {e}")
        return {
            'total_plan': 0,
            'total_producido': 0,
            'avance_pct': 0,
            'parts': []
        }


def get_last_scans(limit: int = 100) -> list:
    """Obtiene últimos escaneos desde MySQL"""
    try:
        direct_mysql = get_direct_mysql()
        return direct_mysql.get_last_scans(limit)
    except Exception as e:
        logger.error(f"Error obteniendo últimos escaneos: {e}")
        return []


# ============================================
# FUNCIONES DE COMPATIBILIDAD
# ============================================

def process_scan_fast(raw: str, linea: str = "M1") -> int:
    """
    Wrapper para compatibilidad con código existente
    Redirige a process_scan_direct
    """
    return process_scan_direct(raw, linea)


def process_scan(raw: str, linea: str = "M1") -> int:
    """
    Wrapper para compatibilidad con código existente
    Redirige a process_scan_direct
    """
    return process_scan_direct(raw, linea)
