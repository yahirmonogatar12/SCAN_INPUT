from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Tuple
import threading
import time
import logging

from ..config import settings
from ..db import get_db
from ..models import ParsedScan, ScanRecord
from .parser import parse_scan, detect_scan_format

logger = logging.getLogger(__name__)
from .local_queue import get_local_queue
from .dual_db import get_dual_db

# Caché de modelos en memoria para evitar consultas repetitivas
_modelo_cache: Dict[str, Tuple[str, float]] = {}  # nparte -> (modelo, timestamp)
_cache_lock = threading.Lock()
CACHE_EXPIRY = 300  # 5 minutos


def get_modelo_cached(nparte: str) -> str:
    """Cache de modelos desde SQLite local (ultra-rápido, NO bloquea)"""
    if nparte in _modelo_cache:
        return _modelo_cache[nparte]
    
    # Consultar desde SQLite local y cachear en memoria (NO bloquea)
    try:
        dual_db = get_dual_db()
        # fetch_if_missing=True para programar fetch en background si falta
        modelo, uph = dual_db.get_modelo_local(nparte, fetch_if_missing=True)
        
        # Cache en memoria (máximo 1000 entradas)
        if modelo and len(_modelo_cache) < 1000:
            _modelo_cache[nparte] = modelo
            
        return modelo
    except Exception:
        return None


def process_scan(raw: str, linea: str = "M1") -> ParsedScan:
    """Procesamiento tradicional directo a MySQL (para compatibilidad)"""
    from datetime import date
    from zoneinfo import ZoneInfo
    from ..models.entities import ScanRecord
    from ..db import get_db
    
    parsed: ParsedScan = parse_scan(raw)
    
    # Obtener modelo desde caché optimizado
    modelo = get_modelo_cached(parsed.nparte)
    
    # Crear record de escaneo
    scan_record = ScanRecord(
        id=None,  # Se asignará por MySQL
        ts=datetime.now(ZoneInfo(settings.TZ)),
        raw=raw,
        tipo=parsed.tipo,
        fecha=date.today(),
        lote=parsed.lote,
        secuencia=parsed.secuencia,
        estacion=parsed.estacion,
        nparte=parsed.nparte,
        modelo=modelo,
        cantidad=parsed.cantidad,
        linea=linea
    )
    
    # Insertar directamente en MySQL
    db = get_db()
    scan_id = db.insert_scan(scan_record)
    
    # Actualizar producción diaria
    db.update_daily_production(
        fecha=scan_record.fecha,
        linea=scan_record.linea,
        nparte=scan_record.nparte,
        cantidad=scan_record.cantidad,
        modelo=scan_record.modelo
    )
    
    return parsed


def process_scan_dual(raw: str, linea: str = "M1") -> ParsedScan:
    """Procesamiento dual: SQLite local + sincronización MySQL background"""
    # SQLite local inmediato con verificación de duplicados
    dual_db = get_dual_db()
    scan_id = dual_db.add_scan_fast(raw, linea)
    
    if scan_id == -2:
        raise ValueError("🚫 Escaneo duplicado: Ya existe en el sistema")
    elif scan_id <= 0:
        raise ValueError("Error procesando escaneo")
    
    # Parse para devolver resultado
    parsed = parse_scan(raw)
    
    # MySQL se sincroniza automáticamente
    return parsed


def process_scan_fast(raw: str, linea: str = "M1") -> int:
    """Procesamiento ultra-rápido con sistema dual SQLite + MySQL
    
    Returns:
        > 0: Éxito (ID del escaneo)
        -1: Error de formato o procesamiento
        -2: Escaneo duplicado
        -3: N° parte fuera de plan
        -4: Plan completo
        -5: Mismo formato consecutivo (falta complemento)
        -6: Parte no EN PROGRESO
        -7: SUB ASSY: BARCODE no coincide con QR
    """
    try:
        # Validación rápida sin strip redundante
        if not raw:
            return -1
        
        raw = raw.strip()
        if not raw:
            return -1

        # Detectar formato (soporta QR nuevo ñ y antiguo ;)
        from .parser import is_complete_qr
        fmt = detect_scan_format(raw)
        
        if fmt == "UNKNOWN":
            # Solo intentar añadir ';' si NO es QR completo y NO es muy largo
            if not is_complete_qr(raw) and len(raw) < 200 and not raw.endswith(';'):
                test_raw = raw + ';'
                fmt = detect_scan_format(test_raw)
                if fmt == 'QR':
                    raw = test_raw
                    has_semicolon = True
                else:
                    return -1
            else:
                return -1

        # Validación QR optimizada
        if fmt == 'QR' and not has_semicolon:
            return -1

        # Procesamiento directo (sin try-catch interno para mejor rendimiento)
        dual_db = get_dual_db()
        scan_id = dual_db.add_scan_fast(raw, linea)

        # Logging condicional solo en errores importantes (reduce I/O)
        if scan_id == -2:
            logger.warning(f"� Duplicado bloqueado: {raw[:30]}...")
        elif scan_id == -3:
            logger.warning(f"📋 Fuera de plan: {raw[:30]}...")
        elif scan_id == -7:
            logger.warning(f"🚫 SUB ASSY no coincide: {raw[:30]}...")
        
        return scan_id
        
    except Exception as e:
        logger.error(f"Error process_scan_fast: {e}")
        return -1


def process_scan_to_cache(raw: str, linea: str = "M1") -> bool:
    """Procesamiento al caché local (solo si MySQL no funciona)"""
    try:
        from .parser import is_complete_qr
        if not raw or not raw.strip() or not is_complete_qr(raw.strip()):
            return False
            
        # Añadir al caché local como respaldo
        local_queue = get_local_queue()
        scan_id = local_queue.add_scan(raw.strip(), linea)
        
        return True
    except Exception:
        return False

