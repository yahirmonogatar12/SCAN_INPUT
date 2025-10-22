"""
Sistema dual SQLite + MySQL para mayor rendimiento local
SQLite: Respuesta ultra-rápida local
MySQL: Sincronización backend en segundo plano
"""
import sqlite3
import threading
import time
import logging
import contextlib
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..config import ROOT_DIR, settings, update_env_var
from ..models.entities import ScanRecord, ResumenProduccion
from .parser import parse_scan
from .metrics_cache import init_metrics_cache, get_metrics_cache

logger = logging.getLogger(__name__)


def get_today_mexico() -> date:
    """Obtiene la fecha actual en la zona horaria de México (Nuevo León)"""
    return datetime.now(ZoneInfo(settings.TZ)).date()


def get_today_mexico_str() -> str:
    """Obtiene la fecha actual en la zona horaria de México como string ISO"""
    return get_today_mexico().isoformat()


class DualDatabaseSystem:
    """Sistema dual SQLite local + MySQL backend"""
    
    def __init__(self):
        self.sqlite_path = settings.LOCAL_SQLITE_PATH
        self.sqlite_path.parent.mkdir(exist_ok=True)
        
        self._lock = threading.Lock()
        # Lock GLOBAL específico para SQLite (evita "database is locked")
        self._sqlite_lock = threading.RLock()  # RLock permite re-entrada del mismo thread
        self._sync_worker_running = False
        self._sync_thread = None
        # Buffer para increments de produced_count (plan_id -> delta acumulado)
        self._plan_produced_buffer = {}
        # Record last mode used to sync plan cache to avoid mixing sources
        self._last_plan_mode = None
        self._last_plan_push = 0.0
        
        # Cache SUB ASSY para consultas rápidas
        self._sub_assy_cache = {}
        self._sub_assy_cache_time = 0
        
        # 🚀 OPTIMIZACIÓN: Cache de validación de planes en memoria
        self._plan_validation_cache = {}  # {(linea, nparte): (plan_count, produced_count, status)}
        self._plan_cache_time = {}  # Timestamp de última actualización
        self._plan_cache_ttl = 5  # segundos - cache muy corto para datos en tiempo real
        
        # Seguimiento de actividad para transiciones automáticas de planes
        self._plan_activity: Dict[tuple[str, int], float] = {}
        # DESACTIVADO: causaba que planes de otras líneas se pausaran automáticamente
        self._auto_pause_seconds = 0
        
        # 🚀 OPTIMIZACIÓN: Cache de mapeo SUB ASSY -> parte principal
        self._sub_assy_to_main_cache = {}  # {sub_assy_part: main_part}
        self._sub_assy_cache_timestamp = {}
        self._sub_assy_cache_ttl = 300  # 5 minutos
        
        # Control de bloqueo temporal para operaciones críticas
        self._critical_operation_lock = threading.Lock()
        self._sync_paused = False
        
        # Inicializar SQLite
        self._init_sqlite()
        
        # 🧹 AUTO-REPARACIÓN AL INICIO: Limpiar pending_scans al arrancar
        self._cleanup_on_startup()
        
        # 🚀 Inicializar sistema de caché de métricas
        self.metrics_cache = init_metrics_cache(self.sqlite_path)
        
        # Iniciar worker de sincronización
        self._start_sync_worker()
        
        # Iniciar worker de métricas en background
        self.metrics_cache.start_background_sync(self)
    
    def _init_sqlite(self):
        """Inicializa base de datos SQLite local con optimizaciones para PCs lentas"""
        with self._get_sqlite_connection() as conn:
            # 🚀 OPTIMIZACIONES PARA PCs LENTAS
            conn.execute("PRAGMA synchronous = NORMAL")  # Menos seguro pero 50x más rápido
            conn.execute("PRAGMA journal_mode = WAL")     # Write-Ahead Logging
            conn.execute("PRAGMA cache_size = 10000")     # 10MB cache
            conn.execute("PRAGMA temp_store = MEMORY")    # Temporales en RAM
            conn.execute("PRAGMA mmap_size = 30000000000") # Memory-mapped I/O
            conn.execute("PRAGMA page_size = 4096")       # Tamaño de página óptimo
            
            # Crear tabla base si no existe
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scans_local (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    raw TEXT NOT NULL,
                    tipo TEXT,
                    fecha TEXT NOT NULL,
                    lote TEXT,
                    secuencia INTEGER,
                    estacion TEXT,
                    nparte TEXT,
                    modelo TEXT,
                    cantidad INTEGER,
                    linea TEXT NOT NULL,
                    synced_to_mysql INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 🏗️ Crear tabla de STAGING para escaneos pendientes (NO se insertan hasta tener par completo)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    raw TEXT NOT NULL,
                    tipo TEXT,
                    fecha TEXT NOT NULL,
                    lote TEXT,
                    secuencia INTEGER,
                    estacion TEXT,
                    nparte TEXT NOT NULL,
                    modelo TEXT,
                    cantidad INTEGER,
                    linea TEXT NOT NULL,
                    scan_format TEXT NOT NULL,
                    barcode_sequence INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Índices para staging (sin WHERE clause que puede causar problemas)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_nparte ON pending_scans(nparte)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_format ON pending_scans(scan_format)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_barcode_seq ON pending_scans(barcode_sequence)")
            
            # Agregar nuevas columnas si no existen (migración en caliente)
            try:
                conn.execute("ALTER TABLE scans_local ADD COLUMN scan_format TEXT DEFAULT 'QR'")
            except sqlite3.OperationalError:
                pass  # La columna ya existe
            
            try:
                conn.execute("ALTER TABLE scans_local ADD COLUMN barcode_sequence INTEGER")
            except sqlite3.OperationalError:
                pass  # La columna ya existe
                
            try:
                conn.execute("ALTER TABLE scans_local ADD COLUMN linked_scan_id INTEGER")
            except sqlite3.OperationalError:
                pass  # La columna ya existe
                
            try:
                conn.execute("ALTER TABLE scans_local ADD COLUMN is_complete INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # La columna ya existe
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS production_totals_local (
                    fecha TEXT NOT NULL,
                    linea TEXT NOT NULL,
                    nparte TEXT NOT NULL,
                    modelo TEXT,
                    cantidad_total INTEGER DEFAULT 0,
                    uph_target INTEGER,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    synced_to_mysql INTEGER DEFAULT 0,
                    PRIMARY KEY (fecha, linea, nparte)
                )
            """)
            
            # Índices para performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scans_fecha ON scans_local(fecha)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scans_synced ON scans_local(synced_to_mysql)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_totals_synced ON production_totals_local(synced_to_mysql)")
            
            # Índice único para QR por 'raw' (más robusto si se sobreescribe lote al insertar)
            try:
                conn.execute("DROP INDEX IF EXISTS idx_qr_unique")
            except Exception:
                pass
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_qr_raw_unique
                ON scans_local(raw)
                WHERE scan_format = 'QR'
            """)
            
            # Tabla cache SUB ASSY local (similar a production_totals_local)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sub_assy_cache (
                    part_no TEXT PRIMARY KEY,
                    sub_assy TEXT NOT NULL,
                    formatted_display TEXT NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Índice para búsquedas rápidas SUB ASSY
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sub_assy_updated ON sub_assy_cache(updated_at)")
            
            # Eliminar índice antiguo basado solo en secuencia (causaba falsos duplicados)
            try:
                conn.execute("DROP INDEX IF EXISTS idx_barcode_unique")
            except Exception:
                pass
            # Nuevo índice único basado en el código completo (raw) para BARCODE
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_barcode_raw_unique
                ON scans_local(raw)
                WHERE scan_format = 'BARCODE'
            """)
            
            # ÍNDICE para vinculación
            conn.execute("CREATE INDEX IF NOT EXISTS idx_linked_scan ON scans_local(linked_scan_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_format ON scans_local(scan_format)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nparte_format ON scans_local(nparte, scan_format)")
            
            conn.commit()

            # Tabla de plan de producción local (cache dual)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS plan_local (
                    id INTEGER PRIMARY KEY,
                    working_date TEXT NOT NULL,
                    line TEXT NOT NULL,
                    part_no TEXT NOT NULL,
                    lot_no TEXT,
                    model_code TEXT,
                    plan_count INTEGER NOT NULL,
                    produced_count INTEGER DEFAULT 0,
                    uph INTEGER,
                    ct INTEGER,
                    status TEXT,
                    sequence INTEGER,
                    started_at TEXT,
                    planned_start TEXT,
                    planned_end TEXT,
                    effective_minutes INTEGER DEFAULT 0,
                    updated_at TEXT,
                    synced_at TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_plan_line_date ON plan_local(line, working_date)")
            
            # Migración: agregar columnas si no existen
            try:
                conn.execute("ALTER TABLE plan_local ADD COLUMN sequence INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            try:
                conn.execute("ALTER TABLE plan_local ADD COLUMN planned_start TEXT")
            except sqlite3.OperationalError:
                pass
            
            try:
                conn.execute("ALTER TABLE plan_local ADD COLUMN planned_end TEXT")
            except sqlite3.OperationalError:
                pass
            
            try:
                conn.execute("ALTER TABLE plan_local ADD COLUMN effective_minutes INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            
            try:
                conn.execute("ALTER TABLE plan_local ADD COLUMN started_at TEXT")
            except sqlite3.OperationalError:
                pass  # Column already exists
                
            conn.commit()
            # Migrar esquema si faltan columnas
            self._migrate_plan_local_schema(conn)
        
        logger.info("SQLite local inicializado correctamente")
    
    def _cleanup_on_startup(self):
        """🧹 AUTO-REPARACIÓN AL INICIAR: Limpia pending_scans al arrancar la aplicación"""
        try:
            with self._get_sqlite_connection(timeout=10.0) as conn:
                # Contar registros antes de limpiar
                cursor = conn.execute("SELECT COUNT(*) FROM pending_scans")
                count_before = cursor.fetchone()[0]
                
                if count_before > 0:
                    logger.warning(f"⚠️  INICIO: Encontrados {count_before} pending_scans de sesión anterior")
                    
                    # Eliminar TODOS los pending_scans (son de sesión anterior, pueden estar corruptos)
                    conn.execute("DELETE FROM pending_scans")
                    conn.commit()
                    
                    logger.warning(f"🧹 AUTO-REPARACIÓN: Limpiados {count_before} pending_scans al iniciar")
                else:
                    logger.info("✅ pending_scans limpio al iniciar")
        except Exception as e:
            logger.error(f"Error en cleanup_on_startup: {e}", exc_info=True)

    def cleanup_orphaned_scans_manual(self, linea: Optional[str] = None, force_all: bool = False) -> int:
        """🚨 FUNCIÓN DE EMERGENCIA: Limpieza MANUAL de pending_scans
        
        Esta función puede ser llamada desde la UI o herramientas de diagnóstico.
        
        Args:
            linea: Línea específica a limpiar (None = todas)
            force_all: Si True, elimina TODOS sin importar edad (emergencia total)
            
        Returns:
            Número de registros eliminados
        """
        try:
            with self._get_sqlite_connection(timeout=10.0) as conn:
                if force_all:
                    # EMERGENCIA TOTAL: Eliminar TODO
                    if linea:
                        deleted = conn.execute("DELETE FROM pending_scans WHERE linea = ?", (linea,))
                    else:
                        deleted = conn.execute("DELETE FROM pending_scans")
                    
                    count = deleted.rowcount
                    logger.warning(f"🚨 LIMPIEZA FORZADA MANUAL: Eliminados {count} pending_scans (linea={linea or 'TODAS'})")
                else:
                    # Limpieza normal (>30s)
                    from datetime import datetime as dt, timedelta
                    thirty_secs_ago = (dt.now(ZoneInfo(settings.TZ)) - 
                                     timedelta(seconds=30)).isoformat()
                    
                    if linea:
                        deleted = conn.execute("""
                            DELETE FROM pending_scans 
                            WHERE ts < ? AND linea = ?
                        """, (thirty_secs_ago, linea))
                    else:
                        deleted = conn.execute("""
                            DELETE FROM pending_scans 
                            WHERE ts < ?
                        """, (thirty_secs_ago,))
                    
                    count = deleted.rowcount
                    if count > 0:
                        logger.warning(f"🧹 LIMPIEZA MANUAL: Eliminados {count} pending_scans >30s (linea={linea or 'TODAS'})")
                
                conn.commit()
                return count
        except Exception as e:
            logger.error(f"Error en cleanup_orphaned_scans_manual: {e}", exc_info=True)
            return 0

    def get_pending_scans_status(self) -> dict:
        """📊 DIAGNÓSTICO: Obtiene el estado actual de pending_scans
        
        Returns:
            Dict con estadísticas de pending_scans por línea y edad
        """
        try:
            with self._get_sqlite_connection(timeout=5.0) as conn:
                cursor = conn.execute("""
                    SELECT linea, scan_format, COUNT(*) as count,
                           MIN(CAST((julianday('now') - julianday(ts)) * 86400 AS INTEGER)) as min_age_secs,
                           MAX(CAST((julianday('now') - julianday(ts)) * 86400 AS INTEGER)) as max_age_secs,
                           AVG(CAST((julianday('now') - julianday(ts)) * 86400 AS INTEGER)) as avg_age_secs
                    FROM pending_scans
                    GROUP BY linea, scan_format
                """)
                
                rows = cursor.fetchall()
                result = {
                    "total_records": sum(r[2] for r in rows),
                    "by_line": {}
                }
                
                for linea, fmt, count, min_age, max_age, avg_age in rows:
                    if linea not in result["by_line"]:
                        result["by_line"][linea] = {}
                    
                    result["by_line"][linea][fmt] = {
                        "count": count,
                        "min_age_seconds": min_age,
                        "max_age_seconds": max_age,
                        "avg_age_seconds": round(avg_age, 1)
                    }
                
                return result
        except Exception as e:
            logger.error(f"Error en get_pending_scans_status: {e}", exc_info=True)
            return {"total_records": 0, "by_line": {}, "error": str(e)}

    @contextlib.contextmanager
    def _get_sqlite_connection(self, timeout: float = 5.0, check_same_thread: bool = True):
        """
        🔒 Context manager thread-safe para conexiones SQLite.
        SIEMPRE usar este método en lugar de sqlite3.connect() directo.
        
        Uso:
            with self._get_sqlite_connection() as conn:
                cursor = conn.execute("SELECT ...")
        """
        with self._sqlite_lock:
            conn = sqlite3.connect(self.sqlite_path, timeout=timeout, check_same_thread=check_same_thread)
            conn.execute(f"PRAGMA busy_timeout = {int(timeout * 1000)}")
            try:
                yield conn
            finally:
                conn.close()
    
    def _check_duplicate_everywhere(self, parsed) -> bool:
        """Verifica duplicados en staging Y en tabla final"""
        try:
            # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
            with self._sqlite_lock:
                with self._get_sqlite_connection(timeout=2.0) as conn:
                    scan_format = getattr(parsed, 'scan_format', 'QR')
                    raw_val = getattr(parsed, 'raw', None)
                    if not raw_val:
                        # Fallbacks mínimos – no exagerar para evitar silencios
                        for attr in ("codigo", "codigo_raw", "original", "texto", "cadena", "codigo_barcode", "code", "raw_text", "raw_data"):
                            raw_val = getattr(parsed, attr, None)
                            if raw_val:
                                break
                    # Verificación rápida de duplicados
                    
                    # Verificar en staging (pending_scans)
                    if scan_format == 'BARCODE':
                        # Duplicado de BARCODE por el código completo (raw), no por secuencia
                        cursor = conn.execute("""
                            SELECT COUNT(*) FROM pending_scans 
                            WHERE scan_format = 'BARCODE' AND raw = ?
                        """, (raw_val or str(parsed),))
                        staging_count = cursor.fetchone()[0]

                    else:
                        # Para QR: duplicado por el QR completo (raw). Evita falsos negativos cuando el lote final se sobreescribe por el del plan.
                        cursor = conn.execute("""
                            SELECT COUNT(*) FROM pending_scans 
                            WHERE scan_format = 'QR' AND raw = ?
                        """, (raw_val or str(parsed),))
                        staging_count = cursor.fetchone()[0]

                    
                    if staging_count > 0:
                        logger.debug("[DUP] Detectado duplicado en staging")
                        return True
                    
                    # Verificar en tabla final (scans_local)
                    dup_final = self._check_duplicate_in_sqlite(parsed)

                    return dup_final
                
        except Exception as e:
            logger.error(f"Error verificando duplicados: {e}")
            return True

    def _try_complete_pair(self, conn, nparte: str, linea: str, modelo: str = None) -> tuple[int, str]:
        """Verifica si tenemos QR+Barcode completos en staging e inserta en tabla final
        
        Returns:
            (result_code, final_nparte): 
                result_code: 1 (success), 0 (incomplete), -7 (SUB ASSY failed)
                final_nparte: nparte to use for plan increment (QR nparte in SUB ASSY mode)
        """
        try:
            # Importar settings al inicio para evitar errores de variable local
            from ..config import settings
            
            # Buscar QR y Barcode pendientes (lógica diferente según modo)
            if (getattr(settings, 'SUB_ASSY_MODE', False) and 
                getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY'):
                # En modo SUB ASSY, buscar cualquier QR/BARCODE pendiente en la línea
                # No importa el nparte porque pueden ser diferentes (Part No vs Sub Assy)
                cursor = conn.execute("""
                    SELECT id, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo, cantidad, 
                           scan_format, barcode_sequence, ts
                    FROM pending_scans 
                    WHERE linea = ?
                    ORDER BY scan_format, id
                """, (linea,))
            else:
                # Modo normal: buscar por el mismo nparte
                cursor = conn.execute("""
                    SELECT id, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo, cantidad, 
                           scan_format, barcode_sequence, ts
                    FROM pending_scans 
                    WHERE nparte = ? AND linea = ?
                    ORDER BY scan_format
                """, (nparte, linea))
            
            pending_scans = cursor.fetchall()
            
            # Verificar si tenemos formatos para emparejamiento
            qr_or_sub_assy_scan = None  # QR o BARCODE del sub_assy
            main_barcode_scan = None    # BARCODE del part_no principal
            
            # En modo SUB ASSY, necesitamos clasificar los BARCODE correctamente
            if (getattr(settings, 'SUB_ASSY_MODE', False) and 
                getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY'):
                
                # Recopilar todos los escaneos por tipo
                qr_scans = []
                barcode_scans = []
                
                for scan in pending_scans:
                    scan_format = scan[10]  # scan_format
                    if scan_format == 'QR':
                        qr_scans.append(scan)
                    elif scan_format == 'BARCODE':
                        barcode_scans.append(scan)
                
                # Si hay QR, usarlo como sub_assy
                if qr_scans:
                    qr_or_sub_assy_scan = qr_scans[0]
                
                # Para BARCODE, distinguir entre Part No principal y Sub Assy
                if barcode_scans:
                    try:
                        # ⚡ USAR CACHÉ LOCAL de sub_assy en vez de MySQL directo (evita congelamiento)
                        with self._get_sqlite_connection(timeout=1.0) as conn_sub:
                            # Verificar cada BARCODE para ver cuál es Part No principal
                            for scan in barcode_scans:
                                scan_nparte = scan[7]  # nparte
                                
                                # Verificar si tiene sub_assy (es Part No principal) usando caché local
                                cur_sub = conn_sub.execute("""
                                    SELECT COUNT(*) FROM sub_assy_cache 
                                    WHERE part_no = ? AND sub_assy IS NOT NULL AND sub_assy != ''
                                """, (scan_nparte,))
                                result = cur_sub.fetchone()
                                has_sub_assy = (result[0] if result else 0) > 0
                                
                                if has_sub_assy:
                                    # Es Part No principal
                                    main_barcode_scan = scan
                                    logger.info(f"🔍 Part No principal identificado: {scan_nparte}")
                                else:
                                    # Es Sub Assy en formato BARCODE (solo si no hay QR)
                                    if not qr_or_sub_assy_scan:
                                        qr_or_sub_assy_scan = scan
                                        logger.info(f"🔍 Sub Assy BARCODE identificado: {scan_nparte}")
                                
                    except Exception as e:
                        logger.error(f"Error clasificando BARCODE en SUB ASSY mode: {e}")
                        # En caso de error, usar el primer BARCODE como principal
                        if barcode_scans and not main_barcode_scan:
                            main_barcode_scan = barcode_scans[0]
                        # Y el segundo como sub_assy si no hay QR
                        if len(barcode_scans) > 1 and not qr_or_sub_assy_scan:
                            qr_or_sub_assy_scan = barcode_scans[1]
            else:
                # Modo normal: clasificación simple
                for scan in pending_scans:
                    scan_format = scan[10]  # scan_format
                    
                    if scan_format == 'QR':
                        qr_or_sub_assy_scan = scan
                    elif scan_format == 'BARCODE':
                        main_barcode_scan = scan
            
            # Compatibilidad con código existente: asignar a variables originales
            if (getattr(settings, 'SUB_ASSY_MODE', False) and 
                getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY'):
                qr_scan = qr_or_sub_assy_scan
                barcode_scan = main_barcode_scan
            else:
                # Modo normal: buscar específicamente QR y BARCODE del mismo nparte
                qr_scan = None
                barcode_scan = None
                for scan in pending_scans:
                    if scan[10] == 'QR':
                        qr_scan = scan
                    elif scan[10] == 'BARCODE':
                        barcode_scan = scan
            
            if qr_scan and barcode_scan:
                # ¡Tenemos par completo! Pero primero validar SUB ASSY si está habilitado
                
                # 🔍 VALIDACIÓN SUB ASSY: Solo en modo ASSY y si está habilitado
                from ..config import settings
                if (getattr(settings, 'SUB_ASSY_MODE', False) and 
                    getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY'):
                    
                    # En modo SUB ASSY: 
                    # - barcode_scan = BARCODE del Part No principal
                    # - qr_scan = QR o BARCODE del Sub Assy
                    sub_assy_nparte = qr_scan[7]  # nparte del sub_assy (puede ser QR o BARCODE)
                    main_part_nparte = barcode_scan[7]  # nparte del part_no principal (siempre BARCODE)
                    
                    try:
                        # ⚡ USAR CACHÉ LOCAL de sub_assy en vez de MySQL directo (evita congelamiento)
                        with self._get_sqlite_connection(timeout=1.0) as conn_sub:
                            # Verificar que el Part No principal tenga como sub_assy el nparte del segundo escaneo
                            cur_sub = conn_sub.execute("""
                                SELECT COUNT(*) FROM sub_assy_cache 
                                WHERE part_no = ? AND sub_assy = ?
                            """, (main_part_nparte, sub_assy_nparte))
                            result = cur_sub.fetchone()
                            barcode_match_count = result[0] if result else 0
                            
                            qr_scan_format = qr_scan[10]  # formato del segundo escaneo
                            
                            if barcode_match_count == 0:
                                # Part No principal no tiene el sub_assy esperado
                                logger.warning(f"🚫 SUB ASSY: Part No '{main_part_nparte}' no tiene sub_assy '{sub_assy_nparte}' (segundo escaneo: {qr_scan_format})")
                                # Eliminar ambos escaneos del staging (rechazar par)
                                conn.execute("DELETE FROM pending_scans WHERE id IN (?, ?)", 
                                           (qr_scan[0], barcode_scan[0]))
                                return (-7, nparte)  # SUB ASSY validation failed
                            else:
                                logger.info(f"✅ SUB ASSY: Part No '{main_part_nparte}' validado con sub_assy '{sub_assy_nparte}' ({qr_scan_format})")
                    
                    except Exception as e:
                        logger.error(f"❌ Error validando SUB ASSY: {e}")
                        # En caso de error, proceder sin validación para no bloquear operación
                        pass
                
                # Determinar nparte final (usar el del BARCODE en modo SUB ASSY, o el común en modo normal)
                if (getattr(settings, 'SUB_ASSY_MODE', False) and 
                    getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY'):
                    final_nparte = barcode_scan[7]  # nparte del BARCODE en modo SUB ASSY
                else:
                    final_nparte = qr_scan[7]  # nparte del QR en modo normal
                
                # Insertar par completo en tabla final
                
                # Intentar tomar lot_no del plan EN PROGRESO (formato ASSYLINE-...)
                lote_final = qr_scan[4]
                try:
                    cur_plan = conn.execute("""
                        SELECT lot_no FROM plan_local
                        WHERE line = ? AND part_no = ? AND status = 'EN PROGRESO'
                        ORDER BY id LIMIT 1
                    """, (linea, final_nparte))
                    prow = cur_plan.fetchone()
                    if prow and prow[0]:
                        lote_final = prow[0]
                except Exception:
                    pass

                # Obtener modelo para el nparte final
                modelo_final = self._get_cached_modelo(final_nparte)

                # Insertar QR
                cursor = conn.execute("""
                    INSERT INTO scans_local 
                    (ts, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo, cantidad, linea, 
                     scan_format, barcode_sequence, is_complete)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """, (qr_scan[12], qr_scan[1], qr_scan[2], qr_scan[3], lote_final, qr_scan[5], 
                     qr_scan[6], final_nparte, modelo_final, qr_scan[9], linea, qr_scan[10], qr_scan[11]))
                qr_final_id = cursor.lastrowid
                
                # Insertar Barcode (usar el nparte final, que puede ser diferente al original del BARCODE)
                cursor = conn.execute("""
                    INSERT INTO scans_local 
                    (ts, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo, cantidad, linea, 
                     scan_format, barcode_sequence, is_complete)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """, (barcode_scan[12], barcode_scan[1], barcode_scan[2], barcode_scan[3], 
                     lote_final, barcode_scan[5], barcode_scan[6], final_nparte, modelo_final, 
                     barcode_scan[9], linea, barcode_scan[10], barcode_scan[11]))
                barcode_final_id = cursor.lastrowid
                
                # Vincular ambos escaneos
                conn.execute("UPDATE scans_local SET linked_scan_id = ? WHERE id = ?", 
                           (barcode_final_id, qr_final_id))
                conn.execute("UPDATE scans_local SET linked_scan_id = ? WHERE id = ?", 
                           (qr_final_id, barcode_final_id))
                
                # Actualizar totales (1 pieza completa) - usar nparte final
                self._update_local_totals(conn, final_nparte, linea, 1, modelo_final)
                
                # Eliminar de staging
                conn.execute("DELETE FROM pending_scans WHERE id IN (?, ?)", 
                           (qr_scan[0], barcode_scan[0]))
                
                logger.info(f"🔗 Par completo insertado: QR {qr_final_id} ↔ Barcode {barcode_final_id}")
                return (1, final_nparte)  # Success with final nparte
            
            return (0, nparte)  # Par incompleto
            
        except Exception as e:
            logger.error(f"Error completando par: {e}")
            return (0, nparte)  # Error treated as incomplete pair
    
    def _check_duplicate_in_sqlite(self, parsed) -> bool:
        """Verifica si ya existe un escaneo duplicado en SQLite local"""
        try:
            # ⚡ USAR LOCK GLOBAL (RLock permite re-entrada si se llama desde otro método con lock)
            with self._sqlite_lock:
                with self._get_sqlite_connection(timeout=2.0) as conn:
                    # Verificar según el formato del escaneo
                    if hasattr(parsed, 'scan_format') and parsed.scan_format == 'BARCODE':
                        # Para BARCODE: duplicado por el código completo (raw)
                        cursor = conn.execute("""
                            SELECT COUNT(*) FROM scans_local 
                            WHERE scan_format = 'BARCODE' AND raw = ?
                        """, (getattr(parsed, 'raw', None) or str(parsed),))
                        count = cursor.fetchone()[0]

                    else:
                        # Para QR: duplicado por el código completo (raw), ya que 'lote' en final puede ser el del plan
                        cursor = conn.execute("""
                            SELECT COUNT(*) FROM scans_local 
                            WHERE scan_format = 'QR' AND raw = ?
                        """, (getattr(parsed, 'raw', None) or str(parsed),))
                        count = cursor.fetchone()[0]

                    return count > 0
                
        except Exception as e:
            logger.error(f"Error verificando duplicados en SQLite: {e}")
            return True  # En caso de error, asumir que es duplicado por seguridad
    
    def _set_plan_active_timestamp(self, linea: Optional[str], plan_id: Optional[int]) -> None:
        """Marca la última actividad conocida para un plan en progreso."""
        if linea is None or plan_id is None:
            return
        self._plan_activity[(linea, plan_id)] = time.time()

    def _clear_plan_activity(self, linea: Optional[str], plan_id: Optional[int]) -> None:
        """Limpia el registro de actividad de un plan cuando deja de estar activo."""
        if linea is None or plan_id is None:
            return
        self._plan_activity.pop((linea, plan_id), None)

    def _auto_transition_plan(self, linea: str, plan_id: int, part_no: str) -> None:
        """Garantiza que el plan correcto esté en progreso y termina cualquier otro activo en la línea."""
        active_rows: list[sqlite3.Row] = []
        try:
            today = get_today_mexico_str()
            with self._get_sqlite_connection(timeout=10.0) as conn:
                conn.row_factory = sqlite3.Row

                status_row = conn.execute(
                    "SELECT COALESCE(status,'') AS status FROM plan_local WHERE id = ?",
                    (plan_id,)
                ).fetchone()
                if not status_row:
                    logger.warning(f"No se encontró plan con id={plan_id} para activación automática")
                    return

                if status_row["status"].strip().upper() == "EN PROGRESO":
                    # Ya está activo
                    return

                active_rows = conn.execute("""
                    SELECT id, part_no FROM plan_local
                    WHERE line = ? AND working_date = ? AND status IN ('EN PROGRESO','PAUSADO')
                """, (linea, today)).fetchall()
        except Exception as e:
            logger.error(f"Error preparando transición automática de plan {part_no}: {e}")
            return

        for active in active_rows:
            active_id = active["id"]
            active_part = active["part_no"]
            if active_id == plan_id:
                continue
            logger.info(f"Auto-terminando plan {active_part} en línea {linea} para activar {part_no}")
            self.actualizar_estado_plan(active_id, "TERMINADO")

        logger.info(f"Auto-iniciando plan {part_no} en línea {linea}")
        self.actualizar_estado_plan(plan_id, "EN PROGRESO")

    def _auto_pause_inactive_plans(self) -> None:
        """Pausa planes en progreso que no han recibido escaneos recientemente."""
        if self._auto_pause_seconds <= 0:
            return

        now = time.time()
        try:
            with self._get_sqlite_connection(timeout=5.0) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("""
                    SELECT id, part_no, line FROM plan_local
                    WHERE status = 'EN PROGRESO'
                """).fetchall()
        except Exception as e:
            logger.error(f"Error consultando planes para auto-pausa: {e}")
            return

        to_pause: list[tuple[int, str, str]] = []
        for row in rows:
            key = (row["line"], row["id"])
            last_seen = self._plan_activity.get(key)
            if last_seen is None:
                # Inicializar timestamp para evitar pausar inmediatamente tras reinicio.
                self._plan_activity[key] = now
                continue
            if now - last_seen >= self._auto_pause_seconds:
                to_pause.append((row["id"], row["part_no"], row["line"]))

        for plan_id, part_no, linea in to_pause:
            self.actualizar_estado_plan(plan_id, "PAUSADO")
            # La limpieza del registro se realiza en actualizar_estado_plan

    def add_scan_fast(self, raw: str, linea: str) -> int:
        """🚀 NUEVO SISTEMA: NO INSERTA HASTA TENER PAR COMPLETO QR+Barcode

        Códigos de retorno:
            >0  id staging (éxito; si par completo ya se insertó en final)
            0   duplicado IGNORADO silenciosamente (sin mostrar error al operador)
            -1  error general / parseo
            -3  parte fuera de plan (no existe en plan del día para la línea)
            -4  plan completo (plan_count alcanzado para esa parte en la línea)
            -5  se intentó escanear dos veces el mismo formato consecutivamente (falta par complementario)
            -6  reservado (el estatus se corrige automáticamente al escanear)
            -7  SUB ASSY: BARCODE no coincide con QR según tabla raw (solo en modo SUB ASSY)
        """
        try:
            # 🧹 PASO 0: LIMPIAR pending_scans antiguos (>30s) ANTES DE TODO
            # Esto previene que QRs/BARCODEs huérfanos bloqueen nuevos escaneos
            try:
                with self._get_sqlite_connection(timeout=2.0) as conn_clean:
                    from datetime import datetime as dt, timedelta
                    thirty_secs_ago = (dt.now(ZoneInfo(settings.TZ)) - 
                                     timedelta(seconds=30)).isoformat()
                    
                    deleted = conn_clean.execute("""
                        DELETE FROM pending_scans 
                        WHERE ts < ? AND linea = ?
                    """, (thirty_secs_ago, linea))
                    
                    deleted_count = deleted.rowcount
                    if deleted_count > 0:
                        logger.warning(f"🧹 Limpiados {deleted_count} pending_scans huérfanos (>30s) en línea {linea}")
                    conn_clean.commit()
            except Exception as e:
                logger.debug(f"Error limpiando pending_scans antiguos: {e}")
            
            # Parsear escaneo (detecta automáticamente QR o BARCODE)
            parsed = parse_scan(raw)
            
            # Convertir BARCODE a formato compatible si es necesario
            if hasattr(parsed, 'scan_format') and parsed.scan_format == 'BARCODE':
                from .parser import convert_barcode_to_scan_record_format
                scan_record = convert_barcode_to_scan_record_format(parsed)
            else:
                scan_record = parsed
            
            # 1) Duplicados primero (staging + final) - IGNORAR SILENCIOSAMENTE
            if self._check_duplicate_everywhere(scan_record):
                format_name = getattr(parsed, 'scan_format', 'QR')
                logger.debug(f"� Escaneo {format_name} duplicado ignorado silenciosamente: {raw[:30]}...")
                return 0  # Retorna 0 para que UI lo ignore sin mostrar error

            # 2) Validación de PLAN (solo si no es duplicado)
            nparte_plan = getattr(scan_record, 'nparte', None)
            scan_format = getattr(parsed, 'scan_format', 'QR')
            
            # En modo SUB ASSY, skipear validación de plan para escaneos QR (contienen sub_assy)
            skip_plan_validation = (getattr(settings, 'SUB_ASSY_MODE', False) and 
                                  getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY' and
                                  scan_format == 'QR')
            
            plan_line: Optional[str] = linea
            plan_part_no = nparte_plan

            if nparte_plan and not skip_plan_validation:
                today = get_today_mexico_str()
                plan_id: Optional[int] = None
                plan_count: Optional[int] = None
                produced_count: Optional[int] = None
                status_val: str = ""

                def _normalize_part(part: str | None) -> str:
                    return (part or "").strip().upper()

                def resolve_plan() -> bool:
                    nonlocal plan_id, plan_line, plan_count, produced_count, status_val, plan_part_no
                    plan_id = None
                    plan_count = None
                    produced_count = None
                    status_val = ""

                    # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
                    with self._sqlite_lock:
                        with self._get_sqlite_connection(timeout=2.0) as conn_plan:
                            conn_plan.row_factory = sqlite3.Row

                            def fetch_plan(part_value: str, preferred_line: Optional[str] = None):
                                """
                                Busca el plan activo para una parte, priorizando:
                                1. Plan EN PROGRESO
                                2. Plan con secuencia más baja que NO esté TERMINADO
                                """
                                normalized = _normalize_part(part_value)
                                if not normalized:
                                    return None
                                row_local = None
                                if preferred_line:
                                    # Buscar plan EN PROGRESO primero, luego el de menor secuencia no TERMINADO
                                    row_local = conn_plan.execute("""
                                        SELECT id, line, plan_count, produced_count, COALESCE(status,'') AS status, sequence
                                        FROM plan_local
                                        WHERE working_date=? AND line=? AND UPPER(TRIM(part_no))=?
                                        ORDER BY 
                                            CASE WHEN status = 'EN PROGRESO' THEN 0 ELSE 1 END,
                                            CASE WHEN status = 'TERMINADO' THEN 999 ELSE sequence END
                                        LIMIT 1
                                    """, (today, preferred_line, normalized)).fetchone()
                                if not row_local:
                                    # Buscar en cualquier línea
                                    row_local = conn_plan.execute("""
                                        SELECT id, line, plan_count, produced_count, COALESCE(status,'') AS status, sequence
                                        FROM plan_local
                                        WHERE working_date=? AND UPPER(TRIM(part_no))=?
                                        ORDER BY 
                                            CASE WHEN status = 'EN PROGRESO' THEN 0 ELSE 1 END,
                                            CASE WHEN status = 'TERMINADO' THEN 999 ELSE sequence END
                                        LIMIT 1
                                    """, (today, normalized)).fetchone()
                                return row_local

                            row_local = fetch_plan(nparte_plan, linea)

                            # En modo SUB ASSY, si no encuentra la parte directa, buscar como sub_assy
                            if not row_local and (getattr(settings, 'SUB_ASSY_MODE', False) and 
                                               getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY'):
                                try:
                                    # ⚡ Usar misma conexión para evitar nested locks
                                    cur_sub = conn_plan.execute("""
                                        SELECT part_no FROM sub_assy_cache 
                                        WHERE sub_assy LIKE ?
                                        LIMIT 1
                                    """, (f'%{nparte_plan}%',))
                                    main_part_row = cur_sub.fetchone()
                                    
                                    if main_part_row:
                                        plan_part_no = main_part_row[0]
                                        row_local = fetch_plan(plan_part_no, linea)
                                        if not row_local:
                                            row_local = fetch_plan(plan_part_no)
                                except Exception as e:
                                    logger.warning(f"Error buscando parte principal para SUB ASSY {nparte_plan}: {e}")

                            # Nota: fetch_plan ya se llamó arriba, no necesitamos llamarlo otra vez
                            # El row_local ya tiene el resultado correcto

                            if row_local:
                                plan_id = row_local["id"]
                                plan_line = row_local["line"] or linea
                                plan_count = row_local["plan_count"]
                                produced_count = row_local["produced_count"]
                                status_val = row_local["status"] or ""

                    return plan_id is not None

                resolved = resolve_plan()

                # ⚡ ELIMINADO: No llamar _fetch_plan_for_part() aquí para evitar bloqueo MySQL
                # Si el plan no está en caché local, el worker lo sincronizará automáticamente cada 15s
                # Confiar solo en el caché SQLite local para mantener UI responsive
                
                # if not resolved:
                #     fetched = self._fetch_plan_for_part(plan_line or linea, plan_part_no or nparte_plan)
                #     if fetched:
                #         resolved = resolve_plan()
                
                # if not resolved and plan_part_no != nparte_plan:
                #     fetched = self._fetch_plan_for_part(linea, nparte_plan)
                #     if fetched:
                #         resolved = resolve_plan()

                if not resolved:
                    return -3  # No existe en plan

                if plan_count is not None and produced_count is not None and produced_count >= plan_count:
                    return -4  # Plan completo alcanzado

                # 🔒 VALIDACIÓN Y AUTO-INICIO: Gestionar estados de planes
                current_status = status_val.strip().upper()
                
                if current_status == 'EN PROGRESO':
                    # El plan ya está activo, permitir escaneo
                    pass
                else:
                    # El plan NO está EN PROGRESO (está en PLAN o PAUSADO)
                    # Verificar si ya hay OTRO plan EN PROGRESO con MENOR secuencia
                    try:
                        with self._get_sqlite_connection(timeout=1.0) as conn_check:
                            # Obtener secuencia del plan actual
                            cur_seq = conn_check.execute("""
                                SELECT sequence FROM plan_local WHERE id = ?
                            """, (plan_id,)).fetchone()
                            
                            plan_sequence = cur_seq[0] if cur_seq else 999
                            
                            # Buscar planes EN PROGRESO con menor secuencia (deben completarse primero)
                            cur_check = conn_check.execute("""
                                SELECT id, part_no, sequence, status, produced_count, plan_count 
                                FROM plan_local
                                WHERE line = ? AND id != ? AND status IN ('EN PROGRESO', 'PAUSADO')
                                  AND working_date = ?
                                ORDER BY sequence
                                LIMIT 1
                            """, (plan_line, plan_id, today))
                            otro_plan = cur_check.fetchone()
                            
                            if otro_plan:
                                otro_seq = otro_plan[2]
                                otro_produced = otro_plan[4] or 0
                                otro_plan_count = otro_plan[5] or 0
                                
                                # Si hay un plan con menor secuencia que NO está completo, bloq uear
                                if otro_seq < plan_sequence and otro_produced < otro_plan_count:
                                    logger.warning(
                                        f"🚫 Bloqueado: Debes completar primero el plan {otro_plan[1]} "
                                        f"(seq={otro_seq}, {otro_produced}/{otro_plan_count}, status={otro_plan[3]}) "
                                        f"antes de iniciar {plan_part_no} (seq={plan_sequence})"
                                    )
                                    return -10  # MODELO DIFERENTE (no permitir escaneo fuera de secuencia)
                    except Exception as e:
                        logger.error(f"Error verificando plan activo: {e}")
                        return -10  # En caso de error, bloquear por seguridad
                    
                    # No hay planes anteriores pendientes, AUTO-INICIAR este plan
                    logger.info(f"🚀 Auto-iniciando plan {plan_part_no} (id={plan_id}, seq={plan_sequence}, status actual={current_status}) en línea {plan_line}")
                    self._auto_transition_plan(plan_line, plan_id, plan_part_no)

            # Modo SOLO QR: si está activo, insertar par completo (QR + BARCODE sintético) inmediatamente
            if getattr(settings, 'SOLO_QR_MODE', False):
                scan_format = getattr(parsed, 'scan_format', 'QR')
                if scan_format != 'QR':
                    return -1  # En modo SOLO QR, ignorar BARCODE
                # Obtener modelo desde caché
                modelo = self._get_cached_modelo(scan_record.nparte)
                # Preparar fecha parseada
                parsed_fecha = getattr(scan_record, 'fecha_iso', None)
                if not parsed_fecha and hasattr(scan_record, 'fecha'):
                    try:
                        parsed_fecha = scan_record.fecha.isoformat()
                    except Exception:
                        parsed_fecha = get_today_mexico_str()
                if not parsed_fecha:
                    parsed_fecha = get_today_mexico_str()
                # Determinar lote final (usar plan EN PROGRESO si aplica)
                    lote_final = scan_record.lote
                    try:
                        # ⚡ USAR HELPER THREAD-SAFE para evitar "database is locked"
                        with self._get_sqlite_connection(timeout=1.0) as conn_lote:
                                cur_plan = conn_lote.execute("""
                                    SELECT lot_no FROM plan_local
                                    WHERE line = ? AND part_no = ? AND status = 'EN PROGRESO'
                                    ORDER BY id LIMIT 1
                                """, ((plan_line or linea), scan_record.nparte))
                                prow = cur_plan.fetchone()
                                if prow and prow[0]:
                                    lote_final = prow[0]
                    except Exception:
                        pass
                # Insertar directamente QR y BARCODE sintético como par completo
                # ⚡ USAR HELPER THREAD-SAFE para evitar "database is locked"
                with self._get_sqlite_connection(timeout=2.0) as conn:
                        ts_now = datetime.now(ZoneInfo(settings.TZ)).isoformat()
                        # QR final
                        cur_qr = conn.execute("""
                            INSERT INTO scans_local 
                            (ts, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo, cantidad, linea, 
                             scan_format, barcode_sequence, is_complete)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'QR', NULL, 1)
                        """, (
                            ts_now,
                            raw,
                            scan_record.tipo,
                            parsed_fecha,
                            lote_final,
                            scan_record.secuencia,
                            linea,  # ⚡ USAR LÍNEA SELECCIONADA en lugar de scan_record.estacion
                            scan_record.nparte,
                            modelo,
                            scan_record.cantidad,
                            linea
                        ))
                        qr_final_id = cur_qr.lastrowid
                        # BARCODE sintético único (para respetar índice único por raw)
                        synthetic_barcode_raw = f"SOLO QR|{raw}"
                        cur_bc = conn.execute("""
                            INSERT INTO scans_local 
                            (ts, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo, cantidad, linea, 
                             scan_format, barcode_sequence, is_complete)
                            VALUES (?, ?, 'BARCODE', ?, ?, NULL, ?, ?, ?, ?, ?, 'BARCODE', NULL, 1)
                        """, (
                            ts_now,
                            synthetic_barcode_raw,
                            parsed_fecha,
                            lote_final,
                            linea,  # ⚡ USAR LÍNEA SELECCIONADA en lugar de scan_record.estacion
                            scan_record.nparte,
                            modelo,
                            scan_record.cantidad,
                            linea
                        ))
                        barcode_final_id = cur_bc.lastrowid
                        # Vincular ambos
                        conn.execute("UPDATE scans_local SET linked_scan_id = ? WHERE id = ?", (barcode_final_id, qr_final_id))
                        conn.execute("UPDATE scans_local SET linked_scan_id = ? WHERE id = ?", (qr_final_id, barcode_final_id))
                        # Actualizar totales
                        self._update_local_totals(conn, scan_record.nparte, linea, 1, modelo)
                        conn.commit()
                        # Incrementar produced_count del plan
                        if nparte_plan:
                            try:
                                lote_escaneado = getattr(scan_record, 'lot_no', None) or getattr(scan_record, 'lote', None)
                                self.increment_local_plan_produced(plan_line or linea, nparte_plan, delta=1, lote=lote_escaneado)
                            except Exception as e_inc:
                                logger.error(f"No se pudo incrementar produced_count local (SOLO QR): {e_inc}")
                logger.info(f"✅ SOLO QR: Par completo insertado para {scan_record.nparte} (BARCODE='SOLO QR')")
                return qr_final_id

            # 3) Validar si es duplicado EXACTO en pending_scans (mismo nparte + secuencia)
            # Permitir múltiples QRs/BARCODEs siempre que sean DIFERENTES piezas
            scan_format = getattr(parsed, 'scan_format', 'QR')
            try:
                # ⚡ USAR HELPER THREAD-SAFE para evitar "database is locked"
                with self._get_sqlite_connection(timeout=1.0) as conn_chk:
                        # Verificar si hay pending del MISMO formato Y MISMO nparte Y MISMA secuencia
                        scan_secuencia = getattr(scan_record, 'secuencia', 0)
                        cur_chk = conn_chk.execute("""
                            SELECT COUNT(*) FROM pending_scans
                            WHERE linea=? AND scan_format=? AND nparte=? AND secuencia=?
                        """, (linea, scan_format, scan_record.nparte, scan_secuencia))
                        pending_exact = cur_chk.fetchone()[0]
                        if pending_exact > 0:
                            # Ya hay uno IDÉNTICO en espera - es duplicado real
                            logger.debug(f"🔇 Código duplicado en pending: {raw[:30]}...")
                            return 0  # Duplicado - ignorar silenciosamente
            except Exception:
                pass  # En error no bloqueamos regla para no frenar operación
            
            # Obtener modelo desde caché
            modelo = self._get_cached_modelo(scan_record.nparte)
            # (scan_format ya determinado arriba)
            barcode_sequence = getattr(parsed, 'secuencia', None) if scan_format == 'BARCODE' else None
            
            # ⚡ USAR HELPER THREAD-SAFE para evitar "database is locked"
            # TIMEOUT AUMENTADO: 5s para evitar fallos cuando sync worker está activo
            with self._get_sqlite_connection(timeout=5.0) as conn:
                # 🔄 PASO 1: Guardar en STAGING (temporal) - NO en tabla final
                # Usar la fecha REAL parseada del QR (fecha_iso) si existe; esto es crítico para detectar duplicados correctamente.
                parsed_fecha = getattr(scan_record, 'fecha_iso', None)
                if not parsed_fecha and hasattr(scan_record, 'fecha'):
                    try:
                        parsed_fecha = scan_record.fecha.isoformat()
                    except Exception:
                        parsed_fecha = get_today_mexico_str()
                if not parsed_fecha:
                    parsed_fecha = get_today_mexico_str()

                cursor = conn.execute("""
                    INSERT INTO pending_scans 
                    (ts, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo, cantidad, linea, 
                     scan_format, barcode_sequence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now(ZoneInfo(settings.TZ)).isoformat(),
                    raw,
                    scan_record.tipo,
                    parsed_fecha,
                    scan_record.lote,
                    scan_record.secuencia,
                    linea,  # ⚡ USAR LÍNEA SELECCIONADA en lugar de scan_record.estacion
                    scan_record.nparte,
                    modelo,
                    scan_record.cantidad,
                    linea,
                    scan_format,
                    barcode_sequence
                ))
                staging_id = cursor.lastrowid
                
                # 🔍 PASO 2: Verificar si ahora tenemos par completo (QR + Barcode)
                pair_result, final_nparte = self._try_complete_pair(conn, scan_record.nparte, linea, modelo)
                
                conn.commit()
            
            # 📊 RESULTADO
            if pair_result == 1:
                # Par completo insertado exitosamente
                # Incrementar produced_count local del plan (1 pieza completa) usando final_nparte
                logger.info(f"🔍 Par completo - nparte_plan={nparte_plan}, final_nparte={final_nparte}, linea={linea}")
                if nparte_plan:
                    try:
                        # Obtener lote del scan_record para asegurar que incrementamos el plan correcto
                        lote_escaneado = getattr(scan_record, 'lot_no', None) or getattr(scan_record, 'lote', None)
                        logger.info(f"⚡ Incrementando produced_count: linea={linea}, nparte={final_nparte}, lote={lote_escaneado}")
                        self.increment_local_plan_produced(linea, final_nparte, delta=1, lote=lote_escaneado)
                    except Exception as e_inc:
                        logger.error(f"No se pudo incrementar produced_count local: {e_inc}")
                else:
                    logger.warning(f"⚠️ nparte_plan es None, NO se incrementa produced_count (linea={linea}, nparte={scan_record.nparte})")
                logger.info(f"✅ PAR COMPLETO: QR+Barcode insertados en DB FINAL ({final_nparte})")
                return staging_id  # Éxito - par completo insertado
            elif pair_result == -7:
                # SUB ASSY validation failed - par rechazado
                return -7
            else:
                # Par incompleto (pair_result == 0)
                opposite = "BARCODE" if scan_format == "QR" else "QR"
                logger.info(f"⏳ {scan_format} en STAGING, esperando {opposite} para insertar en DB ({scan_record.nparte})")
                return staging_id  # Éxito - esperando complemento
            
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                logger.warning(f"🚫 Escaneo duplicado bloqueado: {raw}")
                return -2
            else:
                logger.error(f"❌ Error de integridad SQL: {e}")
                return -1
        except sqlite3.OperationalError as e:
            # Error específico de SQLite (database locked, timeout, etc.)
            if "locked" in str(e).lower() or "timeout" in str(e).lower():
                logger.warning(f"⏳ SQLite ocupado, reintentando... ({raw[:30]}...)")
                # Esperar un poco y reintentar UNA vez
                time.sleep(0.1)
                try:
                    return self.add_scan_fast(raw, linea)  # Reintentar
                except Exception as retry_err:
                    logger.error(f"❌ Fallo después de reintento: {retry_err}")
                    return -1
            else:
                logger.error(f"❌ Error SQLite en add_scan_fast: {e}", exc_info=True)
                return -1
        except Exception as e:
            # Loguear el error completo con traceback para debugging
            logger.error(f"❌ Error en add_scan_fast procesando '{raw[:50]}...': {e}", exc_info=True)
            return -1
    
    def _try_auto_link(self, conn, new_scan_id: int, nparte: str, linea: str, scan_format: str, modelo: str = None) -> bool:
        """Intenta vincular automáticamente QR y Barcode del mismo N° parte
        Returns True si se completó la vinculación (ambos escaneos presentes)"""
        try:
            # Buscar escaneo complementario (QR busca BARCODE y viceversa)
            opposite_format = "BARCODE" if scan_format == "QR" else "QR"
            
            cursor = conn.execute("""
                SELECT id FROM scans_local 
                WHERE nparte = ? AND scan_format = ? 
                AND linked_scan_id IS NULL
                ORDER BY id DESC LIMIT 1
            """, (nparte, opposite_format))
            
            result = cursor.fetchone()
            if result:
                opposite_scan_id = result[0]
                
                # Vincular ambos escaneos y marcarlos como completos
                conn.execute("""
                    UPDATE scans_local 
                    SET linked_scan_id = ?, is_complete = 1 
                    WHERE id = ?
                """, (opposite_scan_id, new_scan_id))
                
                conn.execute("""
                    UPDATE scans_local 
                    SET linked_scan_id = ?, is_complete = 1 
                    WHERE id = ?
                """, (new_scan_id, opposite_scan_id))
                
                logger.info(f"🔗 Vinculación completada: {scan_format} {new_scan_id} ↔ {opposite_format} {opposite_scan_id} (N° parte: {nparte})")
                
                # AHORA SÍ actualizar totales - solo cuando el par está completo
                self._update_local_totals(conn, nparte, linea, 1, modelo)  # 1 pieza completa
                logger.info(f"📊 Pieza completa contabilizada: QR+Barcode = 1 pieza ({nparte})")
                
                return True  # Vinculación exitosa
            else:
                logger.info(f"⏳ Escaneo {scan_format} guardado, esperando {opposite_format} para completar pieza ({nparte})")
                return False  # Sin vinculación, esperando el complemento
                
        except Exception as e:
            logger.error(f"Error en vinculación automática: {e}")
            return False
    
    def get_linked_scans(self, nparte: str) -> dict:
        """Obtiene escaneos vinculados por N° parte"""
        try:
            with self._get_sqlite_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT id, raw, scan_format, linked_scan_id, created_at
                    FROM scans_local 
                    WHERE nparte = ?
                    ORDER BY created_at DESC
                """, (nparte,))
                
                scans = cursor.fetchall()
                result = {
                    'nparte': nparte,
                    'qr_scans': [],
                    'barcode_scans': [],
                    'linked_pairs': []
                }
                
                for scan in scans:
                    scan_data = {
                        'id': scan['id'],
                        'raw': scan['raw'],
                        'linked_to': scan['linked_scan_id'],
                        'created_at': scan['created_at']
                    }
                    
                    if scan['scan_format'] == 'QR':
                        result['qr_scans'].append(scan_data)
                    else:
                        result['barcode_scans'].append(scan_data)
                    
                    if scan['linked_scan_id']:
                        pair = (min(scan['id'], scan['linked_scan_id']), max(scan['id'], scan['linked_scan_id']))
                        if pair not in result['linked_pairs']:
                            result['linked_pairs'].append(pair)
                
                return result
                
        except Exception as e:
            logger.error(f"Error obteniendo escaneos vinculados: {e}")
            return {'nparte': nparte, 'qr_scans': [], 'barcode_scans': [], 'linked_pairs': []}
    
    def _update_local_totals(self, conn, nparte: str, linea: str, cantidad: int, modelo: str = None):
        """Actualiza totales locales en SQLite"""
        today = get_today_mexico_str()
        
        # Obtener UPH target si tenemos el modelo
        uph_target = None
        if modelo:
            uph_target = self._get_cached_uph(nparte)
        
        conn.execute("""
            INSERT INTO production_totals_local 
            (fecha, linea, nparte, modelo, cantidad_total, uph_target, synced_to_mysql)
            VALUES (?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(fecha, linea, nparte) DO UPDATE SET
            modelo = COALESCE(excluded.modelo, modelo),
            cantidad_total = cantidad_total + excluded.cantidad_total,
            uph_target = COALESCE(excluded.uph_target, uph_target),
            updated_at = CURRENT_TIMESTAMP,
            synced_to_mysql = 0
        """, (today, linea, nparte, modelo, cantidad, uph_target))
    
    def get_today_totals_local(self) -> List[ResumenProduccion]:
        """Obtiene totales del día desde SQLite local (muy rápido)"""
        today = get_today_mexico_str()
        
        try:
            with self._get_sqlite_connection() as conn:
                conn.row_factory = sqlite3.Row
                # Calcular uph_real (piezas completas última hora)
                from datetime import datetime, timedelta
                ts_limit = (datetime.now(ZoneInfo(settings.TZ)) - timedelta(hours=1)).isoformat()
                # Piezas completas: contamos solo escaneos completos is_complete=1 y dividimos entre 2 (par QR+BARCODE)
                cursor = conn.execute("""
                    WITH last_hour AS (
                        SELECT linea, nparte, COUNT(*)/2 AS piezas_hora
                        FROM scans_local
                        WHERE ts >= ? AND is_complete = 1
                        GROUP BY linea, nparte
                    )
                    SELECT p.fecha, p.linea, p.nparte, p.modelo, p.cantidad_total, p.uph_target,
                           COALESCE(lh.piezas_hora, 0) AS uph_real
                    FROM production_totals_local p
                    LEFT JOIN last_hour lh ON lh.linea = p.linea AND lh.nparte = p.nparte
                    WHERE p.fecha = ?
                    ORDER BY p.linea, p.nparte
                """, (ts_limit, today))
                
                totals = []
                for row in cursor.fetchall():
                    totals.append(ResumenProduccion(
                        fecha=date.fromisoformat(row['fecha']),
                        linea=row['linea'],
                        nparte=row['nparte'],
                        modelo=row['modelo'],
                        cantidad_total=row['cantidad_total'],
                        uph_target=row['uph_target'],
                        uph_real=row['uph_real']
                    ))
                
                return totals
                
        except Exception as e:
            logger.error(f"Error obteniendo totales locales: {e}")
            return []

    def get_local_totals(self, linea_filter: str = None) -> dict:
        """Obtener totales locales agrupados por línea (SOLO piezas completas - QR+Barcode vinculados)"""
        today = get_today_mexico_str()
        
        try:
            with self._get_sqlite_connection() as conn:
                conn.row_factory = sqlite3.Row
                
                # Calcular piezas en última hora (uph_real)
                from datetime import datetime, timedelta
                ts_limit = (datetime.now(ZoneInfo(settings.TZ)) - timedelta(hours=1)).isoformat()
                # Mapa (linea,nparte)->uph_real
                uph_map = {}
                uph_cursor = conn.execute("""
                    SELECT linea, nparte, COUNT(*)/2 AS piezas_hora
                    FROM scans_local
                    WHERE ts >= ? AND is_complete = 1
                    GROUP BY linea, nparte
                """, (ts_limit,))
                for r in uph_cursor.fetchall():
                    uph_map[(r['linea'], r['nparte'])] = int(r['piezas_hora'])

                if linea_filter:
                    # Contar solo escaneos completos (vinculados) por N° parte
                    cursor = conn.execute("""
                        SELECT nparte, modelo, COUNT(DISTINCT linked_scan_id) / 2 as piezas_completas
                        FROM scans_local 
                        WHERE fecha = ? AND linea = ? AND is_complete = 1
                        GROUP BY nparte, modelo
                    """, (today, linea_filter))
                    
                    results = {}
                    for row in cursor.fetchall():
                        # Obtener UPH desde cache (NO bloquear, programar fetch en background si falta)
                        modelo, uph_target = self.get_modelo_local(row['nparte'], fetch_if_missing=True)
                        results[row['nparte']] = {
                            'modelo': row['modelo'] or modelo or 'Sin modelo',
                            'cantidad': int(row['piezas_completas']),
                            'uph': uph_target or 0,
                            'uph_real': uph_map.get((linea_filter, row['nparte']), 0),
                            'fecha': 'Hoy'
                        }
                    return results
                else:
                    # Obtener todas las líneas - solo piezas completas
                    cursor = conn.execute("""
                        SELECT linea, nparte, modelo, COUNT(DISTINCT linked_scan_id) / 2 as piezas_completas
                        FROM scans_local 
                        WHERE fecha = ? AND is_complete = 1
                        GROUP BY linea, nparte, modelo
                    """, (today,))
                    
                    results = {}
                    for row in cursor.fetchall():
                        linea = row['linea']
                        if linea not in results:
                            results[linea] = {}
                        
                        # Obtener UPH desde cache (NO bloquear, programar fetch en background si falta)
                        modelo, uph_target = self.get_modelo_local(row['nparte'], fetch_if_missing=True)
                        results[linea][row['nparte']] = {
                            'modelo': row['modelo'] or modelo or 'Sin modelo',
                            'cantidad': int(row['piezas_completas']),
                            'uph': uph_target or 0,
                            'uph_real': uph_map.get((linea, row['nparte']), 0),
                            'fecha': 'Hoy'
                        }
                    
                    return results
                    
        except Exception as e:
            logger.error(f"Error obteniendo totales locales: {e}")
            return {}
    
    def get_last_scans_local(self, limit: int = 100) -> List[ScanRecord]:
        """Obtiene últimos escaneos desde SQLite local"""
        try:
            with self._get_sqlite_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM scans_local
                    ORDER BY id DESC
                    LIMIT ?
                """, (limit,))
                
                scans = []
                for row in cursor.fetchall():
                    scans.append(ScanRecord(
                        id=row['id'],
                        ts=datetime.fromisoformat(row['ts']),
                        raw=row['raw'],
                        tipo=row['tipo'],
                        fecha=date.fromisoformat(row['fecha']),
                        lote=row['lote'],
                        secuencia=row['secuencia'],
                        estacion=row['estacion'],
                        nparte=row['nparte'],
                        modelo=row['modelo'],
                        cantidad=row['cantidad'],
                        linea=row['linea']
                    ))
                
                return scans
                
        except Exception as e:
            logger.error(f"Error obteniendo escaneos locales: {e}")
            return []
    
    def _get_cached_modelo(self, nparte: str) -> Optional[str]:
        """Obtiene modelo desde caché SQLite local ultra-rápido (NO bloquea)"""
        try:
            # fetch_if_missing=False para NO bloquear nunca
            modelo, _ = self.get_modelo_local(nparte, fetch_if_missing=False)
            return modelo
        except:
            return None  # Se actualizará en sync worker
    
    def _get_cached_uph(self, nparte: str) -> Optional[int]:
        """Obtiene UPH desde caché rápido"""
        return None  # Se actualizará en sync worker
    
    def _start_sync_worker(self):
        """Inicia worker de sincronización con MySQL"""
        if not self._sync_worker_running:
            self._sync_worker_running = True
            self._sync_thread = threading.Thread(target=self._sync_worker_loop, daemon=True)
            self._sync_thread.start()
            logger.info("🚀 Worker de sincronización SQLite -> MySQL iniciado")
        else:
            logger.warning("⚠️ Worker de sincronización ya estaba ejecutándose")
    
    def _pause_sync_temporarily(self):
        """Pausa temporalmente la sincronización para operaciones críticas"""
        with self._critical_operation_lock:
            self._sync_paused = True
    
    def _resume_sync(self):
        """Reanuda la sincronización después de operaciones críticas"""
        with self._critical_operation_lock:
            self._sync_paused = False
    
    def _sync_worker_loop(self):
        """Loop del worker que sincroniza SQLite -> MySQL"""
        logger.info("🔄 Sync worker loop iniciado")
        cycle_count = 0
        
        while self._sync_worker_running:
            try:
                cycle_count += 1
                
                # Log cada 20 ciclos (cada minuto aprox)
                if cycle_count % 20 == 0:
                    logger.debug(f"♻️ Sync worker activo - Ciclo #{cycle_count}")
                
                # Verificar si la sincronización está pausada
                with self._critical_operation_lock:
                    is_paused = self._sync_paused
                
                if is_paused:
                    # Si está pausada, esperar menos tiempo y continuar
                    time.sleep(0.5)
                    continue
                
                # Obtener timestamp al inicio del ciclo
                now = time.time()
                
                # Sincronizar escaneos pendientes
                synced_scans = self._sync_scans_to_mysql()
                
                # Sincronizar totales pendientes  
                synced_totals = self._sync_totals_to_mysql()
                
                # Actualizar modelos desde tabla raw
                self._update_models_from_raw()
                
                # Actualizar cache SUB ASSY (cada ~30s para no sobrecargar)
                if now - getattr(self, '_last_sub_assy_update', 0) > 30:
                    self._update_sub_assy_cache()
                    self._last_sub_assy_update = now

                # Sincronizar plan_main (cada ~15s) usando marca de tiempo
                last_plan = getattr(self, '_last_plan_sync', 0)
                if now - last_plan > 15:
                    logger.debug(f"⏰ Ejecutando sincronización de plan (último: {now - last_plan:.1f}s atrás)")
                    self._sync_plan_from_mysql()
                    self._last_plan_sync = now
                # Empujar increments produced_count cada 15s también
                if now - getattr(self, '_last_plan_push', 0) > 15:
                    self._push_plan_produced_increments()
                    self._last_plan_push = now
                
                if synced_scans > 0 or synced_totals > 0:
                    logger.info(f"Sincronizado: {synced_scans} escaneos, {synced_totals} totales")
                
                # Verificar si hay planes inactivos para pausar automáticamente
                self._auto_pause_inactive_plans()

                # 🧹 AUTO-REPARACIÓN: Limpieza periódica de pending_scans huérfanos (cada 5 minutos)
                last_cleanup = getattr(self, '_last_cleanup', 0)
                if now - last_cleanup > 300:  # 5 minutos = 300 segundos
                    try:
                        with self._get_sqlite_connection(timeout=5.0) as conn:
                            from datetime import datetime as dt, timedelta
                            thirty_secs_ago = (dt.now(ZoneInfo(settings.TZ)) - 
                                             timedelta(seconds=30)).isoformat()
                            
                            deleted = conn.execute("""
                                DELETE FROM pending_scans 
                                WHERE ts < ?
                            """, (thirty_secs_ago,))
                            
                            count = deleted.rowcount
                            if count > 0:
                                logger.warning(f"🧹 AUTO-REPARACIÓN: Limpiados {count} pending_scans huérfanos (limpieza periódica)")
                            conn.commit()
                            
                        self._last_cleanup = now
                    except Exception as e_cleanup:
                        logger.debug(f"Error en limpieza periódica: {e_cleanup}")

                # Dormir según la carga (AUMENTADO a 5s para reducir contención)
                time.sleep(5)  # Sincronizar cada 5 segundos (menos presión en SQLite)
                
            except Exception as e:
                logger.error(f"❌ Error en sync worker (ciclo #{cycle_count}): {e}", exc_info=True)
                time.sleep(15)  # Esperar más tiempo en caso de error (AUMENTADO)
        
        logger.warning("🛑 Sync worker loop terminado")
    
    def _sync_scans_to_mysql(self) -> int:
        """Sincroniza escaneos pendientes a MySQL"""
        try:
            from ..db import get_db
            
            # Obtener escaneos no sincronizados
            with self._get_sqlite_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM scans_local 
                    WHERE synced_to_mysql = 0 
                    ORDER BY id ASC 
                    LIMIT 100
                """)
                pending_scans = cursor.fetchall()
            
            if not pending_scans:
                return 0
            
            db = get_db()
            synced_count = 0
            
            for scan_row in pending_scans:
                try:
                    # Acceso seguro a columnas (sqlite3.Row no soporta .get())
                    row_keys = set(scan_row.keys())
                    scan_format = scan_row['scan_format'] if 'scan_format' in row_keys else 'QR'
                    is_complete = scan_row['is_complete'] if 'is_complete' in row_keys else 1
                    linked_scan_id = scan_row['linked_scan_id'] if 'linked_scan_id' in row_keys else None
                    # Solo procesar una vez el par usando la fila QR (o BARCODE si no encontramos QR por algún motivo)
                    if scan_format == 'BARCODE':
                        # Si es barcode y su par QR ya fue sincronizado (linked y synced) lo marcamos
                        if linked_scan_id:
                            with self._get_sqlite_connection() as conn:
                                conn.execute("UPDATE scans_local SET synced_to_mysql = 1 WHERE id = ?", (scan_row['id'],))
                                conn.commit()
                        continue
                    if scan_format == 'QR' and is_complete != 1:
                        # Esperar a que esté completo
                        continue

                    # Obtener el BARCODE vinculado para consolidar
                    barcode_row = None
                    if linked_scan_id:
                        with self._get_sqlite_connection() as conn_l:
                            conn_l.row_factory = sqlite3.Row
                            cur2 = conn_l.execute("SELECT * FROM scans_local WHERE id = ?", (linked_scan_id,))
                            barcode_row = cur2.fetchone()

                    if not barcode_row:
                        # No podemos consolidar sin barcode; continuar
                        continue

                    # Construir datos consolidado (usamos info base del QR y extra del BARCODE)
                    # Ajustar lote al lot_no del plan si aplica
                    lote_final = scan_row['lote']
                    try:
                        with self._get_sqlite_connection() as connp:
                            connp.row_factory = sqlite3.Row
                            curp = connp.execute("""
                                SELECT lot_no FROM plan_local
                                WHERE line = ? AND part_no = ? AND status = 'EN PROGRESO'
                                ORDER BY id LIMIT 1
                            """, (scan_row['linea'], scan_row['nparte']))
                            prow = curp.fetchone()
                            if prow and prow['lot_no']:
                                # Si el lote actual parece formato interno (IYYYYMMDD-XXXX) lo reemplazamos
                                import re
                                if re.match(r'^[A-Z]\d{8}-\d{4}$', lote_final) or lote_final != prow['lot_no']:
                                    lote_final = prow['lot_no']
                    except Exception:
                        pass

                    pair_data = {
                        'ts': scan_row['ts'],
                        'raw_qr': scan_row['raw'],
                        'raw_barcode': barcode_row['raw'],
                        'raw_pair': f"{scan_row['raw']}|{barcode_row['raw']}",
                        'tipo': scan_row['tipo'],
                        'fecha': scan_row['fecha'],
                        'lote': lote_final,
                        'secuencia': scan_row['secuencia'],
                        'estacion': scan_row['estacion'],
                        'nparte': scan_row['nparte'],
                        'modelo': scan_row['modelo'],
                        'cantidad': 1,
                        'linea': scan_row['linea'],
                        'barcode_sequence': barcode_row['barcode_sequence']
                    }

                    # Insertar salida según modo
                    try:
                        if settings.APP_MODE == 'IMD':
                            db.insert_output_imd({
                                'ts': pair_data['ts'],
                                'raw': pair_data['raw_pair'],
                                'tipo': pair_data['tipo'],
                                'fecha': pair_data['fecha'],
                                'lote': pair_data['lote'],
                                'secuencia': pair_data['secuencia'],
                                'estacion': pair_data['estacion'],
                                'nparte': pair_data['nparte'],
                                'modelo': pair_data['modelo'],
                                'cantidad': pair_data['cantidad'],
                                'linea': pair_data['linea'],
                            })
                        else:
                            db.insert_pair_scan(pair_data)
                    except Exception as ie:
                        logger.error(f"❌ Error insertando par consolidado MySQL id_local={scan_row['id']}: {ie}")
                        # NO marcar como sincronizado - reintentar en próximo ciclo
                        # Solo marcar si es error de duplicado (que significa que ya existe en MySQL)
                        error_msg = str(ie).lower()
                        if 'duplicate' in error_msg or 'unique constraint' in error_msg:
                            logger.warning(f"⚠️ Duplicado en MySQL para id_local={scan_row['id']}, marcando como sincronizado")
                            try:
                                with self._get_sqlite_connection() as conn_err:
                                    conn_err.execute("UPDATE scans_local SET synced_to_mysql = 1 WHERE id IN (?,?)", (scan_row['id'], linked_scan_id))
                                    conn_err.commit()
                            except Exception:
                                pass
                        # Para otros errores (conexión, timeout, etc.) NO marcar como sincronizado
                        # Se reintentará en el próximo ciclo (cada 3 segundos)
                        continue

                    # Actualizar producción diaria (cantidad=1 pieza)
                    try:
                        if settings.APP_MODE == 'IMD':
                            db.update_daily_production_imd(
                                fecha=date.fromisoformat(scan_row['fecha']),
                                linea=scan_row['linea'],
                                nparte=scan_row['nparte'],
                                cantidad=1,
                                modelo=scan_row['modelo']
                            )
                        else:
                            db.update_daily_production(
                                fecha=date.fromisoformat(scan_row['fecha']),
                                linea=scan_row['linea'],
                                nparte=scan_row['nparte'],
                                cantidad=1,
                                modelo=scan_row['modelo']
                            )
                    except Exception as de:
                        logger.error(f"Error update_daily_production par {scan_row['id']}: {de}")

                    # Marcar ambos como sincronizados
                    with self._get_sqlite_connection() as conn_u:
                        conn_u.execute("UPDATE scans_local SET synced_to_mysql = 1 WHERE id IN (?,?)", (scan_row['id'], linked_scan_id))
                        conn_u.commit()

                    synced_count += 1
                    
                except Exception as e:
                    logger.error(f"Error sincronizando escaneo {scan_row['id']}: {e}")
            
            return synced_count
            
        except Exception as e:
            logger.error(f"Error en _sync_scans_to_mysql: {e}")
            return 0
    
    def _sync_totals_to_mysql(self) -> int:
        """Sincroniza totales pendientes a MySQL"""
        try:
            from ..db import get_db
            today = get_today_mexico_str()
            db = get_db()

            # Calcular uph_real (última hora) por linea+nparte basado en piezas completas
            with self._get_sqlite_connection() as conn:
                conn.row_factory = sqlite3.Row
                from datetime import datetime, timedelta
                ts_limit = (datetime.now(ZoneInfo(settings.TZ)) - timedelta(hours=1)).isoformat()
                cursor = conn.execute("""
                    WITH last_hour AS (
                        SELECT linea, nparte, COUNT(*)/2 AS piezas_hora
                        FROM scans_local
                        WHERE ts >= ? AND is_complete = 1
                        GROUP BY linea, nparte
                    )
                    SELECT p.fecha, p.linea, p.nparte, p.modelo, p.cantidad_total, p.uph_target, 
                           COALESCE(lh.piezas_hora,0) AS uph_real
                    FROM production_totals_local p
                    LEFT JOIN last_hour lh ON lh.linea = p.linea AND lh.nparte = p.nparte
                    WHERE p.fecha = ? AND p.synced_to_mysql = 0
                """, (ts_limit, today))
                rows = cursor.fetchall()

            if not rows:
                return 0

            synced = 0
            for r in rows:
                try:
                    db.update_daily_production(
                        fecha=date.fromisoformat(r['fecha']),
                        linea=r['linea'],
                        nparte=r['nparte'],
                        cantidad=0,  # ya contabilizado, solo refrescar uph_target/modelo
                        modelo=r['modelo']
                    )
                    # Actualizar uph_real explícitamente
                    db.update_uph_real(
                        fecha=date.fromisoformat(r['fecha']),
                        linea=r['linea'],
                        nparte=r['nparte'],
                        uph_real=r['uph_real']
                    )
                    # Marcar como sincronizado
                    with self._get_sqlite_connection() as conn:
                        conn.execute("UPDATE production_totals_local SET synced_to_mysql = 1 WHERE fecha=? AND linea=? AND nparte=?", (r['fecha'], r['linea'], r['nparte']))
                        conn.commit()
                    synced += 1
                except Exception as e:
                    logger.error(f"Error sincronizando total {r['nparte']}:{e}")
            return synced
        except Exception as e:
            logger.error(f"Error en _sync_totals_to_mysql: {e}")
            return 0
    
    def _update_models_from_raw(self):
        """Actualiza modelos desde tabla raw de MySQL"""
        try:
            from ..db import get_db
            
            # Obtener registros sin modelo
            with self._get_sqlite_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT DISTINCT nparte FROM scans_local 
                    WHERE modelo IS NULL 
                    LIMIT 20
                """)
                npartes_sin_modelo = [row['nparte'] for row in cursor.fetchall()]
            
            if not npartes_sin_modelo:
                return
            
            db = get_db()
            
            for nparte in npartes_sin_modelo:
                modelo_ref = db.get_modelo_by_nparte(nparte)
                if modelo_ref:
                    # Actualizar en SQLite local
                    with self._get_sqlite_connection() as conn:
                        conn.execute("""
                            UPDATE scans_local 
                            SET modelo = ? 
                            WHERE nparte = ? AND modelo IS NULL
                        """, (modelo_ref.modelo, nparte))
                        
                        conn.execute("""
                            UPDATE production_totals_local 
                            SET modelo = ?, uph_target = ?, synced_to_mysql = 0
                            WHERE nparte = ? AND modelo IS NULL
                        """, (modelo_ref.modelo, modelo_ref.uph, nparte))
                        
                        conn.commit()
        
        except Exception as e:
            logger.error(f"Error actualizando modelos desde raw: {e}")

    # -------- Plan Main Sync ---------
    def _fetch_plan_for_part(self, linea: str, part_no: str) -> bool:
        """Obtiene un plan específico desde MySQL y lo cachea local."""
        try:
            from ..db import get_db
            db = get_db()
            today = get_today_mexico()

            with db.get_connection() as conn_mysql:
                with conn_mysql.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, lot_no, wo_code, po_code, working_date, line, model_code,
                               part_no, project, process, plan_count, ct, uph, routing, status,
                               COALESCE(produced_count,0) AS produced_count,
                               COALESCE(sequence,0) AS sequence, started_at,
                               planned_start, planned_end, effective_minutes
                        FROM plan_main
                        WHERE line = %s AND working_date = %s AND part_no = %s
                        LIMIT 1
                    """, (linea, today, part_no))

                    row = cursor.fetchone()
                    if not row:
                        return False

                    if isinstance(row, dict):
                        row_dict = dict(row)
                    else:
                        keys = ["id", "lot_no", "wo_code", "po_code", "working_date", "line",
                                "model_code", "part_no", "project", "process", "plan_count",
                                "ct", "uph", "routing", "status", "produced_count", "sequence",
                                "started_at", "planned_start", "planned_end", "effective_minutes"]
                        row_dict = {k: row[idx] for idx, k in enumerate(keys)}

            working_date_str = row_dict['working_date'].isoformat() if row_dict.get('working_date') else today.isoformat()

            mysql_started_at = row_dict.get('started_at')
            if mysql_started_at and not isinstance(mysql_started_at, str):
                mysql_started_at = mysql_started_at.strftime('%Y-%m-%d %H:%M:%S')

            planned_start = row_dict.get('planned_start')
            if planned_start and not isinstance(planned_start, str):
                planned_start = planned_start.strftime('%Y-%m-%d %H:%M:%S')

            planned_end = row_dict.get('planned_end')
            if planned_end and not isinstance(planned_end, str):
                planned_end = planned_end.strftime('%Y-%m-%d %H:%M:%S')

            effective_minutes = row_dict.get('effective_minutes', 0) or 0

            with self._get_sqlite_connection(timeout=5.0) as conn:
                conn.row_factory = sqlite3.Row
                cur_loc = conn.execute("SELECT produced_count FROM plan_local WHERE id=?", (row_dict['id'],))
                existing = cur_loc.fetchone()
                local_pc = existing['produced_count'] if existing else 0
                remote_pc = row_dict.get('produced_count', 0) or 0

                conn.execute("""
                    INSERT INTO plan_local (id, working_date, line, part_no, lot_no, model_code, plan_count, produced_count,
                                            uph, ct, status, sequence, started_at, planned_start, planned_end,
                                            effective_minutes, updated_at, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), NULL)
                    ON CONFLICT(id) DO UPDATE SET
                        plan_count = excluded.plan_count,
                        lot_no = excluded.lot_no,
                        produced_count = CASE WHEN plan_local.produced_count > excluded.produced_count
                                              THEN plan_local.produced_count ELSE excluded.produced_count END,
                        uph = excluded.uph,
                        ct = excluded.ct,
                        status = CASE 
                            WHEN plan_local.status = 'EN PROGRESO' AND excluded.status <> 'EN PROGRESO' 
                                THEN plan_local.status 
                            ELSE excluded.status 
                        END,
                        sequence = excluded.sequence,
                        started_at = COALESCE(excluded.started_at, plan_local.started_at),
                        planned_start = excluded.planned_start,
                        planned_end = excluded.planned_end,
                        effective_minutes = excluded.effective_minutes,
                        updated_at = excluded.updated_at
                """, (
                    row_dict['id'], working_date_str, row_dict['line'], row_dict['part_no'],
                    row_dict.get('lot_no'), row_dict.get('model_code'), row_dict.get('plan_count'),
                    max(local_pc, remote_pc), row_dict.get('uph'), row_dict.get('ct'),
                    row_dict.get('status'), row_dict.get('sequence', 0), mysql_started_at,
                    planned_start, planned_end, effective_minutes
                ))

                conn.commit()

            return True
        except Exception as e:
            logger.error(f"Error trayendo plan {part_no} linea {linea} desde MySQL: {e}")
            return False

    def _sync_plan_from_mysql(self):
        """Descarga plan para las líneas activas del día y lo cachea local.
        Usa plan_main en modo ASSY y plan_imd en modo IMD.
        """
        import time
        
        sync_start = time.time()
        
        try:
            from ..db import get_db
            db = get_db()
            today = get_today_mexico()
            # Determinar líneas en uso (todas las visibles en UI) según modo
            if settings.APP_MODE == 'IMD':
                lineas = ["PANA A", "PANA B", "PANA C", "PANA D"]
            else:
                lineas = ["M1", "M2", "M3", "M4", "D1", "D2", "D3", "H1"]
            
            logger.info(f"[PLAN] Modo actual: {settings.APP_MODE} | Fuente: {'plan_imd' if settings.APP_MODE=='IMD' else 'plan_main'}")
            
            # Usar timeout corto - si falla, se reintentará en 15 segundos
            try:
                with self._get_sqlite_connection(timeout=1.0, check_same_thread=False) as conn:
                    conn.row_factory = sqlite3.Row
                    
                    # Si el modo cambió desde la última sincronización, limpiar el cache de hoy
                    try:
                        if self._last_plan_mode is None or self._last_plan_mode != settings.APP_MODE:
                            conn.execute("DELETE FROM plan_local WHERE working_date = ?", (today.isoformat(),))
                            conn.commit()
                            # Evitar enviar increments pertenecientes al modo anterior
                            self._plan_produced_buffer = {}
                            logger.info(f"[PLAN] Cache local de hoy limpiado por cambio de modo: {self._last_plan_mode} -> {settings.APP_MODE}")
                            self._last_plan_mode = settings.APP_MODE
                    except Exception:
                        # No bloquear si no se puede limpiar; continuará con upserts
                        pass
                    
                    # Procesar líneas en lotes pequeños para evitar locks largos
                    for linea in lineas:
                        # Limpiar registros antiguos para esta línea antes de insertar nuevos
                        today_str = today.isoformat()
                        conn.execute("DELETE FROM plan_local WHERE line=? AND working_date != ?", (linea, today_str))
                        
                        if settings.APP_MODE == 'IMD':
                            rows_raw = db.get_plan_for_line_imd(linea, today)
                        else:
                            rows_raw = db.get_plan_for_line(linea, today)
                        
                        logger.info(f"[PLAN] MySQL devolvió {len(rows_raw)} registros para línea {linea}")
                        
                        # Filtrar cancelados
                        rows = [r for r in rows_raw if str(r.get('status','')).upper() not in ('CANCELLED','CANCELADO')]
                        if not rows:
                            logger.debug(f"[PLAN] Sin datos para línea {linea}")
                            continue
                        
                        logger.debug(f"[PLAN] Procesando {len(rows)} filas para línea {linea}")
                        for r in rows:
                            # Preservar avance local si es mayor (evita reinicio)
                            cur_loc = conn.execute("SELECT produced_count FROM plan_local WHERE id=?", (r['id'],))
                            existing = cur_loc.fetchone()
                            local_pc = existing['produced_count'] if existing else 0
                            remote_pc = r.get('produced_count', 0)
                            
                            working_date_str = r['working_date'].isoformat() if r['working_date'] else today.isoformat()
                            
                            # Obtener started_at de MySQL y convertirlo a string para SQLite
                            mysql_started_at = r.get('started_at')
                            if mysql_started_at and not isinstance(mysql_started_at, str):
                                # Convertir datetime a string en formato ISO
                                mysql_started_at = mysql_started_at.strftime('%Y-%m-%d %H:%M:%S')
                            
                            # Obtener planned_start, planned_end y effective_minutes
                            planned_start = r.get('planned_start')
                            if planned_start and not isinstance(planned_start, str):
                                planned_start = planned_start.strftime('%Y-%m-%d %H:%M:%S')
                            
                            planned_end = r.get('planned_end')
                            if planned_end and not isinstance(planned_end, str):
                                planned_end = planned_end.strftime('%Y-%m-%d %H:%M:%S')
                            
                            effective_minutes = r.get('effective_minutes', 0) or 0
                            
                            # 🛡️ PROTECCIÓN CONTRA SOBRESCRITURA: Sumar incrementos pendientes del buffer
                            plan_id = r['id']
                            pending_increment = self._plan_produced_buffer.get(plan_id, 0)
                            
                            # Si hay incrementos pendientes NO sincronizados, sumarlos SOLO a remote_pc
                            # NO sumar a local_pc porque eso infla el valor en SQLite
                            if pending_increment > 0:
                                logger.warning(f"🔒 PROTECCIÓN: Plan {plan_id} tiene {pending_increment} unidades pendientes en buffer, sumando a MySQL ({remote_pc} + {pending_increment} = {remote_pc + pending_increment})")
                                remote_pc += pending_increment
                            
                            conn.execute("""
                                INSERT INTO plan_local (id, working_date, line, part_no, lot_no, model_code, plan_count, produced_count, uph, ct, status, sequence, started_at, planned_start, planned_end, effective_minutes, updated_at, synced_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(id) DO UPDATE SET
                                    plan_count=excluded.plan_count,
                                    lot_no=excluded.lot_no,
                                    produced_count=CASE WHEN plan_local.produced_count > excluded.produced_count THEN plan_local.produced_count ELSE excluded.produced_count END,
                                    uph=excluded.uph,
                                    ct=excluded.ct,
                                    status=CASE 
                                        WHEN plan_local.status = 'EN PROGRESO' AND excluded.status <> 'EN PROGRESO' 
                                            THEN plan_local.status 
                                        ELSE excluded.status 
                                    END,
                                    sequence=excluded.sequence,
                                    started_at=COALESCE(excluded.started_at, plan_local.started_at),
                                    planned_start=excluded.planned_start,
                                    planned_end=excluded.planned_end,
                                    effective_minutes=excluded.effective_minutes,
                                    updated_at=excluded.updated_at,
                                    synced_at=excluded.synced_at
                            """, (
                                r['id'], working_date_str,
                                r['line'], r['part_no'], r.get('lot_no') if isinstance(r, dict) else r.get('lot_no',''),
                                r.get('model_code') if isinstance(r, dict) else r['model_code'],
                                r['plan_count'], max(local_pc, remote_pc), r.get('uph'), r.get('ct'), r.get('status'),
                                r.get('sequence', 0),
                                mysql_started_at,
                                planned_start,
                                planned_end,
                                effective_minutes,
                                datetime.now(ZoneInfo(settings.TZ)).isoformat(), datetime.now(ZoneInfo(settings.TZ)).isoformat()
                            ))
                            
                            # 📊 LOG: Detectar cambios en produced_count durante sync
                            if local_pc != max(local_pc, remote_pc):
                                if remote_pc > local_pc:
                                    logger.info(f"📈 SYNC: Plan {plan_id} ({r['part_no']}) - MySQL tiene MÁS producción (Local: {local_pc} < MySQL: {remote_pc}) - Actualizando a {remote_pc}")
                                else:
                                    logger.warning(f"⚠️ SYNC: Plan {plan_id} ({r['part_no']}) - Local tiene MÁS producción (Local: {local_pc} > MySQL: {remote_pc}) - PRESERVANDO {local_pc}")
                        
                        # Commit después de cada línea para reducir tiempo de lock
                        conn.commit()
                    
                    # Limpieza de duplicados: conservar fila con datos (lot_no o model_code) y eliminar vacías si existe otra
                    try:
                        conn.execute("""
                            DELETE FROM plan_local
                            WHERE ( (lot_no IS NULL OR lot_no='') AND (model_code IS NULL OR model_code='') )
                            AND id IN (
                                SELECT vac.id FROM plan_local vac
                                JOIN plan_local filled ON filled.line = vac.line
                                  AND filled.working_date = vac.working_date
                                  AND filled.part_no = vac.part_no
                                  AND ( (filled.lot_no IS NOT NULL AND filled.lot_no <> '') OR (filled.model_code IS NOT NULL AND filled.model_code <> '') )
                            )
                        """)
                    except Exception:
                        pass
                    
                    # 🔒 LIMPIEZA: Garantizar solo UN plan EN PROGRESO por línea
                    # Si hay múltiples, mantener el de menor secuencia
                    try:
                        conn.execute("""
                            UPDATE plan_local
                            SET status = 'PAUSADO'
                            WHERE id IN (
                                SELECT id FROM (
                                    SELECT id, line, sequence,
                                           ROW_NUMBER() OVER (PARTITION BY line ORDER BY sequence) as rn
                                    FROM plan_local
                                    WHERE working_date = ? AND status = 'EN PROGRESO'
                                ) sub
                                WHERE rn > 1
                            )
                        """, (today,))
                        logger.debug("✅ Limpieza de planes EN PROGRESO duplicados completada")
                    except Exception as e:
                        logger.warning(f"Error en limpieza de planes duplicados: {e}")
                    
                    #  DETECTAR CAMBIOS Y NOTIFICAR A LA UI (ANTES del commit y dentro del with)
                    # Contar planes actuales para detectar cambios (nuevos, eliminados o modificados)
                    cursor = conn.execute("SELECT COUNT(*) as total, SUM(plan_count) as sum_plan FROM plan_local WHERE working_date = ?", (today.isoformat(),))
                    current_state = cursor.fetchone()
                    current_hash = f"{current_state[0]}_{current_state[1]}"  # total_planes_suma_plan_count
                    
                    # Si cambió desde la última sincronización, marcar para recargar UI
                    if not hasattr(self, '_last_plan_hash') or self._last_plan_hash != current_hash:
                        logger.warning(f"📊 CAMBIOS DETECTADOS EN PLAN: {getattr(self, '_last_plan_hash', 'N/A')} → {current_hash}")
                        self._plan_changed = True  # Flag para que la UI detecte el cambio
                        self._last_plan_hash = current_hash
                    else:
                        self._plan_changed = False
                    
                    conn.commit()
                
                # 🔄 INVALIDAR CACHE: Forzar recarga de plan en siguiente consulta
                self._plan_cache = []
                
                sync_duration = time.time() - sync_start
                logger.info(f"✅ Plan sincronizado a cache local en {sync_duration:.2f}s")
                
            except sqlite3.OperationalError as e:
                # Si está bloqueado, saltarse esta sincronización - reintentará en 15 segundos
                if "locked" in str(e).lower():
                    logger.warning(f"⚠️ SQLite bloqueado, se saltará esta sincronización (reintentará en 15s)")
                    return
                else:
                    raise
                    
        except Exception as e:
            sync_duration = time.time() - sync_start
            logger.error(f"❌ Error sincronizando plan después de {sync_duration:.2f}s: {e}", exc_info=True)

    def _push_plan_produced_increments(self):
        """Envía a MySQL los increments acumulados de produced_count."""
        if not self._plan_produced_buffer:
            logger.debug("📤 PUSH: Buffer vacío, nada que enviar")
            return
        
        logger.info(f"📤 PUSH INICIADO: {len(self._plan_produced_buffer)} planes con incrementos pendientes")
        pushed_count = 0
        
        try:
            from ..db import get_db
            db = get_db()
            for plan_id, delta in list(self._plan_produced_buffer.items()):
                if delta <= 0:
                    continue
                
                try:
                    if settings.APP_MODE == 'IMD':
                        db.increment_plan_produced_imd(plan_id, delta)
                    else:
                        db.increment_plan_produced(plan_id, delta)
                    
                    # ✅ Solo vaciar el buffer SI el push fue exitoso
                    logger.info(f"✅ PUSH: Plan {plan_id} incrementado +{delta} en MySQL")
                    self._plan_produced_buffer[plan_id] = 0
                    pushed_count += 1
                    
                except Exception as e_push:
                    logger.error(f"❌ PUSH ERROR: Plan {plan_id} falló al enviar +{delta}: {e_push}")
                    # NO limpiar el buffer - se reintentará en el próximo ciclo
            
            # Limpiar entradas vacías
            self._plan_produced_buffer = {k:v for k,v in self._plan_produced_buffer.items() if v>0}
            
            remaining = len(self._plan_produced_buffer)
            if remaining > 0:
                logger.warning(f"⚠️ PUSH: {remaining} planes AÚN tienen incrementos pendientes (fallaron)")
            
            logger.info(f"✅ PUSH COMPLETADO: {pushed_count} planes sincronizados a MySQL")
            
        except Exception as e:
            logger.error(f"❌ PUSH ERROR GENERAL: {e}", exc_info=True)

    def is_part_allowed(self, linea: str, nparte: str) -> bool:
        """Verifica si un n° parte está en el plan vigente de la línea (plan_count > produced_count)."""
        try:
            today = get_today_mexico_str()
            with self._get_sqlite_connection() as conn:
                cursor = conn.execute("""
                    SELECT plan_count, produced_count FROM plan_local
                    WHERE line=? AND working_date=? AND part_no=? AND status NOT IN ('CANCELLED', 'CLOSED')
                    LIMIT 1
                """, (linea, today, nparte))
                row = cursor.fetchone()
                if not row:
                    return False
                plan_count, produced = row
                return produced < plan_count if plan_count is not None else True
        except Exception:
            return True  # Falla abierta para no bloquear operación si hay error

    def increment_local_plan_produced(self, linea: str, nparte: str, delta: int = 1, lote: str = None):
        start_time = time.perf_counter()
        logger.info(f"🔢 produced++ INICIO linea={linea} parte={nparte} lote={lote} delta={delta}")
        try:
            today = get_today_mexico_str()
            logger.info(f"📅 Fecha hoy: {today}")
            plan_id = None
            
            # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
            with self._sqlite_lock:
                for attempt in range(3):  # ⚡ Reducido de 10 a 3 intentos para evitar congelamiento
                    try:
                        with self._get_sqlite_connection(timeout=2.0) as conn:  # ⚡ Reducido timeout  # ⚡ 2s en vez de 5s
                            
                            # 🎯 BUSCAR SOLO EL PLAN EN PROGRESO (el lote del QR NO importa)
                            # Solo buscamos por: línea + fecha + nparte + status EN PROGRESO
                            cur = conn.execute(
                                """
                                SELECT id, produced_count, status, lot_no FROM plan_local 
                                WHERE line=? AND working_date=? AND part_no=? 
                                  AND status='EN PROGRESO'
                                LIMIT 1
                                """,
                                (linea, today, nparte),
                            )
                            
                            r = cur.fetchone()
                            if r:
                                plan_id = r[0]
                                current_produced = r[1] or 0
                                status = r[2]
                                plan_lote = r[3]
                                logger.info(f"📊 Plan EN PROGRESO encontrado: id={plan_id}, lote_plan={plan_lote}, lote_qr={lote}, produced_count actual={current_produced}")
                                
                                # 🎯 ACTUALIZAR SOLO ESE PLAN ESPECÍFICO (por ID)
                                rows_updated = conn.execute(
                                    """
                                    UPDATE plan_local SET produced_count = produced_count + ?
                                    WHERE id=?
                                    """,
                                    (delta, plan_id),
                                ).rowcount
                                logger.info(f"✅ UPDATE ejecutado: {rows_updated} filas afectadas para plan_id={plan_id}")
                            else:
                                logger.warning(f"⚠️ NO se encontró plan EN PROGRESO para linea={linea}, fecha={today}, nparte={nparte}")
                                rows_updated = 0
                            
                            conn.commit()
                        break
                    except sqlite3.OperationalError as e:
                        if "locked" in str(e).lower() and attempt < 2:  # ⚡ Máximo 2 reintentos
                            logger.debug(f"produced++ retry {attempt+1} por lock: {e}")
                            time.sleep(0.01 * (attempt + 1))  # ⚡ Sleep reducido a 10ms, 20ms
                            continue
                        raise

            if plan_id:
                # ⚡ Solo acumular en buffer, NO empujar a MySQL aquí (el worker lo hace cada 15s)
                old_buffer_value = self._plan_produced_buffer.get(plan_id, 0)
                self._plan_produced_buffer[plan_id] = old_buffer_value + delta
                logger.info(f"📦 BUFFER: Plan {plan_id} acumulado {old_buffer_value} + {delta} = {self._plan_produced_buffer[plan_id]} (pendiente sync a MySQL)")
                
                self._set_plan_active_timestamp(linea, plan_id)
                # ⚡ ELIMINADO: No llamar _push_plan_produced_increments() aquí para evitar bloqueo MySQL
                # El worker background lo sincronizará automáticamente cada 15s
                elapsed = time.perf_counter() - start_time
                logger.info(f"✅ produced++ OK linea={linea} parte={nparte} plan_id={plan_id} ({elapsed:.3f}s)")
            else:
                logger.warning(f"⚠️ produced++ SIN plan_id linea={linea} parte={nparte} - NO se incrementó")
        except Exception as e:
            logger.warning(f"Error incrementando produced_count local: {e}")

    def get_plan_for_line_local(self, linea: str) -> list:
        """Devuelve plan (lista de dicts) para la línea desde cache local."""
        try:
            today = get_today_mexico_str()
            logger.debug(f"[PLAN] Buscando plan local para línea {linea}, fecha: {today}")
            
            # Leer rápido para no bloquear la UI si hay escrituras en curso
            with self._get_sqlite_connection(timeout=0.5) as conn:
                conn.row_factory = sqlite3.Row
                
                cur = conn.execute("""
                    SELECT id, part_no, lot_no, model_code, plan_count, produced_count, status, uph, ct, sequence, updated_at, started_at, line,
                           planned_start, planned_end, effective_minutes
                    FROM plan_local
                    WHERE line=? AND working_date=?
                    ORDER BY COALESCE(sequence,0), id
                """, (linea, today))
                result = [dict(r) for r in cur.fetchall()]
                logger.debug(f"[PLAN] Plan local para línea {linea}: {len(result)} registros encontrados")
                return result
        except Exception as e:
            logger.error(f"Error leyendo plan local: {e}")
            return []

    def get_uph_real_line_map(self, linea: str) -> dict:
        """Mapa nparte-> UPH real (piezas completas última hora) para la línea.
        Usa una sola consulta agrupada para eficiencia.
        """
        try:
            from datetime import datetime, timedelta
            # Lectura no bloqueante para UI
            with self._get_sqlite_connection(timeout=0.5) as conn:
                conn.row_factory = sqlite3.Row
                ts_limit = (datetime.now(ZoneInfo(settings.TZ)) - timedelta(hours=1)).isoformat()
                cur = conn.execute("""
                    SELECT nparte, COUNT(*)/2 AS uph_real
                    FROM scans_local
                    WHERE linea = ? AND is_complete = 1 AND ts >= ?
                    GROUP BY nparte
                """, (linea, ts_limit))
                return {r['nparte']: int(r['uph_real']) for r in cur.fetchall()}
        except Exception as e:
            logger.error(f"Error obteniendo uph_real linea {linea}: {e}")
            return {}

    def get_uph_real_with_projection(self, linea: str) -> dict:
        """Devuelve mapa nparte -> {'actual':x,'projected':y,'elapsed_min':m}
        actual: piezas completas última hora (máx 60 min)
        projected: si elapsed_min < 60 y >=1, extrapola piezas * 60 / elapsed_min
        elapsed_min: minutos desde la primera pieza completa (dentro de la hora) hasta ahora
        """
        try:
            from datetime import datetime, timedelta
            now = datetime.now(ZoneInfo(settings.TZ))
            ts_limit = (now - timedelta(hours=1)).isoformat()
            # Lectura no bloqueante para UI
            with self._get_sqlite_connection(timeout=0.5) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute("""
                    SELECT nparte, COUNT(*)/2 AS piezas, MIN(ts) AS first_ts, MAX(ts) AS last_ts
                    FROM scans_local
                    WHERE linea=? AND is_complete=1 AND ts >= ?
                    GROUP BY nparte
                """, (linea, ts_limit))
                result = {}
                for r in cur.fetchall():
                    piezas = float(r['piezas']) if r['piezas'] is not None else 0.0
                    first_ts = r['first_ts']
                    elapsed_min = 60.0
                    projected = piezas
                    if first_ts:
                        try:
                            dt_first = datetime.fromisoformat(first_ts)
                            elapsed_sec = (now - dt_first).total_seconds()
                            elapsed_min = max(elapsed_sec / 60.0, 0.01)
                            if elapsed_min < 60 and elapsed_min >= 1 and piezas > 0:
                                projected = round(piezas * 60 / elapsed_min, 2)
                            else:
                                projected = piezas
                        except Exception:
                            elapsed_min = 60.0
                    result[r['nparte']] = {
                        'actual': int(piezas),
                        'projected': int(projected) if projected.is_integer() else projected,
                        'elapsed_min': round(elapsed_min,2)
                    }
                return result
        except Exception as e:
            logger.error(f"Error cálculo proyectado UPH linea {linea}: {e}")
            return {}

    def _migrate_plan_local_schema(self, conn):
        """Verifica columnas requeridas y migra plan_local si faltan (id o lot_no)."""
        try:
            cursor = conn.execute("PRAGMA table_info(plan_local)")
            cols = {row[1]: row[2] for row in cursor.fetchall()}
            needs_recreate = False
            if 'id' not in cols:
                needs_recreate = True
            # If we need add lot_no only, we can ALTER
            if not needs_recreate and 'lot_no' not in cols:
                try:
                    conn.execute("ALTER TABLE plan_local ADD COLUMN lot_no TEXT")
                except Exception:
                    pass
            if needs_recreate:
                # Rename old table
                conn.execute("ALTER TABLE plan_local RENAME TO plan_local_old")
                # Create new table with correct schema
                conn.execute("""
                    CREATE TABLE plan_local (
                        id INTEGER PRIMARY KEY,
                        working_date TEXT NOT NULL,
                        line TEXT NOT NULL,
                        part_no TEXT NOT NULL,
                        lot_no TEXT,
                        model_code TEXT,
                        plan_count INTEGER NOT NULL,
                        produced_count INTEGER DEFAULT 0,
                        uph INTEGER,
                        ct INTEGER,
                        status TEXT,
                        updated_at TEXT,
                        synced_at TEXT
                    )
                """)
                # Attempt to copy data best-effort
                try:
                    # If old table lacked id we fabricate incremental rowid
                    old_cols_cursor = conn.execute("PRAGMA table_info(plan_local_old)")
                    old_cols = [r[1] for r in old_cols_cursor.fetchall()]
                    # Build mapping
                    select_cols = []
                    insert_cols = ["id","working_date","line","part_no","lot_no","model_code","plan_count","produced_count","uph","ct","status","updated_at","synced_at"]
                    # Use rowid as id if not present
                    if 'id' in old_cols:
                        select_cols.append('id')
                    else:
                        select_cols.append('rowid as id')
                    for c in ["working_date","line","part_no","lot_no","model_code","plan_count","produced_count","uph","ct","status","updated_at","synced_at"]:
                        if c in old_cols:
                            select_cols.append(c)
                        else:
                            # Provide default values
                            if c == 'lot_no' or c == 'model_code' or c == 'status':
                                select_cols.append(f"'' AS {c}")
                            elif c in ('plan_count','produced_count','uph','ct'):
                                select_cols.append(f"0 AS {c}")
                            else:
                                select_cols.append(f"NULL AS {c}")
                    select_clause = ",".join(select_cols)
                    conn.execute(f"INSERT OR IGNORE INTO plan_local ({','.join(insert_cols)}) SELECT {select_clause} FROM plan_local_old")
                except Exception:
                    pass
                try:
                    conn.execute("DROP TABLE plan_local_old")
                except Exception:
                    pass
                conn.execute("CREATE INDEX IF NOT EXISTS idx_plan_line_date ON plan_local(line, working_date)")
            conn.commit()
        except Exception as e:
            logger.error(f"Error migrando plan_local: {e}")
    
    def _update_sub_assy_cache(self):
        """Actualiza cache SUB ASSY desde tabla raw de MySQL (solo nuevos/actualizados)"""
        try:
            from ..db import get_db
            
            # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
            with self._sqlite_lock:
                # Obtener timestamp de última actualización para actualización incremental
                with self._get_sqlite_connection(timeout=5.0) as conn:
                    cursor = conn.execute("SELECT MAX(updated_at) FROM sub_assy_cache")
                    last_update = cursor.fetchone()[0] or '2020-01-01'
            
            db = get_db()
            with db.get_connection() as mysql_conn:
                with mysql_conn.cursor() as cursor:
                    # Solo obtener SUB ASSY que podrían ser nuevos o actualizados
                    # (consulta eficiente - no toda la tabla)
                    # Usar consulta más permisiva - solo verificar que sub_assy no sea NULL
                    cursor.execute("""
                        SELECT DISTINCT part_no, sub_assy 
                        FROM raw 
                        WHERE sub_assy IS NOT NULL
                        AND part_no IS NOT NULL
                        LIMIT 1000
                    """)
                    
                    updated_count = 0
                    # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
                    with self._sqlite_lock:
                        with self._get_sqlite_connection(timeout=5.0) as local_conn:
                            for part_no, sub_assy in cursor.fetchall():
                                if part_no and sub_assy:
                                    sub_assy_str = str(sub_assy).strip()
                                    
                                    # Solo procesar si no es cadena vacía
                                    if sub_assy_str:
                                        # Solo guardar el número de parte SUB ASSY, sin prefijo
                                        formatted = sub_assy_str
                                        
                                        local_conn.execute("""
                                            INSERT OR REPLACE INTO sub_assy_cache 
                                            (part_no, sub_assy, formatted_display, updated_at)
                                            VALUES (?, ?, ?, datetime('now'))
                                        """, (part_no, sub_assy_str, formatted))
                                        updated_count += 1
                            
                            local_conn.commit()
                    
                    if updated_count > 0:
                        logger.debug(f"Cache SUB ASSY actualizado: {updated_count} entradas")
                        
        except Exception as e:
            logger.error(f"Error actualizando cache SUB ASSY: {e}")
    
    def get_sub_assy_info(self, nparte: str) -> str:
        """Obtiene información SUB ASSY desde cache local (muy rápido)"""
        try:
            # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
            with self._sqlite_lock:
                # Consultar cache local primero
                with self._get_sqlite_connection(timeout=5.0) as conn:
                    cursor = conn.execute("""
                        SELECT formatted_display FROM sub_assy_cache 
                        WHERE part_no = ?
                        LIMIT 1
                    """, (nparte,))
                    result = cursor.fetchone()
                    
                    if result:
                        return result[0]
            
            # Si no está en cache local, consultar directamente MySQL y cachear
            return self._fetch_and_cache_sub_assy(nparte)
                
        except Exception as e:
            logger.error(f"Error obteniendo SUB ASSY info para {nparte}: {e}")
            return "Error SUB ASSY"
    
    def _fetch_and_cache_sub_assy(self, nparte: str) -> str:
        """Consulta SUB ASSY desde MySQL y lo cachea localmente"""
        try:
            from ..db import get_db
            
            db = get_db()
            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Consulta más permisiva - solo verificar que sub_assy no sea NULL
                    cursor.execute("""
                        SELECT sub_assy FROM raw 
                        WHERE part_no = %s AND sub_assy IS NOT NULL
                        LIMIT 1
                    """, (nparte,))
                    result = cursor.fetchone()
                    
                    logger.debug(f"SUB ASSY query para {nparte}: result={result}, type={type(result)}")
                    
                    if result:
                        # Extraer el valor dependiendo del tipo de resultado
                        if isinstance(result, (list, tuple)) and len(result) > 0:
                            sub_assy_raw = result[0]
                        elif hasattr(result, 'get'):  # dict-like
                            sub_assy_raw = result.get('sub_assy')
                        else:
                            sub_assy_raw = result
                        
                        logger.debug(f"SUB ASSY raw para {nparte}: {sub_assy_raw}, type={type(sub_assy_raw)}")
                        
                        if sub_assy_raw is not None:
                            sub_assy = str(sub_assy_raw).strip()
                            
                            # Verificar que no sea cadena vacía o nombre de columna
                            if sub_assy and sub_assy not in ('sub_assy', 'part_no', 'NULL', 'null'):
                                # Solo devolver el número de parte SUB ASSY, sin prefijo
                                formatted = sub_assy
                                
                                logger.debug(f"SUB ASSY válido para {nparte}: {sub_assy} -> {formatted}")
                                
                                # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
                                # Cachear en SQLite local
                                with self._sqlite_lock:
                                    with self._get_sqlite_connection(timeout=5.0) as local_conn:
                                        local_conn.execute("""
                                            INSERT OR REPLACE INTO sub_assy_cache 
                                            (part_no, sub_assy, formatted_display, updated_at)
                                            VALUES (?, ?, ?, datetime('now'))
                                        """, (nparte, sub_assy, formatted))
                                        local_conn.commit()
                                
                                return formatted
                    
                    # Si llegamos aquí, no hay SUB ASSY válido
                    logger.debug(f"Sin SUB ASSY válido para {nparte} (result={result})")
                    formatted = "Sin SUB ASSY"
                    # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
                    with self._sqlite_lock:
                        with self._get_sqlite_connection(timeout=5.0) as local_conn:
                            local_conn.execute("""
                                INSERT OR REPLACE INTO sub_assy_cache 
                                (part_no, sub_assy, formatted_display, updated_at)
                                VALUES (?, ?, ?, datetime('now'))
                            """, (nparte, '', formatted))
                            local_conn.commit()
                    
                    return formatted
                        
        except Exception as e:
            logger.error(f"Error consultando SUB ASSY desde MySQL para {nparte}: {e}")
            # Agregar más detalles del error
            import traceback
            logger.debug(f"Traceback completo para {nparte}: {traceback.format_exc()}")
            # En caso de error, no cachear nada para permitir reintento
            return "Error SUB ASSY"
    
    def get_modelo_local(self, nparte: str, fetch_if_missing: bool = False) -> tuple:
        """
        Obtiene modelo y UPH target para un número de parte.
        Primero busca en cache local. Si no existe y fetch_if_missing=True, 
        programa consulta MySQL en background.
        
        Args:
            nparte: Número de parte
            fetch_if_missing: Si True, programa fetch en background si no está en cache
        
        Returns:
            tuple: (modelo, uph_target) o (None, None) si no está en cache
        """
        try:
            with self._get_sqlite_connection(timeout=1.0) as conn:
                cursor = conn.execute("""
                    SELECT modelo, uph_target FROM production_totals_local 
                    WHERE nparte = ? AND modelo IS NOT NULL
                    LIMIT 1
                """, (nparte,))
                result = cursor.fetchone()
                if result:
                    return result[0], result[1]
                else:
                    # Si no está en cache y fetch_if_missing=True, programar fetch en background
                    if fetch_if_missing:
                        threading.Thread(
                            target=self._fetch_and_cache_modelo_background,
                            args=(nparte,),
                            daemon=True
                        ).start()
                    return None, None
        except Exception as e:
            logger.error(f"Error obteniendo modelo local: {e}")
            return None, None
    
    def _fetch_and_cache_modelo_background(self, nparte: str):
        """Fetch y cache modelo en background sin bloquear"""
        try:
            self._fetch_and_cache_modelo(nparte)
        except Exception as e:
            logger.debug(f"Error en background fetch de modelo para {nparte}: {e}")
    
    def _fetch_and_cache_modelo(self, nparte: str) -> tuple:
        """Consultar modelo desde MySQL 'raw' table y cachear localmente"""
        try:
            from ..db import get_db
            db = get_db()
            modelo_ref = db.get_modelo_by_nparte(nparte)
            
            if modelo_ref and modelo_ref.modelo:
                # Crear entrada en cache local
                today = get_today_mexico_str()
                with self._get_sqlite_connection() as conn:
                    conn.execute("""
                        INSERT OR IGNORE INTO production_totals_local 
                        (fecha, linea, nparte, modelo, cantidad_total, uph_target, synced_to_mysql)
                        VALUES (?, 'CACHE', ?, ?, 0, ?, 1)
                    """, (today, nparte, modelo_ref.modelo, modelo_ref.uph))
                    conn.commit()
                
                return modelo_ref.modelo, modelo_ref.uph
            else:
                return None, None
                
        except Exception as e:
            logger.error(f"Error consultando modelo desde MySQL: {e}")
            return None, None
    
    def actualizar_estado_plan_cache_only(self, plan_id: int, nuevo_estado: str, linea: str = None) -> bool:
        """
        OPTIMIZACIÓN: Actualiza SOLO el caché en memoria (INSTANTÁNEO, no toca BD)
        
        Esto permite que la UI se actualice INMEDIATAMENTE sin esperar SQLite/MySQL.
        Las bases de datos se sincronizan después en background.
        
        Args:
            plan_id: ID único del plan
            nuevo_estado: Nuevo estado (EN PROGRESO, PAUSADO, TERMINADO)
            linea: Línea de producción (opcional, para optimización)
            
        Returns:
            bool: True siempre (operación en memoria no falla)
        """
        try:
            # Actualizar caché directo en memoria (no requiere BD)
            if hasattr(self, '_plan_cache') and self._plan_cache:
                for plan in self._plan_cache:
                    if plan.get('id') == plan_id:
                        plan['status'] = nuevo_estado
                        logger.debug(f"✅ Caché actualizado: plan_id={plan_id} -> {nuevo_estado}")
                        break
            
            # Actualizar timestamps de actividad si es necesario
            if linea:
                if nuevo_estado == "EN PROGRESO":
                    self._set_plan_active_timestamp(linea, plan_id)
                else:
                    self._clear_plan_activity(linea, plan_id)
            
            return True
        except Exception as e:
            logger.error(f"Error actualizando caché de plan {plan_id}: {e}")
            return False
    
    def actualizar_estado_plan_db_only(self, plan_id: int, nuevo_estado: str, linea: str = None) -> bool:
        """
        ⚡ OPTIMIZACIÓN ULTRA-RÁPIDA: Actualiza SQLite en 1 intento (< 50ms)
        
        Se ejecuta en worker thread después de que el caché ya fue actualizado.
        NO BLOQUEA - el lock global garantiza ejecución sin conflictos.
        
        Args:
            plan_id: ID único del plan
            nuevo_estado: Nuevo estado (EN PROGRESO, PAUSADO, TERMINADO)
            linea: Línea de producción (opcional)
            
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        try:
            # ⚡ ACTUALIZAR SQLITE - 1 INTENTO RÁPIDO (el helper ya tiene lock + timeout)
            with self._get_sqlite_connection(timeout=2.0) as conn:
                # Si el nuevo estado es "EN PROGRESO", guardar el timestamp de inicio
                if nuevo_estado == "EN PROGRESO":
                    cursor = conn.execute("""
                        UPDATE plan_local 
                        SET status = ?, started_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (nuevo_estado, plan_id))
                else:
                    cursor = conn.execute("""
                        UPDATE plan_local 
                        SET status = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (nuevo_estado, plan_id))
                
                if cursor.rowcount == 0:
                    logger.warning(f"⚠️ Plan {plan_id} no encontrado en SQLite")
                    return False
                
                conn.commit()
                logger.debug(f"✅ SQLite actualizado: plan_id={plan_id} -> {nuevo_estado}")
            
            # ⚡ ACTUALIZAR MYSQL EN BACKGROUND (no esperar, no bloquear)
            import threading
            def update_mysql_async():
                try:
                    from ..db import get_db
                    db = get_db()
                    mysql_result = db.actualizar_estado_plan(plan_id, nuevo_estado)
                    if mysql_result:
                        logger.debug(f"✅ MySQL actualizado: plan_id={plan_id} -> {nuevo_estado}")
                    else:
                        logger.warning(f"⚠️ MySQL no actualizado para plan_id: {plan_id}")
                except Exception as mysql_error:
                    logger.error(f"❌ Error MySQL: {mysql_error}")
            
            # Lanzar thread y continuar inmediatamente (no bloquea)
            threading.Thread(target=update_mysql_async, daemon=True).start()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error actualizando plan {plan_id}: {e}")
            return False
    
    def actualizar_estado_plan(self, plan_id: int, nuevo_estado: str) -> bool:
        """
        Actualiza el estado de un plan específico en ambas bases de datos
        
        Args:
            plan_id: ID único del plan
            nuevo_estado: Nuevo estado (EN PROGRESO, PAUSADO, TERMINADO)
            
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        try:
            # Pausar sincronización temporalmente para evitar conflictos
            self._pause_sync_temporarily()
            
            try:
                # Actualizar en SQLite local primero (respuesta rápida)
                with self._get_sqlite_connection(timeout=10.0) as conn:
                    conn.row_factory = sqlite3.Row
                    line_row = conn.execute(
                        "SELECT line FROM plan_local WHERE id = ?",
                        (plan_id,)
                    ).fetchone()
                    plan_line = line_row["line"] if line_row else None
                    # Si el nuevo estado es "EN PROGRESO", guardar el timestamp de inicio
                    if nuevo_estado == "EN PROGRESO":
                        cursor = conn.execute("""
                            UPDATE plan_local 
                            SET status = ?, started_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (nuevo_estado, plan_id))
                    else:
                        cursor = conn.execute("""
                            UPDATE plan_local 
                            SET status = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (nuevo_estado, plan_id))
                    
                    if cursor.rowcount == 0:
                        logger.warning(f"No se encontró el plan con id: {plan_id} en SQLite local")
                        return False
                    
                    conn.commit()
                    logger.info(f"Estado actualizado en SQLite local: plan_id={plan_id} -> {nuevo_estado}")
                    if plan_line:
                        if nuevo_estado == "EN PROGRESO":
                            self._set_plan_active_timestamp(plan_line, plan_id)
                        else:
                            self._clear_plan_activity(plan_line, plan_id)
                
                # Programar actualización en MySQL (en background sin esperar)
                import threading
                def update_mysql_async():
                    try:
                        from ..db import get_db
                        db = get_db()
                        
                        # Ejecutar actualización en MySQL
                        mysql_result = db.actualizar_estado_plan(plan_id, nuevo_estado)
                        
                        if mysql_result:
                            logger.info(f"Estado actualizado en MySQL: plan_id={plan_id} -> {nuevo_estado}")
                        else:
                            logger.warning(f"No se pudo actualizar el estado en MySQL para plan_id: {plan_id}")
                            
                    except Exception as mysql_error:
                        logger.error(f"Error actualizando estado en MySQL: {mysql_error}")
                
                # Ejecutar MySQL en thread separado (no bloquea)
                mysql_thread = threading.Thread(target=update_mysql_async, daemon=True)
                mysql_thread.start()
                
                return True
                
            finally:
                # Reanudar sincronización
                self._resume_sync()
            
        except Exception as e:
            logger.error(f"Error actualizando estado del plan {plan_id}: {e}")
            # Asegurar que se reanude la sincronización en caso de error
            self._resume_sync()
            return False
    
    def verificar_planes_en_progreso(self) -> List[str]:
        """
        Verifica qué planes están actualmente en estado 'EN PROGRESO'
        
        Returns:
            List[str]: Lista de part_no de planes en progreso
        """
        try:
            # Consultar en SQLite local primero (más rápido)
            with self._get_sqlite_connection() as conn:
                cursor = conn.execute("""
                    SELECT part_no FROM plan_local 
                    WHERE status = 'EN PROGRESO'
                    ORDER BY updated_at DESC
                """)
                
                planes_en_progreso = [row[0] for row in cursor.fetchall()]
                logger.info(f"Planes en progreso encontrados: {planes_en_progreso}")
                return planes_en_progreso
                
        except Exception as e:
            logger.error(f"Error verificando planes en progreso: {e}")
            return []
    
    def verificar_planes_en_progreso_por_linea(self, linea: str) -> List[str]:
        """
        Verifica qué planes están actualmente en estado 'EN PROGRESO' en una línea específica
        
        Args:
            linea: Línea específica a verificar (M1, M2, M3, etc.)
            
        Returns:
            List[str]: Lista de part_no de planes en progreso en esa línea
        """
        try:
            # Consultar en SQLite local primero (más rápido)
            with self._get_sqlite_connection(timeout=5.0) as conn:
                cursor = conn.execute("""
                    SELECT part_no FROM plan_local 
                    WHERE status = 'EN PROGRESO' AND line = ?
                    ORDER BY updated_at DESC
                """, (linea,))
                
                planes_en_progreso = [row[0] for row in cursor.fetchall()]
                return planes_en_progreso
                
        except Exception as e:
            logger.error(f"Error verificando planes en progreso en línea {linea}: {e}")
            return []
    
    def check_plan_changed_and_reset(self) -> bool:
        """
        Verifica si el plan cambió desde la última sincronización y resetea el flag.
        
        Returns:
            bool: True si el plan cambió, False si no
        """
        changed = getattr(self, '_plan_changed', False)
        if changed:
            self._plan_changed = False  # Resetear el flag
            logger.info("🔔 UI notificada de cambios en el plan")
        return changed
    
    def stop_sync_worker(self):
        """Detiene el worker de sincronización"""
        self._sync_worker_running = False
        if self._sync_thread:
            self._sync_thread.join(timeout=5)
        
        # Limpiar caches en memoria
        self._sub_assy_cache.clear()
        
        logger.info("Worker de sincronización detenido")


    # --- Compatibilidad y configuración dinámica ---
    @property
    def _sync_worker(self):  # type: ignore
        """Alias de compatibilidad para UI antigua."""
        return getattr(self, "_sync_thread", None)

    def is_sync_alive(self) -> bool:
        """Estado del hilo de sincronización (seguro contra None)."""
        t = getattr(self, "_sync_thread", None)
        return bool(t and t.is_alive())

    def set_sqlite_path(self, new_path: Path, persist_env: bool = True) -> None:
        """Cambia la ruta del archivo SQLite local y reinicia el worker.

        - Detiene el worker de sincronización
        - Reasigna la ruta y re‑inicializa el esquema
        - Reinicia el worker de sincronización
        - Opcionalmente persiste en .env como LOCAL_SQLITE_PATH
        """
        try:
            was_running = self.is_sync_alive()
            self.stop_sync_worker()

            self.sqlite_path = Path(new_path)
            self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            self._init_sqlite()

            # Actualizar settings en caliente
            try:
                settings.LOCAL_SQLITE_PATH = self.sqlite_path
            except Exception:
                pass

            if persist_env:
                try:
                    update_env_var("LOCAL_SQLITE_PATH", str(self.sqlite_path))
                except Exception as e:
                    logger.warning(f"No se pudo persistir LOCAL_SQLITE_PATH: {e}")

            if was_running:
                self._start_sync_worker()

            logger.info(f"Ruta de SQLite local actualizada: {self.sqlite_path}")
        except Exception as e:
            logger.error(f"Error cambiando ruta SQLite local: {e}")

# Instancia global del sistema dual
_dual_db_instance = None
_dual_db_lock = threading.Lock()


def get_dual_db() -> DualDatabaseSystem:
    """Obtiene la instancia global del sistema dual (singleton)"""
    global _dual_db_instance
    
    with _dual_db_lock:
        if _dual_db_instance is None:
            _dual_db_instance = DualDatabaseSystem()
        
        return _dual_db_instance
