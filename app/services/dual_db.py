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
        
        # Control de limpieza automática por cambio de día
        from datetime import datetime
        self._last_known_date = datetime.now(ZoneInfo(settings.TZ)).strftime('%Y-%m-%d')
        self._last_midnight_check = 0
        self._last_hourly_cleanup = 0  # Limpieza de scans incompletos cada hora
        
        # Inicializar SQLite
        self._init_sqlite()
        
        # 🧹 AUTO-REPARACIÓN AL INICIO: Limpiar pending_scans al arrancar
        self._cleanup_on_startup()
        repaired_pairs, orphan_scans = self._repair_unlinked_pairs()
        if repaired_pairs > 0:
            logger.warning(f"🔧 Inicio: Reparados {repaired_pairs} pares huérfanos previos.")
        if orphan_scans > 0:
            logger.warning(f"⚠️ Inicio: Permanecen {orphan_scans} escaneos sin pareja (se reintentará en sync).")
        
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
            
            # 📦 Tabla de scans sin plan (FALLBACK cuando no hay plan activo)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scans_sin_plan (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_id INTEGER NOT NULL,
                    linea TEXT NOT NULL,
                    nparte TEXT NOT NULL,
                    lote TEXT,
                    cantidad INTEGER DEFAULT 1,
                    fecha TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    scan_format TEXT,
                    aplicado INTEGER DEFAULT 0,
                    aplicado_a_plan_id INTEGER,
                    aplicado_at TEXT,
                    FOREIGN KEY (scan_id) REFERENCES scans_local(id)
                )
            """)
            
            # Migración: Agregar scan_format si no existe
            try:
                conn.execute("SELECT scan_format FROM scans_sin_plan LIMIT 1")
            except sqlite3.OperationalError:
                logger.info("🔄 Migrando tabla scans_sin_plan: agregando columna scan_format")
                conn.execute("ALTER TABLE scans_sin_plan ADD COLUMN scan_format TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scans_sin_plan_linea ON scans_sin_plan(linea, nparte, aplicado)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scans_sin_plan_aplicado ON scans_sin_plan(aplicado)")

            # Tabla de errores de escaneo
            conn.execute("""
            CREATE TABLE IF NOT EXISTS scan_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw TEXT NOT NULL,
                nparte TEXT,
                linea TEXT NOT NULL,
                scan_format TEXT,
                error_code INTEGER NOT NULL,
                error_message TEXT,
                ts TEXT NOT NULL,
                fecha TEXT NOT NULL
            )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_errors_fecha ON scan_errors(fecha)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_errors_linea ON scan_errors(linea)")

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
        """🧹 AUTO-REPARACIÓN AL INICIAR: Limpia pending_scans y scans_sin_plan antiguos"""
        try:
            with self._get_sqlite_connection(timeout=10.0) as conn:
                # 1. Limpiar pending_scans de sesión anterior
                cursor = conn.execute("SELECT COUNT(*) FROM pending_scans")
                count_before = cursor.fetchone()[0]
                
                if count_before > 0:
                    logger.warning(f"⚠️  INICIO: Encontrados {count_before} pending_scans de sesión anterior")
                    conn.execute("DELETE FROM pending_scans")
                    conn.commit()
                    logger.warning(f"🧹 AUTO-REPARACIÓN: Limpiados {count_before} pending_scans al iniciar")
                else:
                    logger.info("✅ pending_scans limpio al iniciar")
                
                # 2. Limpiar scans_sin_plan de días anteriores
                from datetime import datetime
                from zoneinfo import ZoneInfo
                today = datetime.now(ZoneInfo(settings.TZ)).strftime('%Y-%m-%d')
                
                cursor_old = conn.execute("""
                    SELECT COUNT(*) FROM scans_sin_plan 
                    WHERE fecha < ? AND aplicado = 0
                """, (today,))
                old_scans = cursor_old.fetchone()[0]
                
                if old_scans > 0:
                    logger.warning(f"🗑️  INICIO: Encontrados {old_scans} scans_sin_plan de días anteriores")
                    conn.execute("""
                        DELETE FROM scans_sin_plan 
                        WHERE fecha < ? AND aplicado = 0
                    """, (today,))
                    conn.commit()
                    logger.warning(f"🧹 LIMPIEZA AUTOMÁTICA: Eliminados {old_scans} scans antiguos")
                else:
                    logger.info("✅ scans_sin_plan sin registros antiguos")
                    
        except Exception as e:
            logger.error(f"Error en cleanup_on_startup: {e}", exc_info=True)

    def _repair_unlinked_pairs(self) -> tuple[int, int]:
        """
        🔧 Repara escaneos completos que quedaron sin vincular (linked_scan_id NULL).

        Returns:
            tuple[int, int]: (pares reparados, escaneos que aún quedaron sueltos)
        """
        repaired_pairs = 0
        leftovers = 0
        try:
            with self._get_sqlite_connection(timeout=5.0) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT id, scan_format, linea, nparte, lote, ts
                    FROM scans_local
                    WHERE linked_scan_id IS NULL
                      AND scan_format IN ('QR', 'BARCODE')
                      AND synced_to_mysql = 0
                """
                ).fetchall()

                if not rows:
                    return (0, 0)

                from collections import defaultdict

                grouped: dict[tuple[str, str, str], dict[str, list[tuple[str, int]]]] = defaultdict(
                    lambda: {"QR": [], "BARCODE": []}
                )
                for row in rows:
                    key = (
                        (row["linea"] or "").strip(),
                        (row["nparte"] or "").strip(),
                        (row["lote"] or "").strip(),
                    )
                    ts_val = row["ts"] or ""
                    grouped[key][row["scan_format"].upper()].append((ts_val, row["id"]))

                for (linea, nparte, lote), bucket in grouped.items():
                    qr_list = sorted(bucket["QR"])
                    bc_list = sorted(bucket["BARCODE"])
                    pairable = min(len(qr_list), len(bc_list))

                    for idx in range(pairable):
                        qr_ts, qr_id = qr_list[idx]
                        bc_ts, bc_id = bc_list[idx]
                        if qr_id == bc_id:
                            leftovers += 1
                            continue

                        conn.execute(
                            """
                            UPDATE scans_local
                            SET linked_scan_id = ?, is_complete = 1, synced_to_mysql = 0
                            WHERE id = ?
                        """,
                            (bc_id, qr_id),
                        )
                        conn.execute(
                            """
                            UPDATE scans_local
                            SET linked_scan_id = ?, is_complete = 1, synced_to_mysql = 0
                            WHERE id = ?
                        """,
                            (qr_id, bc_id),
                        )
                        conn.execute(
                            """
                            UPDATE scans_sin_plan
                            SET aplicado = 1
                            WHERE scan_id IN (?, ?)
                        """,
                            (qr_id, bc_id),
                        )
                        repaired_pairs += 1

                    leftovers += abs(len(qr_list) - len(bc_list))
                    if pairable:
                        logger.debug(
                            f"🔗 Reparados {pairable} pares en línea={linea or '??'} nparte={nparte or '??'} lote={lote or 'N/A'}"
                        )

                conn.commit()
        except Exception as e:
            logger.error(f"Error reparando pares huérfanos: {e}", exc_info=True)
        return (repaired_pairs, leftovers)

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
                # ¡Tenemos par completo! Pero primero validar que sean del mismo modelo
                
                # ✅ VALIDACIÓN: Verificar que QR y BARCODE sean del mismo nparte
                qr_nparte = qr_scan[7]  # nparte del QR
                barcode_nparte = barcode_scan[7]  # nparte del BARCODE
                
                # En modo normal (no SUB ASSY), QR y BARCODE deben ser del mismo nparte
                from ..config import settings
                is_sub_assy_mode = (getattr(settings, 'SUB_ASSY_MODE', False) and 
                                   getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY')
                
                if not is_sub_assy_mode:
                    # Modo normal: ambos deben ser del mismo nparte
                    if qr_nparte != barcode_nparte:
                        logger.warning(
                            f"🚫 MODELO DIFERENTE EN PAR: QR es '{qr_nparte}' pero BARCODE es '{barcode_nparte}'. "
                            f"Eliminando ambos del buffer."
                        )
                        # Eliminar ambos escaneos del staging (rechazar par)
                        conn.execute("DELETE FROM pending_scans WHERE id IN (?, ?)", 
                                   (qr_scan[0], barcode_scan[0]))
                        return (-10, nparte)  # MODELO DIFERENTE en el par
                
                # ✅ VALIDACIÓN: Verificar que el nparte coincida con el plan EN PROGRESO
                try:
                    cur_plan_check = conn.execute("""
                        SELECT id, part_no FROM plan_local
                        WHERE line = ? AND status = 'EN PROGRESO'
                        ORDER BY sequence LIMIT 1
                    """, (linea,))
                    plan_en_progreso = cur_plan_check.fetchone()
                    
                    if plan_en_progreso:
                        plan_nparte = plan_en_progreso[1]
                        nparte_a_validar = qr_nparte if not is_sub_assy_mode else barcode_nparte
                        
                        if plan_nparte != nparte_a_validar:
                            logger.warning(
                                f"🚫 MODELO DIFERENTE AL PLAN: Plan EN PROGRESO es '{plan_nparte}' "
                                f"pero par escaneado es '{nparte_a_validar}'. Eliminando par del buffer."
                            )
                            # Eliminar ambos escaneos del staging
                            conn.execute("DELETE FROM pending_scans WHERE id IN (?, ?)", 
                                       (qr_scan[0], barcode_scan[0]))
                            return (-10, nparte)  # MODELO DIFERENTE al plan EN PROGRESO
                except Exception as e_plan:
                    logger.error(f"Error verificando plan EN PROGRESO: {e_plan}")
                
                # 🔍 VALIDACIÓN SUB ASSY: Solo en modo ASSY y si está habilitado
                if is_sub_assy_mode:
                    
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
                    
                    # Primero ver QUÉ se va a eliminar y GUARDARLOS en scans_sin_plan
                    to_delete = conn_clean.execute("""
                        SELECT id, nparte, scan_format, ts, lote, cantidad, fecha FROM pending_scans 
                        WHERE ts < ? AND linea = ?
                    """, (thirty_secs_ago, linea)).fetchall()
                    
                    if to_delete:
                        logger.warning(f"📦 SCANS HUÉRFANOS ({len(to_delete)}) - Guardando para aplicar cuando se cargue plan:")
                        for scan in to_delete:
                            scan_id, nparte, scan_format, ts, lote, cantidad, fecha = scan
                            logger.warning(f"   - ID:{scan_id} | Part:{nparte} | Tipo:{scan_format} | Lote:{lote}")
                            
                            # Guardar en scans_sin_plan para recuperar después
                            try:
                                conn_clean.execute("""
                                    INSERT INTO scans_sin_plan 
                                    (scan_id, linea, nparte, lote, cantidad, fecha, ts, scan_format, aplicado)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                                """, (scan_id, linea, nparte, lote, cantidad or 1, fecha, ts, scan_format))
                                logger.info(f"   ✅ Guardado en scans_sin_plan: {nparte} ({scan_format})")
                            except Exception as e_save:
                                logger.error(f"   ❌ Error guardando scan {scan_id}: {e_save}")
                        
                        logger.warning(f"💡 CAUSA: Plan no existe aún - Se aplicarán cuando se cargue el plan")
                    
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
            nparte_escaneado = nparte_plan  # Guardar el nparte escaneado original
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
                    # 📦 PARTE FUERA DE PLAN: Antes de permitir, verificar si hay OTRO plan EN PROGRESO
                    # Si hay un plan EN PROGRESO para OTRO modelo, rechazar con MODELO DIFERENTE
                    try:
                        with self._get_sqlite_connection(timeout=1.0) as conn_check:
                            cur_check = conn_check.execute("""
                                SELECT id, part_no FROM plan_local
                                WHERE line = ? AND status = 'EN PROGRESO'
                                  AND working_date = ?
                                LIMIT 1
                            """, (linea, today))
                            plan_en_progreso = cur_check.fetchone()
                            
                            if plan_en_progreso:
                                plan_activo_nparte = plan_en_progreso[1]
                                logger.warning(
                                    f"🚫 MODELO DIFERENTE: Hay plan EN PROGRESO para '{plan_activo_nparte}', "
                                    f"pero escaneaste '{nparte_escaneado}'. Escaneo rechazado."
                                )
                                return -10  # MODELO DIFERENTE - hay un plan activo para otro modelo
                    except Exception as e:
                        logger.error(f"Error verificando plan EN PROGRESO: {e}")
                    
                    # Si NO hay plan EN PROGRESO, permitir guardar como "FUERA DE PLAN"
                    logger.warning(f"📦 FUERA DE PLAN: No existe plan para {nparte_escaneado}, permitiendo escaneo (se guardará automáticamente)...")
                    # NO retornar -3 aquí, permitir que continúe el proceso normal
                    # El scan irá a pending_scans y luego se moverá a scans_sin_plan
                    skip_plan_validation = True  # Flag para skipear validaciones posteriores
                    plan_id = None  # Asegurar que no hay plan_id
                    nparte_plan = None  # IMPORTANTE: Limpiar para activar fallback después

                if plan_id and plan_count is not None and produced_count is not None and produced_count >= plan_count:
                    return -4  # Plan completo alcanzado

                # 🔒 VALIDACIÓN Y AUTO-INICIO: Gestionar estados de planes (SOLO si hay plan)
                if plan_id:
                    current_status = status_val.strip().upper()
                    
                    if current_status == 'EN PROGRESO':
                        # El plan ya está activo, VALIDAR que el nparte escaneado coincida con el plan
                        if nparte_plan and nparte_escaneado and nparte_plan != nparte_escaneado:
                            logger.warning(
                                f"🚫 MODELO DIFERENTE: Plan EN PROGRESO es para '{nparte_plan}', "
                                f"pero escaneaste '{nparte_escaneado}'. Escaneo rechazado."
                            )
                            return -10  # MODELO DIFERENTE - no permitir escaneo de otro modelo
                        # Si coincide, permitir escaneo
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
                # � VALIDACIÓN RÁPIDA: Verificar si ya hay un scan del MISMO tipo esperando
                check_same = conn.execute("""
                    SELECT COUNT(*) FROM pending_scans
                    WHERE nparte = ? AND linea = ? AND scan_format = ?
                """, (scan_record.nparte, linea, scan_format)).fetchone()
                
                if check_same and check_same[0] > 0:
                    # Ya hay un scan del mismo tipo esperando - ERROR
                    logger.warning(f"🚫 TARJETA DUPLICADA: Ya hay un {scan_format}")
                    if scan_format == "QR":
                        return -8  # QR DUPLICADO - necesita escanear BARCODE
                    else:
                        return -9  # BARCODE DUPLICADO - necesita escanear QR

                # �� PASO 1: Guardar en STAGING (temporal) - NO en tabla final
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
                
                # Obtener lote del scan_record para asegurar que incrementamos el plan correcto
                lote_escaneado = getattr(scan_record, 'lot_no', None) or getattr(scan_record, 'lote', None)
                
                if nparte_plan:
                    try:
                        logger.info(f"⚡ Incrementando produced_count: linea={linea}, nparte={final_nparte}, lote={lote_escaneado}")
                        self.increment_local_plan_produced(linea, final_nparte, delta=1, lote=lote_escaneado)
                    except Exception as e_inc:
                        logger.error(f"No se pudo incrementar produced_count local: {e_inc}")
                else:
                    # 📦 FALLBACK: Guardar en scans_sin_plan para aplicar cuando se cargue el plan
                    try:
                        with self._get_sqlite_connection(timeout=2.0) as conn_fallback:
                            # Buscar el scan_id más reciente para este nparte
                            scan_row = conn_fallback.execute("""
                                SELECT id FROM scans_local 
                                WHERE nparte = ? AND linea = ?
                                ORDER BY id DESC LIMIT 1
                            """, (final_nparte, linea)).fetchone()
                            
                            if scan_row:
                                scan_id = scan_row[0]
                                conn_fallback.execute("""
                                    INSERT INTO scans_sin_plan 
                                    (scan_id, linea, nparte, lote, cantidad, fecha, ts, scan_format, aplicado)
                                    VALUES (?, ?, ?, ?, 1, ?, ?, 'PAIR', 0)
                                """, (
                                    scan_id,
                                    linea,
                                    final_nparte,
                                    lote_escaneado,
                                    get_today_mexico_str(),
                                    datetime.now(ZoneInfo(settings.TZ)).isoformat()
                                ))
                                conn_fallback.commit()
                                logger.warning(f"📦 NO EN PLAN: Scan guardado para aplicar cuando se cargue plan (linea={linea}, nparte={final_nparte})")
                            else:
                                logger.error(f"No se encontró scan_id para guardar en fallback")
                    except Exception as e_fallback:
                        logger.error(f"Error guardando scan sin plan: {e_fallback}")
                
                logger.info(f"✅ PAR COMPLETO: QR+Barcode insertados en DB FINAL ({final_nparte})")
                return 999999  # Código especial para PAR COMPLETO (distinguible de staging_id)
            elif pair_result == -7:
                # SUB ASSY validation failed - par rechazado
                return -7
            elif pair_result == -10:
                # MODELO DIFERENTE - par rechazado
                logger.warning(f"🚫 MODELO DIFERENTE: Par rechazado (QR y BARCODE no coinciden con plan EN PROGRESO)")
                return -10
            elif pair_result == 0:
                # Par incompleto
                opposite = "BARCODE" if scan_format == "QR" else "QR"
                logger.info(f"⏳ {scan_format} en STAGING, esperando {opposite} para insertar en DB ({scan_record.nparte})")
                return -5  # Código -5: Esperando par (para que UI lo detecte)
            else:
                # Otro código de error
                logger.warning(f"⚠️ Error inesperado al completar par: código {pair_result}")
                return pair_result
            
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
        Returns True si se completó la vinculación (ambos escaneos presentes)
        Returns -8 si detecta QR+QR duplicado
        Returns -9 si detecta BARCODE+BARCODE duplicado"""
        try:
            # 🚨 VALIDACIÓN: Verificar si hay un scan del MISMO tipo sin vincular
            cursor_same = conn.execute("""
                SELECT id FROM scans_local 
                WHERE nparte = ? AND scan_format = ? 
                AND linked_scan_id IS NULL
                AND id != ?
                ORDER BY id DESC LIMIT 1
            """, (nparte, scan_format, new_scan_id))
            
            same_type_scan = cursor_same.fetchone()
            if same_type_scan:
                # Hay un scan del mismo tipo esperando - ERROR
                logger.warning(f"🚫 ERROR: Detectado {scan_format}+{scan_format} para {nparte}. Escanea el complemento correcto.")
                # Eliminar el scan recién agregado (está incorrecto)
                conn.execute("DELETE FROM scans_local WHERE id = ?", (new_scan_id,))
                conn.commit()
                # Retornar código específico según el tipo
                if scan_format == "QR":
                    return -8  # QR DUPLICADO - necesita escanear BARCODE
                else:
                    return -9  # BARCODE DUPLICADO - necesita escanear QR
            
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

                # 🗑️ LIMPIEZA DE MEDIANOCHE: Borrar scans_sin_plan antiguos automáticamente
                # Verificar cada 10 minutos si cambió el día
                last_midnight_check = getattr(self, '_last_midnight_check', 0)
                if now - last_midnight_check > 600:  # 10 minutos = 600 segundos
                    try:
                        from datetime import datetime as dt
                        current_date = dt.now(ZoneInfo(settings.TZ)).strftime('%Y-%m-%d')
                        last_date = getattr(self, '_last_known_date', current_date)
                        
                        # Si cambió el día, limpiar scans_sin_plan del día anterior
                        if current_date != last_date:
                            logger.info(f"📅 CAMBIO DE DÍA DETECTADO: {last_date} -> {current_date}")
                            with self._get_sqlite_connection(timeout=5.0) as conn:
                                deleted = conn.execute("""
                                    DELETE FROM scans_sin_plan 
                                    WHERE fecha < ? AND aplicado = 0
                                """, (current_date,))
                                
                                count = deleted.rowcount
                                if count > 0:
                                    logger.warning(f"🗑️ LIMPIEZA AUTOMÁTICA (medianoche): Eliminados {count} scans_sin_plan del día anterior")
                                conn.commit()
                            
                            self._last_known_date = current_date
                        
                        self._last_midnight_check = now
                    except Exception as e_midnight:
                        logger.debug(f"Error en limpieza de medianoche: {e_midnight}")

                # 🧹 LIMPIEZA HORARIA: Borrar scans_sin_plan incompletos (sin par) después de 1 hora
                last_hourly_cleanup = getattr(self, '_last_hourly_cleanup', 0)
                if now - last_hourly_cleanup > 3600:  # 1 hora = 3600 segundos
                    try:
                        from datetime import datetime as dt, timedelta
                        one_hour_ago = (dt.now(ZoneInfo(settings.TZ)) - timedelta(hours=1)).isoformat()
                        
                        with self._get_sqlite_connection(timeout=5.0) as conn:
                            # Buscar scans que tienen más de 1 hora y no forman pares
                            # Agrupar por nparte para contar QR y BC
                            cursor = conn.execute("""
                                SELECT nparte, scan_format, COUNT(*) as total
                                FROM scans_sin_plan
                                WHERE ts < ? AND aplicado = 0
                                GROUP BY nparte, scan_format
                            """, (one_hour_ago,))
                            
                            old_scans = cursor.fetchall()
                            
                            if old_scans:
                                # Analizar cuáles están incompletos
                                grupos = {}
                                for nparte, scan_format, count in old_scans:
                                    if nparte not in grupos:
                                        grupos[nparte] = {'QR': 0, 'BC': 0, 'PAIR': 0}
                                    if scan_format == 'PAIR':
                                        grupos[nparte]['PAIR'] += count
                                    elif scan_format in ('QR', 'BARCODE'):
                                        grupos[nparte][scan_format if scan_format == 'QR' else 'BC'] += count
                                
                                # Eliminar solo los que NO forman pares (QR sin BC o BC sin QR)
                                total_deleted = 0
                                for nparte, stats in grupos.items():
                                    qr = stats['QR']
                                    bc = stats['BC']
                                    
                                    # Si hay desbalance (QR sin BC o BC sin QR), eliminar los huérfanos
                                    if qr > bc:
                                        # Eliminar QRs sobrantes
                                        deleted = conn.execute("""
                                            DELETE FROM scans_sin_plan
                                            WHERE nparte = ? AND scan_format = 'QR' AND ts < ? AND aplicado = 0
                                        """, (nparte, one_hour_ago))
                                        total_deleted += deleted.rowcount
                                    elif bc > qr:
                                        # Eliminar BARCODEs sobrantes
                                        deleted = conn.execute("""
                                            DELETE FROM scans_sin_plan
                                            WHERE nparte = ? AND scan_format = 'BARCODE' AND ts < ? AND aplicado = 0
                                        """, (nparte, one_hour_ago))
                                        total_deleted += deleted.rowcount
                                
                                if total_deleted > 0:
                                    logger.warning(f"🗑️ LIMPIEZA HORARIA: Eliminados {total_deleted} scans incompletos (>1 hora sin formar par)")
                                    conn.commit()
                        
                        self._last_hourly_cleanup = now
                    except Exception as e_hourly:
                        logger.debug(f"Error en limpieza horaria: {e_hourly}")

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

    def insert_error_to_mysql(self, raw: str, nparte: str, linea: str, scan_format: str, 
                              error_code: int, error_message: str, ts: str) -> bool:
        """
        Inserta un error de escaneo directamente a MySQL en la tabla input_main
        
        Args:
            raw: Código escaneado completo
            nparte: Número de parte
            linea: Línea de producción
            scan_format: 'QR' o 'BARCODE'
            error_code: Código de error (-8, -9, -10, etc.)
            error_message: Mensaje descriptivo del error
            ts: Timestamp ISO format
            
        Returns:
            bool: True si se insertó correctamente
        """
        try:
            from ..db import get_db
            from datetime import datetime
            db = get_db()
            
            # Convertir timestamp ISO a datetime y date
            dt = datetime.fromisoformat(ts)
            fecha = dt.strftime("%Y-%m-%d")
            
            with db.get_connection() as mysql_conn:
                with mysql_conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO input_main 
                        (raw, nparte, linea, ts, fecha, scan_format, result, error_code, error_message)
                        VALUES (%s, %s, %s, %s, %s, %s, 'NG', %s, %s)
                    """, (raw, nparte, linea, ts, fecha, scan_format, error_code, error_message))
                    mysql_conn.commit()
                    
            logger.debug(f"Error NG enviado a MySQL: {scan_format} - {error_message}")
            return True
            
        except Exception as e:
            logger.error(f"Error insertando NG a MySQL: {e}")
            return False

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
                            planned_start_raw = r.get('planned_start')
                            planned_end_raw = r.get('planned_end')
                            
                            # 📅 CONSTRUIR FECHA COMPLETA: working_date + hora de planned_start/end
                            # MySQL solo guarda hora, necesitamos combinar con working_date
                            if planned_start_raw:
                                # Si planned_start es datetime, extraer solo la hora
                                if not isinstance(planned_start_raw, str):
                                    planned_start_time = planned_start_raw.strftime('%H:%M:%S')
                                else:
                                    # Si es string, puede ser 'HH:MM:SS' o 'YYYY-MM-DD HH:MM:SS'
                                    if ' ' in planned_start_raw:
                                        planned_start_time = planned_start_raw.split(' ')[1]
                                    else:
                                        planned_start_time = planned_start_raw
                                
                                # Combinar working_date (fecha) + hora
                                planned_start = f"{working_date_str} {planned_start_time}"
                            else:
                                planned_start = None
                            
                            if planned_end_raw:
                                # Si planned_end es datetime, extraer solo la hora
                                if not isinstance(planned_end_raw, str):
                                    planned_end_time = planned_end_raw.strftime('%H:%M:%S')
                                else:
                                    # Si es string, puede ser 'HH:MM:SS' o 'YYYY-MM-DD HH:MM:SS'
                                    if ' ' in planned_end_raw:
                                        planned_end_time = planned_end_raw.split(' ')[1]
                                    else:
                                        planned_end_time = planned_end_raw
                                
                                # Combinar working_date (fecha) + hora
                                planned_end = f"{working_date_str} {planned_end_time}"
                            else:
                                planned_end = None
                            
                            effective_minutes = r.get('effective_minutes', 0) or 0
                            
                            # PROTECCIÓN CONTRA SOBRESCRITURA: Sumar incrementos pendientes del buffer
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
                    # INCLUIR: planned_start, planned_end, effective_minutes, sequence para detectar TODOS los cambios
                    cursor = conn.execute("""
                        SELECT 
                            COUNT(*) as total, 
                            SUM(plan_count) as sum_plan,
                            SUM(CAST(COALESCE(effective_minutes, 0) AS INTEGER)) as sum_minutes,
                            GROUP_CONCAT(COALESCE(planned_start, 'NULL') || '|' || COALESCE(planned_end, 'NULL') || '|' || COALESCE(sequence, 0), ';') as schedule_hash
                        FROM plan_local 
                        WHERE working_date = ?
                    """, (today.isoformat(),))
                    current_state = cursor.fetchone()
                    
                    # Hash más completo que detecta cambios en horarios y secuencia
                    current_hash = f"{current_state[0]}_{current_state[1]}_{current_state[2]}_{hash(current_state[3]) if current_state[3] else 0}"
                    
                    # Si cambió desde la última sincronización, marcar para recargar UI
                    if not hasattr(self, '_last_plan_hash') or self._last_plan_hash != current_hash:
                        logger.warning(f"📊 CAMBIOS DETECTADOS EN PLAN: {getattr(self, '_last_plan_hash', 'N/A')} → {current_hash}")
                        self._plan_changed = True  # Flag para que la UI detecte el cambio
                        self._last_plan_hash = current_hash
                    else:
                        self._plan_changed = False
                    
                    conn.commit()
                
                # 📦 APLICAR SCANS PENDIENTES: Si hay scans sin plan, aplicarlos ahora
                self._aplicar_scans_pendientes()
                
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
    
    
    def sync_before_shutdown(self) -> dict:
        """
         SINCRONIZACIÓN FINAL antes de cerrar el programa
        
        Verifica y sincroniza todos los datos pendientes con MySQL:
        - Incrementos de producción pendientes
        - Scans en SQLite que no estén en MySQL
        - Totales de producción desincronizados
        
        Returns:
            dict: Resumen de la sincronización con contadores
        """
        logger.info(" INICIANDO SINCRONIZACIÓN FINAL ANTES DE CERRAR...")
        result = {
            'scans_synced': 0,
            'increments_synced': 0,
            'production_synced': 0,
            'errors': []
        }
        
        try:
            self._aplicar_scans_pendientes()
        except Exception as pend_err:
            logger.error(f"Error aplicando scans pendientes antes de cerrar: {pend_err}")

        try:
            repaired_pairs, leftovers = self._repair_unlinked_pairs()
            if repaired_pairs > 0:
                logger.info(f"🔧 Reparados {repaired_pairs} pares huérfanos antes de sincronizar.")
            if leftovers > 0:
                logger.warning(f"⚠️ Permanecen {leftovers} escaneos sin pareja al cerrar.")
        except Exception as repair_err:
            logger.error(f"Error reparando pares huérfanos antes de cerrar: {repair_err}")

        try:
            for cycle in range(5):
                synced_pairs = self._sync_scans_to_mysql()
                if synced_pairs <= 0:
                    break
                result['scans_synced'] += synced_pairs
                logger.info(f"🔁 Flush previo al cierre (ciclo {cycle + 1}): {synced_pairs} pares enviados.")
        except Exception as flush_err:
            logger.error(f"Error forzando sync de scans antes de cerrar: {flush_err}")
        
        # Aplicar y reparar antes de sincronizar
        try:
            self._aplicar_scans_pendientes()
        except Exception as pend_err:
            logger.error(f"Error aplicando scans pendientes antes de cerrar: {pend_err}")

        try:
            repaired_pairs, leftovers = self._repair_unlinked_pairs()
            if repaired_pairs > 0:
                logger.info(f"🔧 Reparados {repaired_pairs} pares huérfanos antes de sincronizar.")
            if leftovers > 0:
                logger.warning(f"⚠️ Permanecen {leftovers} escaneos sin pareja al cerrar.")
        except Exception as repair_err:
            logger.error(f"Error reparando pares huérfanos antes de cerrar: {repair_err}")

        try:
            for cycle in range(5):
                synced_pairs = self._sync_scans_to_mysql()
                if synced_pairs <= 0:
                    break
                result['scans_synced'] += synced_pairs
                logger.info(f"🔁 Flush previo al cierre (ciclo {cycle + 1}): {synced_pairs} pares enviados.")
        except Exception as flush_err:
            logger.error(f"Error forzando sync de scans antes de cerrar: {flush_err}")

        # Aplicar y reparar antes de sincronizar
        try:
            self._aplicar_scans_pendientes()
        except Exception as pend_err:
            logger.error(f"Error aplicando scans pendientes antes de cerrar: {pend_err}")

        try:
            repaired_pairs, leftovers = self._repair_unlinked_pairs()
            if repaired_pairs > 0:
                logger.info(f"🔧 Reparados {repaired_pairs} pares huérfanos antes de sincronizar.")
            if leftovers > 0:
                logger.warning(f"⚠️ Permanecen {leftovers} escaneos sin pareja al cerrar.")
        except Exception as repair_err:
            logger.error(f"Error reparando pares huérfanos antes de cerrar: {repair_err}")
        
        try:
            # 1. Sincronizar incrementos pendientes en direct_mysql
            try:
                if hasattr(self, '_direct_mysql') and self._direct_mysql:
                    logger.info(" Sincronizando incrementos pendientes...")
                    self._direct_mysql.sync_pending_increments()
                    result['increments_synced'] = len(getattr(self._direct_mysql, '_sync_queue', []))
                    logger.info(f" {result['increments_synced']} incrementos sincronizados")
            except Exception as e:
                error_msg = f"Error sincronizando incrementos: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
            
            # 2. Verificar scans en SQLite que no estén en MySQL
            try:
                logger.info(" Verificando scans pendientes de sincronizar...")
                with self._get_sqlite_connection(timeout=5.0) as conn:
                    # Obtener fecha actual
                    today = datetime.now(ZoneInfo("America/Monterrey")).strftime("%Y-%m-%d")
                    
                    # Contar scans locales del día
                    local_cursor = conn.execute("""
                        SELECT COUNT(*) FROM scans_local 
                        WHERE DATE(ts) = ?
                    """, (today,))
                    local_count = local_cursor.fetchone()[0]
                    
                    logger.info(f" Scans locales hoy: {local_count}")
                    
                    # Verificar en MySQL
                    from ..db import get_db
                    db = get_db()
                    with db.get_connection() as mysql_conn:
                        with mysql_conn.cursor() as mysql_cursor:
                            mysql_cursor.execute("""
                                SELECT COUNT(*) FROM input_main
                                WHERE DATE(ts) = %s
                            """, (today,))
                            mysql_count = mysql_cursor.fetchone()[0]

                            logger.info(f" Scans en MySQL hoy: {mysql_count}")                            # Si hay diferencia, sincronizar los faltantes
                            if local_count > mysql_count:
                                diff = local_count - mysql_count
                                logger.warning(f"  Faltan {diff} scans en MySQL. Sincronizando...")
                                
                                # Obtener scans locales con fecha incluida
                                local_scans = conn.execute("""
                                    SELECT id, raw, nparte, linea, ts, fecha, modelo, scan_format, 
                                           plan_id, barcode_sequence
                                    FROM scans_local
                                    WHERE DATE(ts) = ?
                                    ORDER BY ts DESC
                                    LIMIT ?
                                """, (today, diff)).fetchall()

                                # Insertar en MySQL
                                for scan in local_scans:
                                    try:
                                        mysql_cursor.execute("""
                                            INSERT INTO input_main
                                            (raw, nparte, linea, ts, fecha, modelo, scan_format, barcode_sequence, result)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'OK')
                                            ON DUPLICATE KEY UPDATE id=id
                                        """, (scan[1], scan[2], scan[3], scan[4], scan[5], scan[6], scan[7], scan[9]))
                                        result['scans_synced'] += 1
                                    except Exception as e:
                                        logger.debug(f"Scan ya existe en MySQL: {scan[1]}")
                                
                                mysql_conn.commit()
                                logger.info(f" {result['scans_synced']} scans sincronizados a MySQL")
                            else:
                                logger.info(" Todos los scans están sincronizados")
                    
            except Exception as e:
                error_msg = f"Error sincronizando scans: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
            
            # 3. Sincronizar totales de producción
            try:
                logger.info(" Verificando totales de producción...")
                with self._get_sqlite_connection(timeout=5.0) as conn:
                    today = datetime.now(ZoneInfo("America/Monterrey")).strftime("%Y-%m-%d")
                    
                    # Obtener totales locales
                    local_totals = conn.execute("""
                        SELECT plan_id, produced_count 
                        FROM production_totals_local 
                        WHERE fecha = ?
                    """, (today,)).fetchall()
                    
                    if local_totals:
                        from ..db import get_db
                        db = get_db()
                        with db.get_connection() as mysql_conn:
                            with mysql_conn.cursor() as mysql_cursor:
                                for plan_id, local_produced in local_totals:
                                    # Verificar en MySQL
                                    mysql_cursor.execute("""
                                        SELECT produced_count FROM plan_main 
                                        WHERE id = %s
                                    """, (plan_id,))
                                    mysql_result = mysql_cursor.fetchone()
                                    
                                    if mysql_result:
                                        mysql_produced = mysql_result[0] or 0
                                        if local_produced > mysql_produced:
                                            diff = local_produced - mysql_produced
                                            logger.warning(f"  Plan {plan_id}: Local={local_produced}, MySQL={mysql_produced} (diff={diff})")
                                            
                                            # Actualizar MySQL
                                            mysql_cursor.execute("""
                                                UPDATE plan_main 
                                                SET produced_count = %s, updated_at = NOW()
                                                WHERE id = %s
                                            """, (local_produced, plan_id))
                                            result['production_synced'] += 1
                                
                                mysql_conn.commit()
                                logger.info(f" {result['production_synced']} totales de producción sincronizados")
                    else:
                        logger.info(" No hay totales pendientes de sincronizar")
                        
            except Exception as e:
                error_msg = f"Error sincronizando producción: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
            
            # Resumen final
            total_synced = result['scans_synced'] + result['increments_synced'] + result['production_synced']
            if total_synced > 0:
                logger.info(f" SINCRONIZACIÓN COMPLETADA: {total_synced} registros sincronizados")
                logger.info(f"   - Scans: {result['scans_synced']}")
                logger.info(f"   - Incrementos: {result['increments_synced']}")
                logger.info(f"   - Producción: {result['production_synced']}")
            else:
                logger.info(" SINCRONIZACIÓN COMPLETADA: Todo estaba sincronizado")
            
            if result['errors']:
                logger.warning(f"  Se encontraron {len(result['errors'])} errores durante la sincronización")
            
            return result
            
        except Exception as e:
            logger.error(f" Error en sincronización final: {e}")
            result['errors'].append(str(e))
            return result
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
    
    def _aplicar_scans_pendientes(self):
        """
        📦 FALLBACK: Aplica scans guardados sin plan cuando se carga un nuevo plan.
        
        Esta función se ejecuta automáticamente después de sincronizar planes desde MySQL.
        Si hay scans que fueron escaneados cuando NO había plan activo, los aplica ahora.
        
        IMPORTANTE: Solo cuenta PARES COMPLETOS (QR + BARCODE), no scans individuales.
        """
        try:
            # Abrir conexión propia para evitar problemas con conexiones cerradas
            with self._get_sqlite_connection(timeout=2.0) as conn:
                # Obtener todos los scans pendientes agrupados por nparte/lote/formato
                # Necesitamos contar QR y BARCODE por separado para calcular pares
                pendientes = conn.execute("""
                    SELECT 
                        linea,
                        nparte,
                        lote,
                        scan_format,
                        COUNT(*) as scan_count,
                        GROUP_CONCAT(id) as fallback_ids
                    FROM scans_sin_plan
                    WHERE aplicado = 0
                    GROUP BY linea, nparte, lote, scan_format
                    ORDER BY ts ASC
                """).fetchall()
                
                if not pendientes:
                    logger.debug("📦 No hay scans pendientes para aplicar")
                    return  # No hay nada pendiente
                
                logger.warning(f"📦 APLICANDO SCANS PENDIENTES: Analizando {len(pendientes)} grupos de scans...")
                
                # DEBUG: Mostrar todos los planes disponibles
                planes_disponibles = conn.execute("""
                    SELECT id, line, part_no, lot_no, status, produced_count
                    FROM plan_local
                    WHERE status IN ('PAUSADO', 'EN PROGRESO')
                    ORDER BY line, status, sequence
                """).fetchall()
                
                logger.debug(f"📋 Planes disponibles para aplicar scans: {len(planes_disponibles)}")
                for pl in planes_disponibles[:10]:  # Mostrar solo primeros 10
                    logger.debug(f"   • Plan {pl[0]}: {pl[1]} | {pl[2]} | Lote:{pl[3]} | {pl[4]} | Producido:{pl[5]}")
                
                # Agrupar por (linea, nparte, lote) para contar pares
                from collections import defaultdict
                grupos = defaultdict(lambda: {'QR': 0, 'BARCODE': 0, 'ids': []})
                
                for row in pendientes:
                    linea, nparte, lote, scan_format, count, ids_str = row
                    key = (linea, nparte, lote)
                    
                    if scan_format == 'PAIR':
                        # PAIR = pieza completa (QR+BARCODE ya vinculados)
                        grupos[key]['PAIR'] = grupos[key].get('PAIR', 0) + count
                    elif scan_format in ('QR', 'BARCODE'):
                        # Scans individuales que necesitan su complemento
                        grupos[key][scan_format] = count
                    else:
                        # Si no tiene scan_format o es desconocido, lo contamos genérico
                        grupos[key]['GENERIC'] = grupos[key].get('GENERIC', 0) + count
                    
                    # Guardar IDs para marcar como aplicados
                    if ids_str:
                        grupos[key]['ids'].extend(ids_str.split(','))
                
                aplicados_total = 0
                incompletos_summary = {}  # Para consolidar mensajes de scans incompletos
                
                for (linea, nparte, lote), info in grupos.items():
                    qr_count = info.get('QR', 0)
                    bc_count = info.get('BARCODE', 0)
                    pair_count = info.get('PAIR', 0)  # Piezas ya completas
                    generic_count = info.get('GENERIC', 0)
                    
                    # Calcular pares completos:
                    # 1. PAIR = piezas ya completas (QR+BC vinculados cuando no había plan)
                    # 2. min(QR, BC) = pares que se pueden formar con scans sueltos
                    # 3. GENERIC = scans sin formato (legacy)
                    pares_completos = pair_count + min(qr_count, bc_count) + generic_count
                    
                    if pares_completos == 0:
                        # Consolidar por nparte (sumar QR y BC de todos los lotes)
                        if nparte not in incompletos_summary:
                            incompletos_summary[nparte] = {'QR': 0, 'BC': 0, 'PAIR': 0, 'lotes': 0}
                        incompletos_summary[nparte]['QR'] += qr_count
                        incompletos_summary[nparte]['BC'] += bc_count
                        incompletos_summary[nparte]['PAIR'] += pair_count
                        incompletos_summary[nparte]['lotes'] += 1
                        continue
                    
                    try:
                        # Buscar si ahora existe un plan para este nparte
                        # PRIORIDAD:
                        # 1. Plan EN PROGRESO con lote exacto
                        # 2. Plan EN PROGRESO sin importar lote
                        # 3. Plan PAUSADO con lote exacto
                        # 4. Plan PAUSADO sin importar lote
                        plan_row = conn.execute("""
                            SELECT id, produced_count, lot_no, status
                            FROM plan_local
                            WHERE line = ? AND part_no = ?
                            AND status IN ('PAUSADO', 'EN PROGRESO')
                            ORDER BY 
                                CASE WHEN status = 'EN PROGRESO' THEN 0 ELSE 1 END,
                                CASE WHEN lot_no = ? THEN 0 ELSE 1 END,
                                sequence ASC
                            LIMIT 1
                        """, (linea, nparte, lote)).fetchone()
                        
                        if plan_row:
                            plan_id, produced_count, plan_lot, plan_status = plan_row
                            logger.debug(f"✅ Encontrado plan {plan_id} para {nparte} (Lote scan:{lote}, Lote plan:{plan_lot}, Estado:{plan_status})")
                        else:
                            logger.debug(f"❌ NO encontrado plan para {linea} | {nparte} | Lote:{lote}")
                            # DEBUG: Buscar si hay plan con otro estado
                            any_plan = conn.execute("""
                                SELECT id, status, lot_no
                                FROM plan_local
                                WHERE line = ? AND part_no = ?
                                LIMIT 1
                            """, (linea, nparte)).fetchone()
                            if any_plan:
                                logger.debug(f"   ⚠️ Existe plan {any_plan[0]} pero estado es '{any_plan[1]}' (lote: {any_plan[2]})")
                        
                        if plan_row:
                            plan_id, produced_count, plan_lot, plan_status = plan_row
                            
                            qr_candidates = []
                            barcode_candidates = []
                            missing_scans = 0
                            
                            for fallback_id in info['ids']:
                                scan_info = conn.execute("""
                                    SELECT scan_id FROM scans_sin_plan WHERE id = ?
                                """, (int(fallback_id),)).fetchone()
                                
                                if not scan_info:
                                    logger.warning(f"⚠️  scans_sin_plan sin referencia (id={fallback_id})")
                                    continue
                                
                                scan_id = scan_info[0]
                                if scan_id is None:
                                    logger.warning(f"⚠️  scans_sin_plan {fallback_id} no tiene scan_id asociado")
                                    continue
                                
                                scan_row = conn.execute("""
                                    SELECT id, ts, scan_format, linked_scan_id
                                    FROM scans_local
                                    WHERE id = ?
                                """, (scan_id,)).fetchone()
                                
                                if not scan_row:
                                    missing_scans += 1
                                    logger.warning(f"⚠️  No se encontró scan_local id={scan_id} (fallback {fallback_id})")
                                    continue
                                
                                ts_val = scan_row[1] or ""
                                scan_format_local = (scan_row[2] or "").upper()
                                
                                conn.execute("""
                                    UPDATE scans_local 
                                    SET is_complete = 1,
                                        synced_to_mysql = 0
                                    WHERE id = ?
                                """, (scan_id,))
                                logger.debug(f"✅ Scan {scan_id} marcado como completo y pendiente de sync")
                                
                                if scan_format_local == "QR":
                                    qr_candidates.append((ts_val, scan_id))
                                elif scan_format_local == "BARCODE":
                                    barcode_candidates.append((ts_val, scan_id))
                                else:
                                    logger.debug(f"ℹ️  Scan {scan_id} con formato '{scan_format_local}' fuera de emparejamiento automático")
                            
                            qr_candidates.sort(key=lambda item: (item[0], item[1]))
                            barcode_candidates.sort(key=lambda item: (item[0], item[1]))
                            
                            linked_pairs = 0
                            for qr_item, bc_item in zip(qr_candidates, barcode_candidates):
                                qr_id = qr_item[1]
                                bc_id = bc_item[1]
                                
                                conn.execute("""
                                    UPDATE scans_local 
                                    SET linked_scan_id = ?, is_complete = 1, synced_to_mysql = 0
                                    WHERE id = ?
                                """, (bc_id, qr_id))
                                conn.execute("""
                                    UPDATE scans_local 
                                    SET linked_scan_id = ?, is_complete = 1, synced_to_mysql = 0
                                    WHERE id = ?
                                """, (qr_id, bc_id))
                                linked_pairs += 1
                                logger.debug(f"🔗 Emparejado QR {qr_id} ↔ BARCODE {bc_id}")
                            
                            leftover_qr = len(qr_candidates) - linked_pairs
                            leftover_bc = len(barcode_candidates) - linked_pairs
                            if leftover_qr or leftover_bc:
                                logger.warning(f"⚠️  Scans sin pareja tras aplicar plan {plan_id} (QR:{leftover_qr}, BARCODE:{leftover_bc})")
                            
                            real_pares_completos = pair_count + linked_pairs + generic_count
                            if real_pares_completos != pares_completos:
                                logger.debug(f"ℹ️  Ajuste de pares completos ({pares_completos} → {real_pares_completos}) antes de actualizar plan")
                            pares_completos = real_pares_completos
                            
                            nuevo_produced = produced_count + pares_completos
                            conn.execute("""
                                UPDATE plan_local
                                SET produced_count = ?,
                                    updated_at = ?
                                WHERE id = ?
                            """, (nuevo_produced, datetime.now(ZoneInfo(settings.TZ)).isoformat(), plan_id))
                            
                            for fallback_id in info['ids']:
                                conn.execute("""
                                    UPDATE scans_sin_plan
                                    SET aplicado = 1,
                                        aplicado_a_plan_id = ?,
                                        aplicado_at = ?
                                    WHERE id = ?
                                """, (plan_id, datetime.now(ZoneInfo(settings.TZ)).isoformat(), int(fallback_id)))
                            
                            aplicados_total += pares_completos
                            logger.info(f"✅ Scans pendientes aplicados: Plan {plan_id} ({nparte}) +{pares_completos} piezas (QR:{qr_count}, BC:{bc_count}, PAIR:{pair_count}) → {nuevo_produced}")
                            
                            self._plan_produced_buffer[plan_id] = self._plan_produced_buffer.get(plan_id, 0) + pares_completos
                            if missing_scans:
                                logger.warning(f"⚠️  {missing_scans} registros de scans_sin_plan no encontraron scan_local asociado")
                            
                            # 🔔 NOTIFICAR AL UI: Marcar que el plan cambió para forzar actualización inmediata
                            self._plan_changed = True
                        else:
                            logger.debug(f"⏳ Sin plan activo para {nparte} (lote: {lote})")
                    
                    except Exception as e:
                        logger.error(f"Error aplicando scans pendientes para {nparte}: {e}")
                        continue
                
                # Log consolidado de scans incompletos (UNA SOLA VEZ por nparte)
                if incompletos_summary:
                    logger.debug("⏳ Resumen de scans incompletos (esperando complemento):")
                    for nparte, stats in incompletos_summary.items():
                        logger.debug(f"   • {nparte}: QR={stats['QR']}, BC={stats['BC']}, PAIR={stats['PAIR']} ({stats['lotes']} lotes diferentes)")
                
                if aplicados_total > 0:
                    logger.warning(f"🎉 SCANS RECUPERADOS: {aplicados_total} piezas aplicadas a planes activos")
                    # 🔔 NOTIFICAR AL UI: Marcar que el plan cambió para forzar actualización inmediata
                    self._plan_changed = True
                else:
                    logger.debug(f"⏳ Scans aún esperando plan compatible")
                
                conn.commit()
        
        except Exception as e:
            logger.error(f"Error en _aplicar_scans_pendientes: {e}")
    
    def get_scans_sin_plan_count(self, linea: str = None) -> int:
        """
        Obtiene el número de scans pendientes de aplicar (sin plan activo).
        
        Args:
            linea: Línea específica (opcional). Si no se proporciona, cuenta todos.
        
        Returns:
            Número de scans pendientes
        """
        try:
            with self._get_sqlite_connection(timeout=1.0) as conn:
                if linea:
                    cursor = conn.execute("""
                        SELECT COUNT(*) FROM scans_sin_plan
                        WHERE aplicado = 0 AND linea = ?
                    """, (linea,))
                else:
                    cursor = conn.execute("""
                        SELECT COUNT(*) FROM scans_sin_plan
                        WHERE aplicado = 0
                    """)
                
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error obteniendo scans sin plan: {e}")
            return 0


    def sync_before_shutdown(self) -> dict:
        """
         SINCRONIZACIÓN FINAL antes de cerrar el programa
        
        Verifica y sincroniza todos los datos pendientes con MySQL:
        - Incrementos de producción pendientes
        - Scans en SQLite que no estén en MySQL
        - Totales de producción desincronizados
        
        Returns:
            dict: Resumen de la sincronización con contadores
        """
        logger.info(" INICIANDO SINCRONIZACIÓN FINAL ANTES DE CERRAR...")
        result = {
            'scans_synced': 0,
            'increments_synced': 0,
            'production_synced': 0,
            'errors': []
        }
        
        try:
            # 1. Sincronizar incrementos pendientes en direct_mysql
            try:
                if hasattr(self, '_direct_mysql') and self._direct_mysql:
                    logger.info(" Sincronizando incrementos pendientes...")
                    self._direct_mysql.sync_pending_increments()
                    pending_queue = getattr(self._direct_mysql, '_sync_queue', [])
                    result['increments_synced'] = len(pending_queue)
                    if result['increments_synced'] > 0:
                        logger.info(f" {result['increments_synced']} incrementos sincronizados")
            except Exception as e:
                error_msg = f"Error sincronizando incrementos: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
            
            # 2. Verificar scans en SQLite que no estén en MySQL
            try:
                logger.info(" Verificando scans pendientes de sincronizar...")
                with self._get_sqlite_connection(timeout=5.0) as conn:
                    today = datetime.now(ZoneInfo("America/Monterrey")).strftime("%Y-%m-%d")
                    
                    local_cursor = conn.execute("SELECT COUNT(*) FROM scans_local WHERE DATE(ts) = ?", (today,))
                    local_count = local_cursor.fetchone()[0]
                    
                    logger.info(f" Scans locales hoy: {local_count}")
                    
                    from ..db import get_db
                    db = get_db()
                    with db.get_connection() as mysql_conn:
                        with mysql_conn.cursor() as mysql_cursor:
                            mysql_cursor.execute("SELECT COUNT(*) FROM input_main WHERE DATE(ts) = %s", (today,))
                            mysql_count = mysql_cursor.fetchone()[0]

                            logger.info(f" Scans en MySQL hoy: {mysql_count}")
                            
                            if local_count > mysql_count:
                                diff = local_count - mysql_count
                                logger.warning(f"  Faltan {diff} scans en MySQL. Sincronizando...")
                                
                                # Obtener scans locales del día con fecha incluida
                                local_scans = conn.execute("""
                                    SELECT id, raw, nparte, linea, ts, fecha, modelo, scan_format, plan_id, barcode_sequence
                                    FROM scans_local WHERE DATE(ts) = ? ORDER BY ts DESC LIMIT ?
                                """, (today, diff)).fetchall()

                                for scan in local_scans:
                                    try:
                                        # Convertir plan_id y barcode_sequence para compatibilidad
                                        mysql_cursor.execute("""
                                            INSERT INTO input_main 
                                            (raw, nparte, linea, ts, fecha, modelo, scan_format, barcode_sequence, result)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'OK')
                                            ON DUPLICATE KEY UPDATE id=id
                                        """, (scan[1], scan[2], scan[3], scan[4], scan[5], scan[6], scan[7], scan[9]))
                                        result['scans_synced'] += 1
                                    except Exception:
                                        pass
                                
                                mysql_conn.commit()
                                logger.info(f" {result['scans_synced']} scans sincronizados a MySQL")
                            else:
                                logger.info(" Todos los scans están sincronizados")
                    
            except Exception as e:
                error_msg = f"Error sincronizando scans: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
            
            # 3. Sincronizar totales de producción
            try:
                logger.info(" Verificando totales de producción...")
                with self._get_sqlite_connection(timeout=5.0) as conn:
                    today = datetime.now(ZoneInfo("America/Monterrey")).strftime("%Y-%m-%d")
                    
                    local_totals = conn.execute("""
                        SELECT plan_id, produced_count FROM production_totals_local WHERE fecha = ?
                    """, (today,)).fetchall()
                    
                    if local_totals:
                        from ..db import get_db
                        db = get_db()
                        with db.get_connection() as mysql_conn:
                            with mysql_conn.cursor() as mysql_cursor:
                                for plan_id, local_produced in local_totals:
                                    mysql_cursor.execute("SELECT produced_count FROM plan_main WHERE id = %s", (plan_id,))
                                    mysql_result = mysql_cursor.fetchone()
                                    
                                    if mysql_result:
                                        mysql_produced = mysql_result[0] or 0
                                        if local_produced > mysql_produced:
                                            diff = local_produced - mysql_produced
                                            logger.warning(f"  Plan {plan_id}: Local={local_produced}, MySQL={mysql_produced} (diff={diff})")
                                            
                                            mysql_cursor.execute("""
                                                UPDATE plan_main SET produced_count = %s, updated_at = NOW() WHERE id = %s
                                            """, (local_produced, plan_id))
                                            result['production_synced'] += 1
                                
                                mysql_conn.commit()
                                if result['production_synced'] > 0:
                                    logger.info(f" {result['production_synced']} totales de producción sincronizados")
                    else:
                        logger.info(" No hay totales pendientes de sincronizar")
                        
            except Exception as e:
                error_msg = f"Error sincronizando producción: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
            
            total_synced = result['scans_synced'] + result['increments_synced'] + result['production_synced']
            if total_synced > 0:
                logger.info(f" SINCRONIZACIÓN COMPLETADA: {total_synced} registros sincronizados")
            else:
                logger.info(" SINCRONIZACIÓN COMPLETADA: Todo estaba sincronizado")
            
            if result['errors']:
                logger.warning(f"  {len(result['errors'])} errores durante sincronización")
            
            return result
            
        except Exception as e:
            logger.error(f" Error en sincronización final: {e}")
            result['errors'].append(str(e))
            return result
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
