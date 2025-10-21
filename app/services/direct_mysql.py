"""
Sistema DIRECTO a MySQL sin parseo local
- Escaneo RAW directo a MySQL
- Parseo en MySQL via stored procedures
- Conteo cada 15 seg desde MySQL
- ⚡ COLA ASÍNCRONA para escaneo ultra-rápido
"""
import logging
import sqlite3
from datetime import date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, List, Callable
import threading
import time
import queue

from ..config import settings
from ..db import get_db

logger = logging.getLogger(__name__)

# Singleton holder for DirectMySQLSystem
_direct_mysql_instance: Optional["DirectMySQLSystem"] = None


class DirectMySQLSystem:
    """Sistema optimizado: RAW directo a MySQL con cola asíncrona"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._count_cache = {}  # Cache temporal de conteos (15 seg)
        self._last_count_update = 0
        self._count_timer = None
        
        # ⚡ CACHÉ DEL PLAN para validación rápida
        self._plan_cache: Dict[tuple[str, str], Dict[str, object]] = {}
        self._plan_cache_time = 0
        self._plan_cache_ttl = 60  # Actualizar cada 60 segundos
        
        # ⚡ COLA ASÍNCRONA para escaneo instantáneo
        self._scan_queue = queue.Queue(maxsize=1000)  # Cola de escaneos pendientes
        self._queue_worker = None
        self._stop_worker = False
        self._scan_cache = {}  # Cache local temporal {raw: scan_id}
        # Ventana de "modo offline" si MySQL falla, para evitar bloqueos
        self._mysql_offline_until = 0.0
        # Estado por línea para controlar cambios de plan y evitar emparejar huérfanos del plan anterior
        self._active_plan_by_line: Dict[str, int] = {}
        self._plan_switch_time: Dict[str, float] = {}
        # Umbral por línea para evitar emparejar huérfanos previos al cambio de plan
        self._plan_switch_last_id: Dict[str, int] = {}
        # Listeners para notificar a la UI tras completar pares
        self._scan_listeners: List[Callable[[str, str, str], None]] = []
        
        # ⚡ Cola de sincronización para reintentar incrementos fallidos
        self._sync_queue = []
        self._sync_timer = None
        
        # ⏰ Timer para cierre automático de planes a las 5:30 PM
        self._auto_close_timer = None
        self._last_auto_close_date = None
        
        # Inicializar sistema
        self._init_mysql_parsers()  # ✅ Recrear funciones MySQL con soporte QR nuevo
        self._start_count_timer()
        self._load_plan_cache()  # ⚡ Cargar plan al iniciar
        self._start_queue_worker()  # ⚡ Iniciar worker de cola
        self._start_sync_timer()  # ⚡ Iniciar timer de sincronización
        self._start_auto_close_timer()  # ⏰ Iniciar timer de cierre automático 5:30 PM
    
    def is_connected(self) -> bool:
        """Verificar si hay conexión a MySQL"""
        try:
            db = get_db()
            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception:
            return False

    def is_quick_online(self) -> bool:
        """Chequeo rápido y no bloqueante del estado online.
        Devuelve False si estamos en ventana offline temporal.
        """
        try:
            return time.time() >= getattr(self, '_mysql_offline_until', 0)
        except Exception:
            return False
    
    def _init_mysql_parsers(self):
        """Crea stored procedures en MySQL para parseo (ACTUALIZADO para QR con ñ)"""
        try:
            db = get_db()
            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Eliminar funciones antiguas para forzar recreación
                    logger.info("🔄 Actualizando funciones MySQL para soporte QR nuevo...")
                    cursor.execute("DROP FUNCTION IF EXISTS extract_nparte")
                    cursor.execute("DROP FUNCTION IF EXISTS detect_format")
                    cursor.execute("DROP FUNCTION IF EXISTS extract_fecha")
                    cursor.execute("DROP FUNCTION IF EXISTS extract_linea")
                    
                    # Función para extraer N° Parte del RAW
                    cursor.execute("""
                        CREATE FUNCTION extract_nparte(raw_scan TEXT)
                        RETURNS VARCHAR(50)
                        DETERMINISTIC
                        BEGIN
                            DECLARE nparte VARCHAR(50);
                            
                            -- Si es QR (termina en ; o ñ/Ñ)
                            IF raw_scan LIKE '%;%' OR raw_scan LIKE '%ñ%' OR raw_scan LIKE '%Ñ%' THEN
                                -- Formato QR antiguo: I20250226-0013-00002;MAIN;EBR41039117;1;
                                -- Formato QR nuevo: I20251001'0004'00005ñMAINñEBR24212304ñ1ñ
                                IF raw_scan LIKE '%;%' THEN
                                    SET nparte = SUBSTRING_INDEX(SUBSTRING_INDEX(raw_scan, ';', 3), ';', -1);
                                ELSE
                                    -- Usar ñ o Ñ como separador
                                    SET nparte = SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(REPLACE(raw_scan, 'ñ', ';'), 'Ñ', ';'), ';', 3), ';', -1);
                                END IF;
                            ELSE
                                -- Si es BARCODE (EBR41039117922509201292)
                                -- Los últimos 12 chars son ISO(2)+Fecha(6)+Seq(4)
                                SET nparte = LEFT(raw_scan, CHAR_LENGTH(raw_scan) - 12);
                            END IF;
                            
                            RETURN nparte;
                        END
                    """)
                    
                    # Función para detectar formato (QR o BARCODE)
                    cursor.execute("""
                        CREATE FUNCTION detect_format(raw_scan TEXT)
                        RETURNS VARCHAR(10)
                        DETERMINISTIC
                        BEGIN
                            IF raw_scan LIKE '%;%' OR raw_scan LIKE '%ñ%' OR raw_scan LIKE '%Ñ%' THEN
                                RETURN 'QR';
                            ELSE
                                RETURN 'BARCODE';
                            END IF;
                        END
                    """)
                    
                    # Función para extraer fecha ISO del RAW
                    cursor.execute("""
                        CREATE FUNCTION extract_fecha(raw_scan TEXT)
                        RETURNS DATE
                        DETERMINISTIC
                        BEGIN
                            DECLARE fecha_str VARCHAR(8);
                            DECLARE fecha_iso DATE;
                            
                            -- Si es QR (con ; o ñ/Ñ)
                            IF raw_scan LIKE '%;%' OR raw_scan LIKE '%ñ%' OR raw_scan LIKE '%Ñ%' THEN
                                -- Formato: I20250226-0013-00002;... o I20251001'0004'00005ñ...
                                SET fecha_str = SUBSTRING(raw_scan, 2, 8); -- YYYYMMDD
                                SET fecha_iso = STR_TO_DATE(fecha_str, '%Y%m%d');
                            ELSE
                                -- Si es BARCODE, extraer de los últimos 12
                                -- ISO(2) + DDMMYY(6) + Seq(4)
                                SET fecha_str = SUBSTRING(raw_scan, -10, 6); -- DDMMYY
                                SET fecha_iso = STR_TO_DATE(fecha_str, '%d%m%y');
                            END IF;
                            
                            RETURN fecha_iso;
                        END
                    """)
                    
                    # Función para extraer línea (estación)
                    cursor.execute("""
                        CREATE FUNCTION extract_linea(raw_scan TEXT)
                        RETURNS VARCHAR(50)
                        DETERMINISTIC
                        BEGIN
                            DECLARE linea VARCHAR(50);
                            
                            -- Si es QR (con ; o ñ/Ñ)
                            IF raw_scan LIKE '%;%' OR raw_scan LIKE '%ñ%' OR raw_scan LIKE '%Ñ%' THEN
                                -- Formato QR antiguo: I20250226-0013-00002;MAIN;EBR41039117;1;
                                -- Formato QR nuevo: I20251001'0004'00005ñMAINñEBR24212304ñ1ñ
                                IF raw_scan LIKE '%;%' THEN
                                    SET linea = SUBSTRING_INDEX(SUBSTRING_INDEX(raw_scan, ';', 2), ';', -1);
                                ELSE
                                    -- Usar ñ o Ñ como separador
                                    SET linea = SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(REPLACE(raw_scan, 'ñ', ';'), 'Ñ', ';'), ';', 2), ';', -1);
                                END IF;
                            ELSE
                                SET linea = 'BARCODE'; -- Default para barcodes
                            END IF;
                            
                            RETURN linea;
                        END
                    """)
                    
                    conn.commit()
                    logger.info("✅ Stored procedures de parseo creadas en MySQL")
                    
        except Exception as e:
            logger.error(f"Error creando stored procedures: {e}")
            # No es crítico, continuamos sin ellas
    
    def _normalize_part(self, part: str | None) -> str:
        return (part or "").strip().upper()

    def _normalize_line(self, line: str | None) -> str:
        return (line or "").strip().upper()

    def _update_plan_cache_entry(self, info: Dict[str, object]) -> None:
        part_no = self._normalize_part(info.get("part_no") if isinstance(info, dict) else None)
        line = self._normalize_line(info.get("line") if isinstance(info, dict) else None)
        if not part_no or not line:
            return
        with self._lock:
            self._plan_cache[(part_no, line)] = {
                "status": info.get("status", "EN PROGRESO"),
                "lot_no": info.get("lot_no"),
                "plan_id": info.get("id"),
                "plan_count": info.get("plan_count"),
                "produced_count": info.get("produced_count"),
                "line": line,
                "part_no": part_no,
            }
            self._plan_cache_time = time.time()

    # ---------- Timers y workers ----------
    def _start_count_timer(self) -> None:
        """Inicia un hilo ligero que refresca el cache y conteos cada ~15s."""
        if self._count_timer and self._count_timer.is_alive():
            return

        def loop():
            while not getattr(self, "_stop_worker", False):
                try:
                    time.sleep(15)
                    # Refrescar plan en cache (no bloqueante)
                    self._load_plan_cache(force=False)
                    # Limpieza simple del cache de duplicados
                    if len(self._scan_cache) > 1000:
                        with self._lock:
                            self._scan_cache.clear()
                except Exception:
                    time.sleep(5)

        self._count_timer = threading.Thread(target=loop, daemon=True)
        self._count_timer.start()

    def _start_queue_worker(self) -> None:
        """Worker que procesa la cola del escáner en background."""
        if self._queue_worker and self._queue_worker.is_alive():
            return

        def worker():
            while not self._stop_worker:
                try:
                    try:
                        scan_data = self._scan_queue.get(timeout=0.05)
                    except queue.Empty:
                        continue
                    raw = scan_data.get('raw')
                    linea = scan_data.get('linea')
                    nparte = scan_data.get('nparte')
                    lot_no = scan_data.get('lot_no')
                    self._process_scan_to_mysql(raw, linea, nparte=nparte, lot_no_hint=lot_no)
                    self._scan_queue.task_done()
                except Exception:
                    logger.exception("Error en queue worker")

        self._queue_worker = threading.Thread(target=worker, daemon=True)
        self._queue_worker.start()

    # ---------- Cache de plan ----------
    def _load_plan_cache(self, force: bool = False) -> None:
        """Recarga cache mínimo (id/status/lot_no) de planes EN PROGRESO."""
        try:
            now = time.time()
            if not force and (now - self._plan_cache_time) < self._plan_cache_ttl:
                return
            # Evitar consultas si estamos en ventana offline
            if now < getattr(self, '_mysql_offline_until', 0):
                return
            db = get_db()
            today = date.today()
            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT id, part_no, line, status, lot_no, plan_count,
                               COALESCE(produced_count,0) AS produced_count
                        FROM plan_main
                        WHERE working_date = %s AND status = 'EN PROGRESO'
                        """,
                        (today,),
                    )
                    rows = cursor.fetchall() or []
            new_cache = {}
            for r in rows:
                if isinstance(r, dict):
                    part_no = self._normalize_part(r.get('part_no'))
                    line = self._normalize_line(r.get('line'))
                    entry = {
                        'status': r.get('status'), 'lot_no': r.get('lot_no'), 'plan_id': r.get('id'),
                        'plan_count': r.get('plan_count'), 'produced_count': r.get('produced_count'),
                        'line': line, 'part_no': part_no,
                    }
                else:
                    part_no = self._normalize_part(r[1])
                    line = self._normalize_line(r[2])
                    entry = {
                        'status': r[3], 'lot_no': r[4], 'plan_id': r[0],
                        'plan_count': r[5], 'produced_count': r[6],
                        'line': line, 'part_no': part_no,
                    }
                if part_no and line:
                    new_cache[(part_no, line)] = entry
            with self._lock:
                self._plan_cache = new_cache
                self._plan_cache_time = now
        except Exception as e:
            logger.warning(f"Error cargando plan cache: {e}")
            # Entrar a modo offline 120s para no saturar
            try:
                self._mysql_offline_until = time.time() + 120
            except Exception:
                pass

    # ---------- API pública usada por UI/servicios ----------
    def register_scan_listener(self, callback: Callable[[str, str, str], None]) -> None:
        """Registra un callback para notificar eventos de escaneo completados."""
        if not callable(callback):
            return
        with self._lock:
            if callback not in self._scan_listeners:
                self._scan_listeners.append(callback)

    def unregister_scan_listener(self, callback: Callable[[str, str, str], None]) -> None:
        """Elimina un callback previamente registrado."""
        with self._lock:
            try:
                self._scan_listeners.remove(callback)
            except ValueError:
                pass

    def _notify_scan_listeners(self, linea: str, nparte: str, event: str) -> None:
        """Invoca los listeners registrados de forma segura."""
        listeners: List[Callable[[str, str, str], None]]
        with self._lock:
            listeners = list(self._scan_listeners)
        for listener in listeners:
            try:
                listener(linea, nparte, event)
            except Exception as err:
                logger.debug(f"Listener de scan lanzó excepción: {err}")

    def add_scan_direct(self, raw: str, linea: str = "M1") -> int:
        """Encola el escaneo para el worker evitando cualquier IO en el hilo de UI.
        Hace sólo validaciones mínimas y dup-cache en memoria.
        """
        try:
            if not raw:
                return -1
            raw = raw.strip()
            # Duplicados en memoria (rápido)
            with self._lock:
                if raw in self._scan_cache:
                    return -2
            # Extraer N° parte de forma ligera (sin SQLite)
            # Soporta QR nuevo (ñ/Ñ), QR antiguo (;) y BARCODE
            from .parser import is_complete_qr
            nparte = None
            if is_complete_qr(raw):
                # QR formato nuevo (ñ o Ñ) o antiguo (;)
                if 'ñ' in raw or 'Ñ' in raw:
                    # Reemplazar ñ/Ñ por ; para split uniforme
                    parts = raw.replace('ñ', ';').replace('Ñ', ';').split(';')
                    if len(parts) >= 3:
                        nparte = parts[2]  # Tercer campo es nparte
                else:
                    parts = raw.split(';')
                    if len(parts) >= 3:
                        nparte = parts[2]
            elif len(raw) >= 13 and raw[-12:].isdigit():
                nparte = raw[:-12]
            if not nparte:
                return -1
            nparte = self._normalize_part(nparte)
            linea = self._normalize_line(linea)
            
            # VALIDACIÓN RÁPIDA: Verificar plan EN PROGRESO antes de encolar
            try:
                from datetime import date
                today_iso = date.today().isoformat()
                with sqlite3.connect(settings.LOCAL_SQLITE_PATH, timeout=0.5) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute(
                        """
                        SELECT part_no FROM plan_local 
                        WHERE working_date=? AND line=? AND status='EN PROGRESO'
                        LIMIT 1
                        """,
                        (today_iso, linea),
                    )
                    plan_progreso = cursor.fetchone()
                    
                    if plan_progreso:
                        part_no_progreso = plan_progreso['part_no']
                        if nparte != part_no_progreso:
                            logger.warning(
                                f"🚫 [scan-quick] Modelo diferente bloqueado: "
                            )
                            return -10  # Modelo diferente al plan en progreso
            except sqlite3.OperationalError as db_err:
                # Si la DB está bloqueada, proceder con cautela
                logger.warning(f"⚠️ [scan-quick] DB bloqueada, permitiendo escaneo: {db_err}")
            except Exception as val_err:
                # Si falla la validación, permitir el escaneo (fail-safe)
                logger.debug(f"[scan-quick] Validación omitida: {val_err}")
            
            # Encolar para que el worker valide en SQLite/MySQL
            scan_data = {'raw': raw, 'linea': linea, 'nparte': nparte, 'lot_no': None}
            try:
                self._scan_queue.put_nowait(scan_data)
                temp_id = int(time.time() * 1000) % 1000000 or 1
                with self._lock:
                    self._scan_cache[raw] = temp_id
                return temp_id
            except queue.Full:
                return -1
        except Exception as e:
            logger.error(f"Error en add_scan_direct: {e}")
            return -1

    def _process_scan_to_mysql(self, raw: str, linea: str = "M1", *, nparte: Optional[str] = None, lot_no_hint: Optional[str] = None) -> int:
        """Procesa el escaneo: asegura cambio de estado local inmediato y sincroniza MySQL.
        (Inserción de escaneo a MySQL omitida aquí para enfocarnos en la transición de plan.)
        """
        try:
            if not raw:
                return -1
            raw = raw.strip()
            # Detectar N° parte (soporta QR nuevo ñ, antiguo ; y BARCODE)
            from .parser import is_complete_qr
            if not nparte:
                if is_complete_qr(raw):
                    # QR formato nuevo (ñ/Ñ) o antiguo (;)
                    if 'ñ' in raw or 'Ñ' in raw:
                        # Normalizar ambos separadores a ; para split uniforme
                        parts = raw.replace('ñ', ';').replace('Ñ', ';').split(';')
                        if len(parts) >= 3:
                            nparte = parts[2]
                    else:
                        parts = raw.split(';')
                        if len(parts) >= 3:
                            nparte = parts[2]
                elif len(raw) >= 13 and raw[-12:].isdigit():
                    # BARCODE
                    nparte = raw[:-12]
                else:
                    return -1
            if not nparte:
                return -1
            nparte = self._normalize_part(nparte)
            linea = self._normalize_line(linea)
            logger.debug(f"[scan] start linea={linea} nparte={nparte} raw={raw[:40]}")
            
            # VALIDACIÓN: Verificar si hay un plan EN PROGRESO y si el escaneo corresponde
            today_iso = date.today().isoformat()
            with sqlite3.connect(settings.LOCAL_SQLITE_PATH, timeout=5.0) as conn:
                conn.row_factory = sqlite3.Row
                # Buscar plan EN PROGRESO en esta línea
                cur_progreso = conn.execute(
                    """
                    SELECT part_no FROM plan_local 
                    WHERE working_date=? AND line=? AND status='EN PROGRESO'
                    LIMIT 1
                    """,
                    (today_iso, linea),
                )
                plan_en_progreso = cur_progreso.fetchone()
                
                # Si hay un plan EN PROGRESO, validar que el escaneo sea del mismo modelo
                if plan_en_progreso:
                    part_no_progreso = plan_en_progreso['part_no']
                    if nparte != part_no_progreso:
                        logger.warning(
                            f"🚫 [scan] Modelo diferente bloqueado: "
                            f"Plan EN PROGRESO={part_no_progreso}, Escaneado={nparte}"
                        )
                        return -10  # Código nuevo: Modelo diferente al plan en progreso
            
            # Buscar plan_id en local
            last_plan_id = self._active_plan_by_line.get(linea)
            plan_changed = False

            with sqlite3.connect(settings.LOCAL_SQLITE_PATH, timeout=5.0) as conn:
                conn.row_factory = sqlite3.Row
                # Ordenar por prioridad:
                # 1. Excluir planes donde produced_count >= plan_count (ya completados físicamente)
                # 2. Primero PLAN (no iniciado), luego PAUSADO, luego EN PROGRESO
                # 3. Por ID ascendente (el primero en la lista tiene prioridad)
                # NOTA: No excluimos por status porque puede no estar actualizado
                cur = conn.execute(
                    """
                    SELECT id, lot_no, status, produced_count, plan_count 
                    FROM plan_local 
                    WHERE working_date=? AND line=? AND part_no=? 
                      AND (
                        produced_count IS NULL 
                        OR plan_count IS NULL 
                        OR produced_count < plan_count
                      )
                    ORDER BY 
                        CASE status 
                            WHEN 'PLAN' THEN 1 
                            WHEN 'PAUSADO' THEN 2 
                            WHEN 'EN PROGRESO' THEN 3
                            WHEN 'TERMINADO' THEN 5
                            ELSE 4
                        END,
                        id ASC
                    LIMIT 1
                    """,
                    (today_iso, linea, nparte),
                )
                r = cur.fetchone()
                if not r:
                    logger.warning(f"[scan] plan no encontrado para linea={linea} parte={nparte} (todos completos)")
                    return -3
                plan_id = r['id']
                lot_no = lot_no_hint or r['lot_no']
                plan_changed = last_plan_id is not None and last_plan_id != plan_id
                
                # Log descriptivo
                produced = r['produced_count'] or 0
                target = r['plan_count'] or 0
                logger.info(f"[scan] 📋 Plan seleccionado: id={plan_id}, lot={lot_no}, status={r['status']}, producido={produced}/{target}")
            logger.debug(f"[scan] plan_id={plan_id} lot={lot_no}")
            # Cerrar otros y activar este localmente (y luego MySQL)
            t_state = time.perf_counter()
            self._finish_other_plans(linea, plan_id)
            logger.debug(f"[scan] finish_other_plans en {time.perf_counter()-t_state:.3f}s")
            t_state = time.perf_counter()
            self._start_plan_now(plan_id)
            logger.debug(f"[scan] start_plan_now en {time.perf_counter()-t_state:.3f}s")

            if plan_changed:
                self._plan_switch_time[linea] = time.time()
            elif last_plan_id is None and linea not in self._plan_switch_time:
                self._plan_switch_time[linea] = 0.0
            self._active_plan_by_line[linea] = plan_id

            switch_last_id = self._plan_switch_last_id.get(linea, 0)

            # Emparejamiento e incremento de producido (soporta QR nuevo ñ y antiguo ;)
            es_qr = is_complete_qr(raw)
            # Si MySQL estuvo fallando, no intentar conectar y trabajar solo local
            mysql_now_allowed = time.time() >= getattr(self, '_mysql_offline_until', 0)
            try:
                if not mysql_now_allowed:
                    logger.debug("[scan] MySQL en ventana offline, salto conexión")
                    raise RuntimeError("MYSQL_OFFLINE_WINDOW")
                db = get_db()
                with db.get_connection() as conn:
                    with conn.cursor() as cursor:
                        if plan_changed:
                            cursor.execute(
                                "SELECT COALESCE(MAX(id), 0) AS max_id FROM input_main WHERE linea=%s",
                                (linea,),
                            )
                            max_id_row = cursor.fetchone()
                            if max_id_row:
                                if isinstance(max_id_row, dict):
                                    switch_last_id = max_id_row.get('max_id', 0) or 0
                                else:
                                    switch_last_id = max_id_row[0] or 0
                            else:
                                switch_last_id = 0
                            self._plan_switch_last_id[linea] = switch_last_id
                        else:
                            switch_last_id = self._plan_switch_last_id.get(linea, 0)
                        from datetime import date as _date
                        today = _date.today()
                        if es_qr:
                            # PRIMERO: Verificar si ya existe un QR huérfano (evitar QR+QR)
                            # Solo considerar huérfanos recientes (últimos 10 segundos)
                            check_qr_query = """
                                SELECT id FROM input_main
                                WHERE nparte=%s AND lot_no=%s AND linea=%s
                                  AND DATE(created_at)=CURRENT_DATE()
                                  AND raw IS NOT NULL AND raw_barcode IS NULL AND is_complete=0
                                  AND scan_format='QR'
                                  AND TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 10
                            """
                            check_params = [nparte, lot_no, linea]
                            if switch_last_id:
                                check_qr_query += " AND id > %s"
                                check_params.append(switch_last_id)
                            check_qr_query += " LIMIT 1"
                            cursor.execute(check_qr_query, tuple(check_params))
                            existing_qr = cursor.fetchone()
                            
                            if existing_qr:
                                # Ya existe un QR esperando BARCODE - NO PERMITIR OTRO QR
                                logger.warning(f"[scan] ❌ RECHAZADO: Ya existe un QR esperando BARCODE. Escanea el BARCODE primero.")
                                self._notify_scan_listeners(linea, nparte, "DUPLICATE_QR_REJECTED")
                                return -8  # Código de error: QR duplicado
                            
                            # Buscar BARCODE huérfano para emparejar (últimos 10 segundos)
                            query = """
                                SELECT id FROM input_main
                                WHERE nparte=%s AND lot_no=%s AND linea=%s
                                  AND DATE(created_at)=CURRENT_DATE()
                                  AND raw_barcode IS NOT NULL AND is_complete=0
                                  AND scan_format='BARCODE'
                                  AND TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 10
                            """
                            params = [nparte, lot_no, linea]
                            if switch_last_id:
                                query += " AND id > %s"
                                params.append(switch_last_id)
                            query += " LIMIT 1"
                            cursor.execute(query, tuple(params))
                            orphan = cursor.fetchone()
                            if orphan:
                                orphan_id = orphan['id'] if isinstance(orphan, dict) else orphan[0]
                                cursor.execute(
                                    """
                                    UPDATE input_main
                                    SET raw=%s,
                                        raw_pair=CONCAT(raw_barcode,'|',%s),
                                        is_complete=1,
                                        scan_format='PAIR'
                                    WHERE id=%s
                                    """,
                                    (raw, raw, orphan_id),
                                )
                                conn.commit()
                                # Guardar umbral y notificar listeners
                                try:
                                    self._plan_switch_last_id[linea] = max(self._plan_switch_last_id.get(linea, 0), int(orphan_id))
                                except Exception:
                                    self._plan_switch_last_id[linea] = orphan_id
                                self._notify_scan_listeners(linea, nparte, "PAIR_COMPLETED")
                                return 1
                            else:
                                # Insertar QR huérfano y esperar complemento - NO ENVIAR AÚN
                                cursor.execute(
                                    """
                                    INSERT INTO input_main (raw, nparte, lot_no, linea, estacion, tipo, fecha, ts, created_at, is_complete, scan_format)
                                    VALUES (%s, %s, %s, %s, %s, 'P', CURRENT_DATE(), NOW(), NOW(), 0, 'QR')
                                    """,
                                    (raw, nparte, lot_no, linea, linea),
                                )
                                conn.commit()
                                logger.debug(f"[scan] ⏳ QR huérfano almacenado, esperando BARCODE para completar PAIR")
                                self._notify_scan_listeners(linea, nparte, "WAITING_FOR_BARCODE")
                                return -5
                        else:
                            # PRIMERO: Verificar si ya existe un BARCODE huérfano (evitar BARCODE+BARCODE)
                            # Solo considerar huérfanos recientes (últimos 10 segundos)
                            check_barcode_query = """
                                SELECT id FROM input_main
                                WHERE nparte=%s AND lot_no=%s AND linea=%s
                                  AND DATE(created_at)=CURRENT_DATE()
                                  AND raw_barcode IS NOT NULL AND is_complete=0
                                  AND scan_format='BARCODE'
                                  AND TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 10
                            """
                            check_params = [nparte, lot_no, linea]
                            if switch_last_id:
                                check_barcode_query += " AND id > %s"
                                check_params.append(switch_last_id)
                            check_barcode_query += " LIMIT 1"
                            cursor.execute(check_barcode_query, tuple(check_params))
                            existing_barcode = cursor.fetchone()
                            
                            if existing_barcode:
                                # Ya existe un BARCODE esperando QR - NO PERMITIR OTRO BARCODE
                                logger.warning(f"[scan] ❌ RECHAZADO: Ya existe un BARCODE esperando QR. Escanea el QR primero.")
                                self._notify_scan_listeners(linea, nparte, "DUPLICATE_BARCODE_REJECTED")
                                return -9  # Código de error: BARCODE duplicado
                            
                            # BARCODE: buscar QR huérfano para emparejar (últimos 10 segundos)
                            query = """
                                SELECT id FROM input_main
                                WHERE nparte=%s AND lot_no=%s AND linea=%s
                                  AND DATE(created_at)=CURRENT_DATE()
                                  AND raw IS NOT NULL AND raw_barcode IS NULL AND is_complete=0
                                  AND scan_format='QR'
                                  AND TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 10
                            """
                            params = [nparte, lot_no, linea]
                            if switch_last_id:
                                query += " AND id > %s"
                                params.append(switch_last_id)
                            query += " LIMIT 1"
                            cursor.execute(query, tuple(params))
                            orphan = cursor.fetchone()
                            if orphan:
                                orphan_id = orphan['id'] if isinstance(orphan, dict) else orphan[0]
                                cursor.execute(
                                    """
                                    UPDATE input_main
                                    SET raw_barcode=%s,
                                        raw_pair=CONCAT(%s,'|',raw),
                                        is_complete=1,
                                        scan_format='PAIR'
                                    WHERE id=%s
                                    """,
                                    (raw, raw, orphan_id),
                                )
                                conn.commit()
                                # Guardar umbral y notificar listeners
                                try:
                                    self._plan_switch_last_id[linea] = max(self._plan_switch_last_id.get(linea, 0), int(orphan_id))
                                except Exception:
                                    self._plan_switch_last_id[linea] = orphan_id
                                self._notify_scan_listeners(linea, nparte, "PAIR_COMPLETED")
                                return 1
                            else:
                                # Insertar BARCODE huérfano y esperar complemento - NO ENVIAR AÚN
                                # Preparar RAW sintético para cumplir NOT NULL en 'raw'
                                raw_sint = f"ONLY_BARCODE|{raw}"
                                cursor.execute(
                                    """
                                    INSERT INTO input_main (raw, raw_barcode, nparte, lot_no, linea, estacion, tipo, fecha, ts, created_at, is_complete, scan_format)
                                    VALUES (%s, %s, %s, %s, %s, %s, 'P', CURRENT_DATE(), NOW(), NOW(), 0, 'BARCODE')
                                    """,
                                    (raw_sint, raw, nparte, lot_no, linea, linea)
                                )
                                conn.commit()
                                logger.debug(f"[scan] ⏳ BARCODE huérfano almacenado, esperando QR para completar PAIR")
                                self._notify_scan_listeners(linea, nparte, "WAITING_FOR_QR")
                                return -5
            except Exception as e:
                if str(e) != "MYSQL_OFFLINE_WINDOW":
                    try:
                        self._mysql_offline_until = time.time() + 120  # 2 minutos
                        logger.warning(f"[scan] MySQL offline 120s por error: {e}")
                    except Exception:
                        pass
                else:
                    logger.debug("[scan] MySQL offline window activa, no se intenta conexión")
            logger.debug(f"[scan] completed linea={linea} nparte={nparte}")
            return 1
        except Exception as e:
            logger.error(f"Error en _process_scan_to_mysql: {e}")
            return -1

    def _finish_other_plans(self, line: str, exclude_plan_id: int) -> None:
        """Marca como TERMINADO cualquier plan EN PROGRESO o PAUSADO en la línea, excepto el indicado."""
        start_time = time.perf_counter()
        try:
            from ..config import settings
            from ..db import get_db
            today_iso = date.today().isoformat()

            other_ids: list[int] = []
            for attempt in range(10):
                try:
                    with sqlite3.connect(settings.LOCAL_SQLITE_PATH, timeout=5.0) as conn:
                        conn.execute("PRAGMA busy_timeout=5000")
                        conn.row_factory = sqlite3.Row
                        cur = conn.execute(
                            """
                            SELECT id FROM plan_local
                            WHERE working_date = ? AND line = ?
                              AND status IN ('EN PROGRESO','PAUSADO') AND id <> ?
                            """,
                            (today_iso, line, exclude_plan_id),
                        )
                        rows = cur.fetchall() or []
                        other_ids = [r["id"] for r in rows]
                        if other_ids:
                            conn.executemany(
                                "UPDATE plan_local SET status='TERMINADO', updated_at=CURRENT_TIMESTAMP WHERE id = ?",
                                [(pid,) for pid in other_ids],
                            )
                            conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower() and attempt < 9:
                        logger.debug(f"finish_other_plans[{line}] retry {attempt+1} por lock: {e}")
                        import time as _t
                        _t.sleep(0.05 * (attempt + 1))
                        continue
                    raise

            if not other_ids:
                logger.debug(f"finish_other_plans[{line}] sin planes activos ({time.perf_counter()-start_time:.3f}s)")
                return

            logger.debug(f"finish_other_plans[{line}] cerró {len(other_ids)} plan(es) en {time.perf_counter()-start_time:.3f}s")

            try:
                db = get_db()
                with db.get_connection() as conn:
                    with conn.cursor() as cursor:
                        for pid in other_ids:
                            cursor.execute(
                                """
                                UPDATE plan_main
                                SET status='TERMINADO', updated_at = NOW()
                                WHERE id = %s
                                """,
                                (pid,),
                            )
                    conn.commit()
            except Exception as e:
                logger.warning(f"No se pudo terminar planes antiguos en MySQL: {e}")
        except Exception as e:
            logger.warning(f"Error finalizando planes locales antiguos: {e}")

    def _start_plan_now(self, plan_id: int) -> None:
        """Marca EN PROGRESO un plan localmente y luego sincroniza en MySQL."""
        start_time = time.perf_counter()
        try:
            from ..config import settings
            for attempt in range(10):
                try:
                    with sqlite3.connect(settings.LOCAL_SQLITE_PATH, timeout=5.0) as conn:
                        conn.execute("PRAGMA busy_timeout=5000")
                        conn.execute(
                            "UPDATE plan_local SET status='EN PROGRESO', started_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id = ?",
                            (plan_id,),
                        )
                        conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower() and attempt < 9:
                        logger.debug(f"start_plan_now[{plan_id}] retry {attempt+1} por lock: {e}")
                        time.sleep(0.05 * (attempt + 1))
                        continue
                    raise
        except Exception as e:
            logger.warning(f"No se pudo actualizar SQLite local a EN PROGRESO para plan_id={plan_id}: {e}")
        else:
            logger.debug(f"start_plan_now[{plan_id}] actualizado localmente en {time.perf_counter()-start_time:.3f}s")

        try:
            db = get_db()
            db.actualizar_estado_plan(plan_id, 'EN PROGRESO')
        except Exception as e:
            logger.warning(f"No se pudo actualizar MySQL a EN PROGRESO para plan_id={plan_id}: {e}")

    def _start_sync_timer(self) -> None:
        """Inicia timer para sincronizar incrementos pendientes cada 30 segundos"""
        def sync_task():
            try:
                self.sync_pending_increments()
            except Exception as e:
                logger.error(f"Error en sync_task: {e}")
            finally:
                # Re-programar para 30 segundos después
                self._sync_timer = threading.Timer(30.0, sync_task)
                self._sync_timer.daemon = True
                self._sync_timer.start()
                
        # Iniciar primera ejecución
        self._sync_timer = threading.Timer(30.0, sync_task)
        self._sync_timer.daemon = True
        self._sync_timer.start()
        logger.info("⏰ Timer de sincronización iniciado (cada 30 seg)")

    def _add_to_sync_queue(self, plan_id: int, linea: str, nparte: str, increment: int) -> None:
        """Agrega incremento pendiente a cola de sincronización"""
        self._sync_queue.append({
            'plan_id': plan_id,
            'linea': linea,
            'nparte': nparte,
            'increment': increment,
            'timestamp': time.time()
        })
        logger.debug(f"📝 Agregado a cola de sync: plan_id={plan_id}, increment={increment}")

    def sync_pending_increments(self) -> None:
        """Sincroniza incrementos pendientes con MySQL (llamar periódicamente)"""
        if not self._sync_queue:
            return
            
        pending = self._sync_queue.copy()
        self._sync_queue.clear()
        
        synced = 0
        failed = []
        
        try:
            db = get_db()
            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    for item in pending:
                        try:
                            cursor.execute(
                                """
                                UPDATE plan_main
                                SET produced_count = COALESCE(produced_count,0)+%s,
                                    updated_at=NOW()
                                WHERE id=%s
                                """,
                                (item['increment'], item['plan_id']),
                            )
                            synced += 1
                        except Exception as e:
                            logger.warning(f"Fallo sincronizar plan_id={item['plan_id']}: {e}")
                            failed.append(item)
                            
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Error en sincronización batch: {e}")
            failed.extend(pending)
            
        # Re-agregar los que fallaron
        if failed:
            self._sync_queue.extend(failed)
            
        if synced > 0:
            logger.info(f"✅ Sincronizados {synced} incrementos pendientes con MySQL")

    def _start_auto_close_timer(self) -> None:
        """Inicia timer para cerrar planes automáticamente a las 5:30 PM y 10:00 PM (Monterrey)"""
        from datetime import datetime, timedelta
        
        # Diccionario para rastrear ejecuciones por horario
        if not hasattr(self, '_executed_closures'):
            self._executed_closures = {}  # {fecha: [hora1, hora2]}
        
        def check_and_close():
            try:
                # Obtener hora actual de Monterrey (UTC-6)
                monterrey_tz = ZoneInfo("America/Monterrey")
                now_monterrey = datetime.now(monterrey_tz)
                today_date = now_monterrey.date().isoformat()
                current_hour = now_monterrey.hour
                current_minute = now_monterrey.minute
                
                # Inicializar lista de horas ejecutadas para hoy
                if today_date not in self._executed_closures:
                    self._executed_closures[today_date] = []
                
                executed_today = self._executed_closures[today_date]
                
                # Verificar si es hora de cerrar (5:30 PM o 10:00 PM)
                should_close_530pm = (current_hour == 17 and current_minute >= 30) and '17:30' not in executed_today
                should_close_10pm = (current_hour == 22 and current_minute >= 0) and '22:00' not in executed_today
                
                if should_close_530pm:
                    logger.info("🕔 5:30 PM - Ejecutando cierre automático de planes (Turno 1)")
                    self._auto_close_active_plans()
                    self._executed_closures[today_date].append('17:30')
                    logger.info("✅ Cierre automático 5:30 PM completado")
                    
                elif should_close_10pm:
                    logger.info("� 10:00 PM - Ejecutando cierre automático de planes (Turno 2)")
                    self._auto_close_active_plans()
                    self._executed_closures[today_date].append('22:00')
                    logger.info("✅ Cierre automático 10:00 PM completado")
                
                # Limpiar historial de días anteriores (mantener solo últimos 3 días)
                if len(self._executed_closures) > 3:
                    oldest_date = min(self._executed_closures.keys())
                    del self._executed_closures[oldest_date]
                        
            except Exception as e:
                logger.error(f"❌ Error en check_and_close: {e}")
            finally:
                # Re-programar para 1 minuto después (verificar cada minuto)
                self._auto_close_timer = threading.Timer(60.0, check_and_close)
                self._auto_close_timer.daemon = True
                self._auto_close_timer.start()
                
        # Iniciar primera verificación
        self._auto_close_timer = threading.Timer(60.0, check_and_close)
        self._auto_close_timer.daemon = True
        self._auto_close_timer.start()
        logger.info("⏰ Timer de cierre automático iniciado (5:30 PM y 10:00 PM)")

    def _auto_close_active_plans(self) -> None:
        """Cierra automáticamente todos los planes activos (EN PROGRESO o PAUSADO) a las 5:30 PM"""
        closed_count = 0
        
        try:
            from datetime import datetime
            monterrey_tz = ZoneInfo("America/Monterrey")
            now_monterrey = datetime.now(monterrey_tz)
            today_iso = now_monterrey.date().isoformat()
            
            logger.info("🕔 Iniciando cierre automático de todos los planes activos...")
            
            # 1. Cerrar planes en SQLite local (TODAS las líneas)
            with sqlite3.connect(settings.LOCAL_SQLITE_PATH, timeout=5.0) as conn:
                # Obtener todos los planes activos
                cursor = conn.execute(
                    """
                    SELECT id, line, part_no, status 
                    FROM plan_local 
                    WHERE working_date = ? 
                      AND status IN ('EN PROGRESO', 'PAUSADO')
                    """,
                    (today_iso,)
                )
                local_plans = cursor.fetchall()
                
                if local_plans:
                    # ✅ Actualizar a TERMINADO todos los planes activos (SIN ended_at que no existe)
                    conn.execute(
                        """
                        UPDATE plan_local 
                        SET status = 'TERMINADO',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE working_date = ? 
                          AND status IN ('EN PROGRESO', 'PAUSADO')
                        """,
                        (today_iso,)
                    )
                    conn.commit()
                    closed_count = len(local_plans)
                    logger.info(f"✅ LOCAL: {closed_count} planes cerrados automáticamente")
                    
                    # Log de cada plan cerrado
                    for plan in local_plans:
                        logger.info(f"   📋 Plan cerrado: Line={plan[1]}, Part={plan[2]}, Status={plan[3]} → TERMINADO")
                else:
                    logger.info("ℹ️ No hay planes activos para cerrar en SQLite local")
            
            # 2. Sincronizar con MySQL (TODAS las líneas)
            try:
                db = get_db()
                with db.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE plan_main
                            SET status = 'TERMINADO',
                                ended_at = NOW(),
                                updated_at = NOW()
                            WHERE DATE(working_date) = CURRENT_DATE()
                              AND status IN ('EN PROGRESO', 'PAUSADO')
                            """
                        )
                        mysql_closed = cursor.rowcount
                        conn.commit()
                        logger.info(f"✅ MySQL: {mysql_closed} planes cerrados automáticamente")
                        
            except Exception as mysql_err:
                logger.warning(f"⚠️ Error cerrando planes en MySQL (local ya cerrado): {mysql_err}")
            
            # 3. Limpiar estado interno de TODAS las líneas
            self._active_plan_by_line.clear()
            self._plan_switch_time.clear()
            self._plan_switch_last_id.clear()
            logger.info("🧹 Estado interno limpiado para todas las líneas")
            
            # 4. Notificar listeners
            if closed_count > 0:
                for listener in self._scan_listeners:
                    try:
                        listener("ALL", "", "PLANS_AUTO_CLOSED")
                    except Exception:
                        pass
                        
            logger.info(f"🏁 CIERRE AUTOMÁTICO COMPLETADO - {closed_count} planes terminados a las 5:30 PM")
            
        except Exception as e:
            logger.error(f"❌ Error en cierre automático de planes: {e}")

    def stop(self) -> None:
        """Detiene timers y workers"""
        self._stop_worker = True
        if self._count_timer:
            self._count_timer.cancel()
        if self._sync_timer:
            self._sync_timer.cancel()
        if self._auto_close_timer:
            self._auto_close_timer.cancel()
        # Sincronizar pendientes antes de cerrar
        self.sync_pending_increments()
        logger.info("🛑 DirectMySQLSystem detenido")


def get_direct_mysql() -> DirectMySQLSystem:
    """Obtiene instancia singleton del sistema directo MySQL"""
    global _direct_mysql_instance
    if _direct_mysql_instance is None:
        _direct_mysql_instance = DirectMySQLSystem()
    return _direct_mysql_instance
