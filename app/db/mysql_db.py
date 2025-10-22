import logging
from typing import List, Optional, Dict, Any
import pymysql
from pymysql import Error
from contextlib import contextmanager
from datetime import datetime, date
import time
import threading

from ..config import settings
from ..models.entities import ScanRecord, ModeloRef, ResumenProduccion


logger = logging.getLogger(__name__)


class MySQLDatabase:
    """Adaptador para MySQL usando PyMySQL con optimizaciones de rendimiento"""

    def __init__(self):
        self.host = settings.MYSQL_HOST
        self.port = settings.MYSQL_PORT
        self.database = settings.MYSQL_DB
        self.user = settings.MYSQL_USER
        self.password = settings.MYSQL_PASSWORD
        self._connection_lock = threading.Lock()
        self._last_queue_check = 0
        self._cached_queue_size = 0
        self._schema_initialized = False
        self._test_connection()

    def _test_connection(self):
        """Verifica que la conexión a MySQL funcione"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            logger.info(f"✅ Conexión MySQL exitosa: {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"❌ Error conectando a MySQL: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Context manager para conexiones MySQL optimizado con timeouts robustos"""
        connection = None
        with self._connection_lock:
            try:
                connection = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True,  # Habilitar autocommit para operaciones simples
                    connect_timeout=5,   # ✅ Timeout de conexión reducido para detectar problemas rápido
                    read_timeout=10,     # ✅ Timeout de lectura para evitar bloqueos
                    write_timeout=10,    # ✅ Timeout de escritura para evitar bloqueos
                )
                
                # Configurar zona horaria de México (Nuevo León) al conectarse
                with connection.cursor() as cursor:
                    cursor.execute("SET time_zone = '-06:00'")  # UTC-6 (Nuevo León, México)
                
                yield connection
            except Error as e:
                if connection:
                    try:
                        connection.rollback()
                    except:
                        pass
                logger.error(f"Error MySQL: {e}")
                raise
            finally:
                if connection:
                    try:
                        connection.close()
                    except:
                        pass

    def init_schema(self):
        """Inicializa el esquema de base de datos (solo una vez)"""
        if self._schema_initialized:
            logger.debug("Esquema ya inicializado, omitiendo...")
            return
            
        logger.info("Inicializando esquema MySQL...")
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Solo verificar tablas críticas sin modificarlas
                    logger.debug("Verificando tablas existentes...")
                    
                    # Verificar input_main
                    cursor.execute("SHOW TABLES LIKE 'input_main'")
                    if not cursor.fetchone():
                        logger.warning("⚠️ Tabla input_main no existe. Crear manualmente.")
                    else:
                        logger.debug("✓ Tabla input_main existe")
                    
                    # Verificar plan_main
                    cursor.execute("SHOW TABLES LIKE 'plan_main'")
                    if not cursor.fetchone():
                        logger.warning("⚠️ Tabla plan_main no existe. Crear manualmente.")
                    else:
                        logger.debug("✓ Tabla plan_main existe")
                    
                    # Verificar columnas críticas de forma no bloqueante
                    try:
                        cursor.execute("SHOW COLUMNS FROM input_main LIKE 'is_complete'")
                        if not cursor.fetchone():
                            logger.info("Agregando columna is_complete...")
                            cursor.execute("ALTER TABLE input_main ADD COLUMN is_complete TINYINT DEFAULT 1")
                    except Exception as e:
                        logger.debug(f"is_complete: {e}")
                    
                    try:
                        cursor.execute("SHOW COLUMNS FROM input_main LIKE 'raw_barcode'")
                        if not cursor.fetchone():
                            logger.info("Agregando columna raw_barcode...")
                            cursor.execute("ALTER TABLE input_main ADD COLUMN raw_barcode VARCHAR(128) NULL")
                    except Exception as e:
                        logger.debug(f"raw_barcode: {e}")
                    
                    try:
                        cursor.execute("SHOW COLUMNS FROM input_main LIKE 'raw_pair'")
                        if not cursor.fetchone():
                            logger.info("Agregando columna raw_pair...")
                            cursor.execute("ALTER TABLE input_main ADD COLUMN raw_pair VARCHAR(260) NULL")
                    except Exception as e:
                        logger.debug(f"raw_pair: {e}")
                    
                    try:
                        cursor.execute("SHOW COLUMNS FROM input_main LIKE 'scan_format'")
                        if not cursor.fetchone():
                            logger.info("Agregando columna scan_format...")
                            cursor.execute("ALTER TABLE input_main ADD COLUMN scan_format VARCHAR(16) NULL")
                    except Exception as e:
                        logger.debug(f"scan_format: {e}")
                    
                    try:
                        cursor.execute("SHOW COLUMNS FROM plan_main LIKE 'produced_count'")
                        if not cursor.fetchone():
                            logger.info("Agregando columna produced_count...")
                            cursor.execute("ALTER TABLE plan_main ADD COLUMN produced_count INT DEFAULT 0")
                    except Exception as e:
                        logger.debug(f"produced_count: {e}")
                    
                conn.commit()
            
            self._schema_initialized = True
            logger.info("✅ Esquema MySQL verificado correctamente")
            
        except Exception as e:
            logger.warning(f"⚠️ Error verificando esquema (no crítico): {e}")
            # No fallar por errores de schema, continuar
            self._schema_initialized = True

    def insert_scan(self, scan: ScanRecord) -> int:
        """Inserta un escaneo en MySQL"""
        sql = """
        INSERT INTO input_main 
        (ts, raw, tipo, fecha, lot_no, secuencia, estacion, nparte, modelo, cantidad, linea)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (
                        scan.ts, scan.raw, scan.tipo, scan.fecha,
                        scan.lote, scan.secuencia, scan.estacion,
                        scan.nparte, scan.modelo, scan.cantidad, scan.linea
                    ))
                    scan_id = cursor.lastrowid
                conn.commit()
            logger.info(f"✅ Escaneo insertado: ID {scan_id}")
            return scan_id
        except Exception as e:
            logger.error(f"❌ Error insertando escaneo: {e}")
            raise

    def get_last_scans(self, limit: int = 50) -> List[ScanRecord]:
        """Obtiene los últimos escaneos con límite más conservador"""
        # Reducir límite por defecto para mejor rendimiento
        limit = min(limit, 50)
        
        sql = """
     SELECT id, ts, raw, tipo, fecha, lot_no as lote, secuencia, estacion, 
               nparte, modelo, cantidad, linea
        FROM input_main 
        ORDER BY id DESC 
        LIMIT %s
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (limit,))
                    rows = cursor.fetchall()
            
            scans = []
            for row in rows:
                scans.append(ScanRecord(
                    id=row['id'],
                    ts=row['ts'],
                    raw=row['raw'],
                    tipo=row['tipo'],
                    fecha=row['fecha'],
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
            logger.error(f"❌ Error obteniendo escaneos: {e}")
            return []

    def get_today_totals(self) -> List[ResumenProduccion]:
        """Obtiene totales del día actual"""
        today = date.today()
        sql = """
        SELECT p.fecha, p.linea, p.nparte, p.modelo, p.cantidad_total, 
               p.uph_target, p.uph_real
        FROM produccion_main_input p
        WHERE p.fecha = %s
        ORDER BY p.linea, p.nparte
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (today,))
                    rows = cursor.fetchall()
            
            totals = []
            for row in rows:
                totals.append(ResumenProduccion(
                    fecha=row['fecha'],
                    linea=row['linea'],
                    nparte=row['nparte'],
                    modelo=row['modelo'],
                    cantidad_total=row['cantidad_total'],
                    uph_target=row['uph_target'],
                    uph_real=float(row['uph_real']) if row['uph_real'] else None
                ))
            return totals
        except Exception as e:
            logger.error(f"❌ Error obteniendo totales del día: {e}")
            return []

    def update_daily_production(self, fecha: date, linea: str, nparte: str, cantidad: int, modelo: str = None):
        """Actualiza la producción diaria con modelo y UPH desde tabla raw"""
        # Obtener modelo y UPH de la tabla raw
        if not modelo:
            modelo_ref = self.get_modelo_by_nparte(nparte)
            if modelo_ref:
                modelo = modelo_ref.modelo
                uph_target = modelo_ref.uph
            else:
                uph_target = None
        else:
            # Si ya tenemos modelo, obtener UPH
            modelo_ref = self.get_modelo_by_nparte(nparte)
            uph_target = modelo_ref.uph if modelo_ref else None
        
        sql = """
        INSERT INTO produccion_main_input (fecha, linea, nparte, modelo, cantidad_total, uph_target)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        modelo = VALUES(modelo),
        cantidad_total = cantidad_total + VALUES(cantidad_total),
        uph_target = VALUES(uph_target),
        updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (fecha, linea, nparte, modelo, cantidad, uph_target))
                conn.commit()
        except Exception as e:
            logger.error(f"❌ Error actualizando producción diaria: {e}")
            raise

    def insert_pair_scan(self, data: Dict[str, Any]):
        """Inserta un registro consolidado de un PAR (QR + BARCODE) en una sola fila.

        Campos esperados:
            ts, raw_qr, raw_barcode, fecha, lote, secuencia (del QR), estacion, nparte,
            modelo, cantidad (=1), linea, barcode_sequence
        Guarda scan_format='PAIR', is_complete=1 y raw_pair.
        """
        sql = (
            "INSERT INTO input_main (ts, raw, raw_barcode, raw_pair, tipo, fecha, lot_no, secuencia, estacion, nparte, modelo, cantidad, linea, scan_format, barcode_sequence, is_complete) "
            "VALUES (%(ts)s, %(raw_qr)s, %(raw_barcode)s, %(raw_pair)s, %(tipo)s, %(fecha)s, %(lote)s, %(secuencia)s, %(estacion)s, %(nparte)s, %(modelo)s, %(cantidad)s, %(linea)s, 'PAIR', %(barcode_sequence)s, 1)"
        )
        try:
            with self.get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(sql, data)
                    conn.commit()
                    logger.info(f"✅ Par consolidado insertado en MySQL (nparte={data.get('nparte')}, lote={data.get('lote')}, sec={data.get('secuencia')})")
                except Exception as e:
                    # Manejar duplicado (fila QR previa ya existe): actualizamos columnas para convertirla en PAR
                    if '1062' in str(e):
                        try:
                            upd_sql = (
                                "UPDATE input_main SET raw_barcode=%s, raw_pair=%s, scan_format='PAIR', "
                                "barcode_sequence=%s, is_complete=1 "
                                "WHERE lot_no=%s AND secuencia=%s AND estacion=%s AND nparte=%s"
                            )
                            with conn.cursor() as cursor:
                                cursor.execute(
                                    upd_sql,
                                    (
                                        data.get('raw_barcode'),
                                        data.get('raw_pair'),
                                        data.get('barcode_sequence'),
                                        data.get('lote'),
                                        data.get('secuencia'),
                                        data.get('estacion'),
                                        data.get('nparte'),
                                    )
                                )
                            conn.commit()
                            # ✅ Par actualizado a PAR consolidado correctamente (sin warning innecesario)
                        except Exception as ue:
                            logger.error(f"❌ Error actualizando fila existente a PAR: {ue}")
                            raise
                    else:
                        logger.error(f"❌ Error insertando par consolidado: {e}")
                        raise
        except Exception:
            raise

    def update_uph_real(self, fecha: date, linea: str, nparte: str, uph_real: float):
        sql = """
        UPDATE produccion_main_input
        SET uph_real = %s, updated_at = CURRENT_TIMESTAMP
        WHERE fecha=%s AND linea=%s AND nparte=%s
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (uph_real, fecha, linea, nparte))
                conn.commit()
        except Exception as e:
            logger.error(f"❌ Error actualizando uph_real: {e}")
            raise

    # -------- PLAN MAIN ---------
    def get_plan_for_line(self, linea: str, working_date: date) -> List[dict]:
        """Obtiene plan de producción para una línea y fecha desde plan_main."""
        sql = """
        SELECT id, lot_no, wo_code, po_code, working_date, line, model_code, part_no, project, process,
               plan_count, ct, uph, routing, status, COALESCE(produced_count,0) as produced_count,
               COALESCE(sequence,0) as sequence, started_at,
               planned_start, planned_end, effective_minutes
        FROM plan_main
        WHERE line = %s AND working_date = %s AND status <> 'CANCELLED'
        ORDER BY COALESCE(sequence,0), id
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (linea, working_date))
                    rows = cursor.fetchall()
            return rows or []
        except Exception as e:
            logger.error(f"❌ Error obteniendo plan para {linea}: {e}")
            return []

    # -------- PLAN IMD ---------
    def get_plan_for_line_imd(self, linea: str, working_date: date) -> List[dict]:
        sql = """
        SELECT id, lot_no, wo_code, po_code, working_date, line, model_code, part_no, project, process,
               plan_count, ct, uph, routing, status, COALESCE(produced_count,0) as produced_count,
               COALESCE(sequence,0) as sequence, started_at,
               planned_start, planned_end, effective_minutes
        FROM plan_imd
        WHERE line = %s AND working_date = %s AND status <> 'CANCELLED'
        ORDER BY COALESCE(sequence,0), id
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (linea, working_date))
                    rows = cursor.fetchall()
            return rows or []
        except Exception as e:
            logger.error(f"❌ Error obteniendo plan_imd para {linea}: {e}")
            return []

    def increment_plan_produced_imd(self, plan_id: int, delta: int = 1):
        sql = """
        UPDATE plan_imd
        SET produced_count = COALESCE(produced_count,0) + %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (delta, plan_id))
                conn.commit()
        except Exception as e:
            logger.error(f"❌ Error incrementando produced_count IMD plan_id={plan_id}: {e}")
            raise  # ← FIX: Re-lanzar la excepción para que el caller sepa que falló

    # -------- OUTPUT IMD ---------
    def insert_output_imd(self, data: Dict[str, Any]) -> int:
        sql = (
            "INSERT INTO output_imd (ts, raw, tipo, fecha, lot_no, secuencia, estacion, nparte, modelo, cantidad, line) "
            "VALUES (%(ts)s, %(raw)s, %(tipo)s, %(fecha)s, %(lote)s, %(secuencia)s, %(estacion)s, %(nparte)s, %(modelo)s, %(cantidad)s, %(linea)s)"
        )
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, data)
                    rid = cursor.lastrowid
                conn.commit()
            return rid
        except Exception as e:
            logger.error(f"❌ Error insertando output_imd: {e}")
            raise

    def update_daily_production_imd(self, fecha: date, linea: str, nparte: str, cantidad: int, modelo: str = None):
        # En modo IMD puedes optar por otra tabla de totales; aquí opcionalmente reutilizamos produccion_main_input o crear una específica.
        return self.update_daily_production(fecha, linea, nparte, cantidad, modelo)

    def increment_plan_produced(self, plan_id: int, delta: int = 1):
        """Incrementa produced_count en plan_main de manera atómica."""
        sql = """
        UPDATE plan_main
        SET produced_count = COALESCE(produced_count,0) + %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (delta, plan_id))
                conn.commit()
        except Exception as e:
            logger.error(f"❌ Error incrementando produced_count plan_id={plan_id}: {e}")
            raise  # ← FIX: Re-lanzar la excepción para que el caller sepa que falló

    # Métodos para modelos
    def insert_modelo(self, modelo: ModeloRef):
        """No disponible - tabla 'raw' es de solo lectura desde MES"""
        raise NotImplementedError("No se puede modificar tabla externa 'raw' - es de solo lectura")

    def list_modelos(self) -> List[ModeloRef]:
        """Lista todos los modelos desde tabla externa 'raw'"""
        sql = """
        SELECT part_no as nparte, model as modelo, 
               CAST(COALESCE(uph, '0') AS UNSIGNED) as uph, 
               c_t as ct, 1 as activo
        FROM raw 
        WHERE raw = 1
        ORDER BY part_no
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
            
            modelos = []
            for row in rows:
                modelos.append(ModeloRef(
                    nparte=row['nparte'],
                    modelo=row['modelo'],
                    uph=row['uph'],
                    ct=float(row['ct']) if row['ct'] else None,
                    activo=bool(row['activo'])
                ))
            return modelos
        except Exception as e:
            logger.error(f"❌ Error listando modelos: {e}")
            return []

    def get_modelo_by_nparte(self, nparte: str) -> Optional[ModeloRef]:
        """Obtiene un modelo desde tabla externa 'raw'"""
        sql = """
        SELECT part_no as nparte, project as modelo, 
               CAST(COALESCE(uph, '0') AS UNSIGNED) as uph, 
               c_t as ct, 1 as activo
        FROM raw 
        WHERE part_no = %s
        LIMIT 1
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (nparte,))
                    row = cursor.fetchone()
            
            if row:
                return ModeloRef(
                    nparte=row['nparte'],
                    modelo=row['modelo'],
                    uph=row['uph'],
                    ct=float(row['ct']) if row['ct'] else None,
                    activo=bool(row['activo'])
                )
            return None
        except Exception as e:
            logger.error(f"❌ Error obteniendo modelo {nparte}: {e}")
            return None

    def delete_modelo(self, nparte: str):
        """No disponible - tabla 'raw' es de solo lectura desde MES"""
        raise NotImplementedError("No se puede modificar tabla externa 'raw' - es de solo lectura")

    # Métodos de cola (para backup offline)
    def queue_scan(self, raw: str, linea: str = "M1"):
        """Cola ahora es caché local - no implementado en MySQL"""
        pass  # Queue local, no en MySQL

    def queue_size(self) -> int:
        """Retorna 0 - cola ahora es caché local en memoria"""
        return 0  # Queue local, no en MySQL

    def process_queue(self):
        """No implementado - queue es ahora caché local"""
        pass  # Queue local, no en MySQL

    def actualizar_estado_plan(self, plan_id: int, nuevo_estado: str) -> bool:
        """
        Actualiza el estado de un plan en la tabla MySQL plan_main usando su ID único
        
        Si no encuentra el plan_id, busca por nparte y línea (FALLBACK para IDs regenerados)

        Args:
            plan_id: ID único del plan
            nuevo_estado: Nuevo estado (EN PROGRESO, PAUSADO, TERMINADO)

        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Intentar actualizar con el plan_id original
                    sql = """
                    UPDATE plan_main
                    SET status = %s, updated_at = NOW()
                    WHERE id = %s
                    """
                    cursor.execute(sql, (nuevo_estado, plan_id))

                    if cursor.rowcount > 0:
                        conn.commit()
                        logger.info(f"Estado actualizado en MySQL: plan_id={plan_id} -> {nuevo_estado}")
                        return True

                    # FALLBACK: Buscar el nparte y línea del plan en SQLite
                    logger.warning(f"No se encontró el plan con id: {plan_id} en MySQL")
                    logger.info(f"🔄 FALLBACK: Buscando plan alternativo por nparte y línea...")
                    
                    # Obtener nparte y línea del plan local
                    import sqlite3
                    sqlite_path = "app/db.sqlite3"
                    sqlite_conn = sqlite3.connect(sqlite_path, timeout=5.0)
                    sqlite_cursor = sqlite_conn.cursor()
                    
                    sqlite_cursor.execute("""
                        SELECT nparte, linea FROM plan_local WHERE id = ?
                    """, (plan_id,))
                    result = sqlite_cursor.fetchone()
                    sqlite_conn.close()
                    
                    if not result:
                        logger.warning(f"⚠️ No se pudo obtener nparte/línea del plan {plan_id} en SQLite")
                        return False
                    
                    nparte, linea = result
                    logger.info(f"📋 Plan original: nparte={nparte}, linea={linea}")
                    
                    # Buscar plan activo con mismo nparte y línea en MySQL
                    cursor.execute("""
                        SELECT id FROM plan_main
                        WHERE nparte = %s AND linea = %s
                        AND status IN ('PENDIENTE', 'EN PROGRESO', 'PAUSADO')
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (nparte, linea))
                    
                    fallback_result = cursor.fetchone()
                    if not fallback_result:
                        logger.warning(f"⚠️ No se encontró plan alternativo para nparte={nparte}, linea={linea}")
                        return False
                    
                    fallback_id = fallback_result[0]
                    logger.info(f"✅ FALLBACK: Encontrado plan_id={fallback_id} para mismo nparte/línea")
                    
                    # Actualizar el plan encontrado
                    cursor.execute("""
                        UPDATE plan_main
                        SET status = %s, updated_at = NOW()
                        WHERE id = %s
                    """, (nuevo_estado, fallback_id))
                    
                    conn.commit()
                    logger.info(f"✅ Estado actualizado vía FALLBACK: plan_id={fallback_id} -> {nuevo_estado}")
                    return True

        except Exception as e:
            logger.error(f"❌ Error actualizando estado del plan {plan_id} en MySQL: {e}")
            return False

