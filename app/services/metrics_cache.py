"""
Sistema de caché de métricas en SQLite para actualización asíncrona de cards
sin bloquear el front-end
"""
import sqlite3
import threading
import time
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class MetricsCacheManager:
    """
    Gestor de caché de métricas en SQLite para evitar congelamientos en el front-end.
    
    Estrategia:
    1. Worker background sincroniza métricas desde MySQL a SQLite cada N segundos
    2. UI lee métricas instantáneamente desde SQLite (sin bloqueos)
    3. Actualizaciones inmediatas al escanear se reflejan primero en SQLite
    """
    
    def __init__(self, sqlite_path: Path):
        self.sqlite_path = sqlite_path
        self._lock = threading.RLock()
        self._worker_thread = None
        self._stop_worker = threading.Event()
        self._update_interval = 3  # Actualizar cada 3 segundos
        
        # Línea activa actual (solo sincronizar esta)
        self._active_line: Optional[str] = None
        
        # Inicializar tabla de caché
        self._init_cache_table()
        
        logger.info("✅ MetricsCacheManager inicializado")
    
    def set_active_line(self, linea: str):
        """Establece la línea activa para sincronización"""
        with self._lock:
            if self._active_line != linea:
                self._active_line = linea
                logger.info(f"🎯 Línea activa cambiada a: {linea}")
    
    def get_active_line(self) -> Optional[str]:
        """Obtiene la línea activa actual"""
        with self._lock:
            return self._active_line
    
    def _init_cache_table(self):
        """Crea tabla de caché de métricas si no existe"""
        try:
            with sqlite3.connect(self.sqlite_path, timeout=10) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS metrics_cache (
                        linea TEXT NOT NULL,
                        fecha TEXT NOT NULL,
                        plan_total INTEGER DEFAULT 0,
                        plan_acumulado INTEGER DEFAULT 0,
                        produccion_real INTEGER DEFAULT 0,
                        eficiencia REAL DEFAULT 0.0,
                        uph REAL DEFAULT 0.0,
                        upph REAL DEFAULT 0.0,
                        num_personas INTEGER DEFAULT 0,
                        minutos_efectivos_total INTEGER DEFAULT 0,
                        minutos_efectivos_transcurridos INTEGER DEFAULT 0,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (linea, fecha)
                    )
                """)
                
                # Índice para consultas rápidas
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_metrics_linea_fecha 
                    ON metrics_cache(linea, fecha)
                """)
                
                conn.commit()
                logger.debug("✅ Tabla metrics_cache inicializada")
        except Exception as e:
            logger.error(f"❌ Error inicializando tabla metrics_cache: {e}")
    
    def update_metrics_instant(self, linea: str, fecha: str, metrics: Dict[str, Any]):
        """
        Actualización instantánea de métricas (llamado después de cada escaneo)
        Esta actualización es local y no bloquea
        """
        try:
            with self._lock:
                with sqlite3.connect(self.sqlite_path, timeout=10) as conn:
                    conn.execute("""
                        INSERT OR REPLACE INTO metrics_cache (
                            linea, fecha, plan_total, plan_acumulado, 
                            produccion_real, eficiencia, uph, upph,
                            num_personas, minutos_efectivos_total,
                            minutos_efectivos_transcurridos, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        linea, fecha,
                        metrics.get('plan_total', 0),
                        metrics.get('plan_acumulado', 0),
                        metrics.get('produccion_real', 0),
                        metrics.get('eficiencia', 0.0),
                        metrics.get('uph', 0.0),
                        metrics.get('upph', 0.0),
                        metrics.get('num_personas', 0),
                        metrics.get('minutos_efectivos_total', 0),
                        metrics.get('minutos_efectivos_transcurridos', 0),
                        datetime.now().isoformat()
                    ))
                    conn.commit()
                    
            logger.debug(f"📊 Métricas actualizadas en caché para {linea} - {fecha}")
        except Exception as e:
            logger.error(f"❌ Error actualizando métricas en caché: {e}")
    
    def get_metrics_from_cache(self, linea: str, fecha: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene métricas desde caché (ultra-rápido, sin bloqueos)
        """
        try:
            with sqlite3.connect(self.sqlite_path, timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT * FROM metrics_cache 
                    WHERE linea = ? AND fecha = ?
                """, (linea, fecha))
                
                row = cursor.fetchone()
                
                if row:
                    metrics = {
                        'plan_total': row['plan_total'],
                        'plan_acumulado': row['plan_acumulado'],
                        'produccion_real': row['produccion_real'],
                        'eficiencia': row['eficiencia'],
                        'uph': row['uph'],
                        'upph': row['upph'],
                        'num_personas': row['num_personas'],
                        'minutos_efectivos_total': row['minutos_efectivos_total'],
                        'minutos_efectivos_transcurridos': row['minutos_efectivos_transcurridos'],
                        'updated_at': row['updated_at']
                    }
                    
                    logger.debug(f"✅ Métricas leídas desde caché: {linea} - Eficiencia: {metrics['eficiencia']:.1f}%")
                    return metrics
                else:
                    logger.debug(f"⚠️ No hay métricas en caché para {linea} - {fecha}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Error leyendo métricas desde caché: {e}")
            return None
    
    def calculate_and_update_metrics(self, linea: str, fecha: str, plan_rows: list = None, num_personas: int = None):
        """
        Calcula métricas DIRECTAMENTE desde SQLite (plan_local)
        ✅ Ultra-rápido: Lee producción real directamente de la BD
        ✅ Sin cálculos complejos: Suma simple de plan_count y produced_count
        
        Args:
            linea: Línea de producción
            fecha: Fecha en formato ISO
            plan_rows: Filas del plan (opcional)
            num_personas: Número de personas (opcional, si no se provee usa valor por defecto)
        """
        try:
            # ✅ Leer datos directamente de SQLite
            with sqlite3.connect(self.sqlite_path, timeout=5) as conn:
                cursor = conn.cursor()
                
                # Obtener plan total y producción total de la línea
                cursor.execute("""
                    SELECT 
                        SUM(plan_count) as plan_total,
                        SUM(produced_count) as produccion_total
                    FROM plan_local
                    WHERE line = ?
                """, (linea,))
                
                result = cursor.fetchone()
                plan_total_linea = result[0] or 0
                produccion_acumulada = result[1] or 0
                
                logger.debug(f"📊 [CACHE] SQLite: {linea} - Plan={plan_total_linea}, Prod={produccion_acumulada}")
            
            # Calcular plan acumulado (por ahora igual al plan total)
            # TODO: Implementar cálculo con minutos efectivos si es necesario
            plan_acumulado = plan_total_linea
            
            # Calcular eficiencia
            if plan_acumulado > 0:
                eficiencia = (produccion_acumulada / plan_acumulado) * 100
            else:
                eficiencia = 0.0
            
            # Calcular UPH (última hora) - AHORA CUENTA PARES COMPLETOS
            uph = self._calculate_uph_from_db(linea, fecha)
            
            # Obtener número de personas
            if num_personas is None:
                # Intentar obtener desde configuración como fallback
                from ..config import settings
                num_personas = getattr(settings, 'NUM_PERSONAS_LINEA', 6)
            
            # Calcular UPPH
            upph = (uph / num_personas) if num_personas > 0 else 0.0
            
            # Crear diccionario de métricas
            metrics = {
                'plan_total': plan_total_linea,
                'plan_acumulado': plan_acumulado,
                'produccion_real': produccion_acumulada,
                'eficiencia': eficiencia,
                'uph': uph,
                'upph': upph,
                'num_personas': num_personas,
                'minutos_efectivos_total': 450,  # Placeholder
                'minutos_efectivos_transcurridos': 0  # Placeholder
            }
            
            # Actualizar caché
            self.update_metrics_instant(linea, fecha, metrics)
            
            logger.debug(f"✅ [CACHE] Métricas actualizadas: {linea} - Plan={plan_total_linea}, Prod={produccion_acumulada}, Efic={eficiencia:.1f}%, UPH={uph}, UPPH={upph:.2f}")
            
        except Exception as e:
            logger.error(f"❌ Error calculando métricas desde SQLite: {e}", exc_info=True)
    
    def _calculate_uph_from_db(self, linea: str, fecha: str) -> float:
        """
        Calcula UPH como velocidad proyectada (piezas por hora basado en tiempo transcurrido)
        UPH = (total_piezas_completas / segundos_transcurridos) * 3600
        Nunca retorna 0 si hay al menos 1 scan completo
        """
        try:
            with sqlite3.connect(self.sqlite_path, timeout=5) as conn:
                cursor = conn.cursor()
                
                # Obtener el primer y último scan del turno actual, más el total de piezas
                cursor.execute("""
                    SELECT 
                        COUNT(*)/2 as total_piezas,
                        MIN(ts) as primer_scan,
                        MAX(ts) as ultimo_scan
                    FROM scans_local
                    WHERE linea = ?
                    AND fecha = ?
                    AND is_complete = 1
                """, (linea, fecha))
                
                result = cursor.fetchone()
                
                if not result or not result[0] or result[0] == 0:
                    return 0.0
                
                total_piezas = float(result[0])
                primer_scan = result[1]
                ultimo_scan = result[2]
                
                if not primer_scan or not ultimo_scan:
                    return 0.0
                
                # Calcular segundos transcurridos desde el primer scan
                cursor.execute("""
                    SELECT (julianday(?) - julianday(?)) * 86400 as segundos
                """, (ultimo_scan, primer_scan))
                
                segundos_result = cursor.fetchone()
                segundos_transcurridos = float(segundos_result[0]) if segundos_result else 0.0
                
                # Usar mínimo de 60 segundos (1 minuto) para evitar proyecciones irreales
                # Ejemplo: 1 pieza en 5 segundos NO debe proyectar 720 UPH
                SEGUNDOS_MINIMOS = 60.0
                if segundos_transcurridos < SEGUNDOS_MINIMOS:
                    segundos_transcurridos = SEGUNDOS_MINIMOS
                
                # UPH = (piezas / segundos) * 3600 segundos/hora
                uph_proyectado = (total_piezas / segundos_transcurridos) * 3600.0
                
                logger.debug(f"📊 UPH calculado: {total_piezas} piezas en {segundos_transcurridos:.1f}s = {uph_proyectado:.1f} UPH")
                
                return uph_proyectado
                
        except Exception as e:
            logger.error(f"❌ Error calculando UPH: {e}")
            return 0.0
    
    def start_background_sync(self, dual_db_instance):
        """
        Inicia worker en background que sincroniza métricas periódicamente
        """
        if self._worker_thread and self._worker_thread.is_alive():
            logger.warning("⚠️ Worker de métricas ya está corriendo")
            return
        
        self._stop_worker.clear()
        self._worker_thread = threading.Thread(
            target=self._metrics_sync_worker,
            args=(dual_db_instance,),
            daemon=True,
            name="MetricsSyncWorker"
        )
        self._worker_thread.start()
        logger.info("🚀 Worker de sincronización de métricas iniciado")
    
    def _metrics_sync_worker(self, dual_db_instance):
        """Worker que actualiza métricas en background"""
        logger.info("🔄 Metrics sync worker iniciado")
        
        while not self._stop_worker.is_set():
            try:
                # Obtener línea activa
                linea_activa = self.get_active_line()
                
                # Solo sincronizar si hay una línea activa
                if not linea_activa:
                    logger.debug("⏸️ Sin línea activa, esperando...")
                    time.sleep(self._update_interval)
                    continue
                
                # Obtener fecha actual
                from datetime import date
                fecha_hoy = date.today().isoformat()
                
                # Sincronizar SOLO la línea activa
                try:
                    # Obtener plan_rows desde dual_db
                    plan_rows = dual_db_instance.get_plan_for_line_local(linea_activa)
                    
                    if plan_rows:
                        # Calcular y actualizar métricas
                        self.calculate_and_update_metrics(linea_activa, fecha_hoy, plan_rows)
                        logger.debug(f"✅ Métricas sincronizadas para {linea_activa}")
                    else:
                        logger.debug(f"⚠️ Sin plan para {linea_activa}")
                    
                except Exception as e:
                    logger.error(f"❌ Error sincronizando métricas para {linea_activa}: {e}")
                
                # Esperar antes del próximo ciclo
                time.sleep(self._update_interval)
                
            except Exception as e:
                logger.error(f"❌ Error en metrics sync worker: {e}")
                time.sleep(5)  # Esperar más tiempo si hay error
        
        logger.info("🛑 Metrics sync worker detenido")
    
    def stop_background_sync(self):
        """Detiene el worker de sincronización"""
        self._stop_worker.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        logger.info("🛑 Worker de métricas detenido")
    
    def cleanup_old_cache(self, days_to_keep: int = 7):
        """Limpia caché de métricas antiguas"""
        try:
            with sqlite3.connect(self.sqlite_path, timeout=10) as conn:
                conn.execute("""
                    DELETE FROM metrics_cache 
                    WHERE datetime(updated_at) < datetime('now', '-' || ? || ' days')
                """, (days_to_keep,))
                conn.commit()
                logger.info(f"🧹 Caché de métricas limpiado (>{days_to_keep} días)")
        except Exception as e:
            logger.error(f"❌ Error limpiando caché de métricas: {e}")


# Instancia global (se inicializa desde dual_db.py)
metrics_cache: Optional[MetricsCacheManager] = None


def init_metrics_cache(sqlite_path: Path) -> MetricsCacheManager:
    """Inicializa el gestor de caché de métricas"""
    global metrics_cache
    metrics_cache = MetricsCacheManager(sqlite_path)
    return metrics_cache


def get_metrics_cache() -> Optional[MetricsCacheManager]:
    """Obtiene la instancia global del caché de métricas"""
    return metrics_cache
