"""
Sistema de caché local para queue_scans
Reemplaza la tabla MySQL queue_scans con almacenamiento local en memoria/archivo
"""
import json
import threading
import time
import logging
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import List, Dict, Any
from pathlib import Path

from ..config import ROOT_DIR, settings

logger = logging.getLogger(__name__)


class LocalQueueCache:
    """Caché local para escaneos pendientes cuando MySQL no esté disponible"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._queue: List[Dict[str, Any]] = []
        self._cache_file = ROOT_DIR / "data" / "queue_cache.json"
        self._next_id = 1
        self._load_from_file()
    
    def _load_from_file(self):
        """Carga la cola desde archivo JSON"""
        try:
            if self._cache_file.exists():
                with open(self._cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self._queue = data.get('queue', [])
                    self._next_id = data.get('next_id', 1)
        except Exception as e:
            print(f"Error cargando cache de cola: {e}")
            self._queue = []
            self._next_id = 1
    
    def _save_to_file(self):
        """Guarda la cola en archivo JSON"""
        try:
            self._cache_file.parent.mkdir(exist_ok=True)
            data = {
                'queue': self._queue,
                'next_id': self._next_id
            }
            with open(self._cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            print(f"Error guardando cache de cola: {e}")
    
    def add_scan(self, raw: str, linea: str = "M1") -> int:
        """Añade un escaneo a la cola local"""
        with self._lock:
            scan_item = {
                'id': self._next_id,
                'raw': raw,
                'linea': linea,
                'ts': datetime.now().isoformat(),
                'processed': False
            }
            self._queue.append(scan_item)
            self._next_id += 1
            self._save_to_file()
            return scan_item['id']
    
    def get_pending_count(self) -> int:
        """Retorna cantidad de escaneos pendientes"""
        with self._lock:
            return len([item for item in self._queue if not item['processed']])
    
    def get_pending_scans(self) -> List[Dict[str, Any]]:
        """Obtiene todos los escaneos pendientes"""
        with self._lock:
            return [item for item in self._queue if not item['processed']]
    
    def mark_processed(self, scan_id: int) -> bool:
        """Marca un escaneo como procesado"""
        with self._lock:
            for item in self._queue:
                if item['id'] == scan_id:
                    item['processed'] = True
                    self._save_to_file()
                    return True
            return False
    
    def clear_processed(self):
        """Elimina escaneos ya procesados del caché"""
        with self._lock:
            self._queue = [item for item in self._queue if not item['processed']]
            self._save_to_file()
    
    def clear_all(self):
        """Limpia toda la cola y reinicia worker"""
        with self._lock:
            self._queue = []
            self._next_id = 1
            self._save_to_file()
    
    def get_total_count(self) -> int:
        """Retorna el total de elementos en la cola"""
        with self._lock:
            return len(self._queue)
    
    def _start_worker(self):
        """Inicia el worker automático para procesar cola pendiente"""
        if not self._worker_running:
            self._worker_running = True
            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self._worker_thread.start()
            logger.info("Worker automático de caché local iniciado")
    
    def _worker_loop(self):
        """Loop principal del worker para procesar elementos pendientes"""
        while self._worker_running:
            try:
                processed = self._process_pending_batch()
                if processed > 0:
                    logger.info(f"Worker procesó {processed} elementos del caché local")
                
                # Dormir entre 2-5 segundos dependiendo de la carga
                pending_count = self.get_pending_count()
                if pending_count > 50:
                    time.sleep(2)  # Procesar más rápido si hay muchos pendientes
                elif pending_count > 10:
                    time.sleep(3)  # Velocidad media
                else:
                    time.sleep(5)  # Procesar más lento si hay pocos
                    
            except Exception as e:
                logger.error(f"Error en worker del caché local: {e}")
                time.sleep(10)  # Esperar más tiempo en caso de error
    
    def _process_pending_batch(self, batch_size: int = 10) -> int:
        """Procesa un lote de elementos pendientes y los envía a MySQL"""
        try:
            from ..db import get_db
            from ..services.parser import parse_scan
            from ..models.entities import ScanRecord
            
            # ✅ Verificar conexión MySQL ANTES de procesar
            db = get_db()
            try:
                with db.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
            except Exception as e:
                logger.warning(f"❌ MySQL no disponible, manteniendo {self.get_pending_count()} elementos en caché: {e}")
                return 0  # No procesar si MySQL no está disponible
            
            processed_count = 0
            
            # Obtener lote de elementos pendientes
            pending_items = []
            with self._lock:
                for item in self._queue[:batch_size]:
                    if not item['processed']:
                        pending_items.append(item)
            
            # Procesar cada elemento
            for item in pending_items:
                try:
                    # Parsear el escaneo
                    parsed = parse_scan(item['raw'])
                    
                    # Obtener modelo
                    modelo = None
                    mref = db.get_modelo_by_nparte(parsed.nparte)
                    if mref and mref.activo:
                        modelo = mref.modelo
                    
                    # Crear record de escaneo
                    scan_record = ScanRecord(
                        id=None,
                        ts=datetime.now(ZoneInfo(settings.TZ)),
                        raw=item['raw'],
                        tipo=parsed.tipo,
                        fecha=date.today(),
                        lote=parsed.lote,
                        secuencia=parsed.secuencia,
                        estacion=parsed.estacion,
                        nparte=parsed.nparte,
                        modelo=modelo,
                        cantidad=parsed.cantidad,
                        linea=item['linea']
                    )
                    
                    # Insertar en MySQL
                    scan_id = db.insert_scan(scan_record)
                    
                    # Actualizar producción diaria
                    db.update_daily_production(
                        fecha=scan_record.fecha,
                        linea=scan_record.linea,  # Usar linea del item
                        nparte=scan_record.nparte,
                        cantidad=scan_record.cantidad
                    )
                    
                    # Marcar como procesado
                    self.mark_processed(item['id'])
                    processed_count += 1
                    
                    # Elemento procesado exitosamente
                    
                except Exception as e:
                    logger.error(f"Error procesando elemento {item['id']}: {e}")
                    # No marcar como procesado para reintentar después
            
            # Limpiar elementos procesados periódicamente
            if processed_count > 0:
                self.clear_processed()
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Error en _process_pending_batch: {e}")
            return 0
    
    def stop_worker(self):
        """Detiene el worker automático"""
        self._worker_running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        logger.info("Worker automático de caché local detenido")


# Instancia global del caché local
_local_queue_instance = None
_local_queue_lock = threading.Lock()


def get_local_queue() -> LocalQueueCache:
    """Obtiene la instancia global del caché local (singleton)"""
    global _local_queue_instance
    
    with _local_queue_lock:
        if _local_queue_instance is None:
            _local_queue_instance = LocalQueueCache()
        
        return _local_queue_instance


def reset_local_queue():
    """Resetea la instancia del caché local (útil para pruebas)"""
    global _local_queue_instance
    
    with _local_queue_lock:
        _local_queue_instance = None