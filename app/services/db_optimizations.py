"""
Optimizaciones adicionales para el sistema dual DB
Este módulo proporciona decoradores y utilidades para mejorar el rendimiento
"""
import time
import functools
import logging
from typing import Any, Callable, Dict, Optional
from datetime import datetime, timedelta
import threading

logger = logging.getLogger(__name__)


class CacheManager:
    """Gestor de caché en memoria con TTL"""
    
    def __init__(self, ttl: int = 300):
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._timestamps: Dict[str, float] = {}
        self._lock = threading.RLock()
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }
    
    def get(self, key: str) -> Optional[Any]:
        """Obtiene valor de caché si es válido"""
        with self._lock:
            if key not in self._cache:
                self._stats['misses'] += 1
                return None
            
            # Verificar TTL
            if time.time() - self._timestamps[key] > self.ttl:
                self.invalidate(key)
                self._stats['misses'] += 1
                self._stats['evictions'] += 1
                return None
            
            self._stats['hits'] += 1
            return self._cache[key]
    
    def set(self, key: str, value: Any) -> None:
        """Almacena valor en caché"""
        with self._lock:
            self._cache[key] = value
            self._timestamps[key] = time.time()
    
    def invalidate(self, key: str) -> None:
        """Invalida entrada específica"""
        with self._lock:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
    
    def invalidate_pattern(self, pattern: str) -> None:
        """Invalida todas las claves que coincidan con el patrón"""
        with self._lock:
            keys_to_remove = [k for k in self._cache if pattern in k]
            for key in keys_to_remove:
                self._cache.pop(key, None)
                self._timestamps.pop(key, None)
    
    def clear(self) -> None:
        """Limpia toda la caché"""
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()
    
    def get_stats(self) -> Dict[str, int]:
        """Obtiene estadísticas de caché"""
        with self._lock:
            total = self._stats['hits'] + self._stats['misses']
            hit_rate = (self._stats['hits'] / total * 100) if total > 0 else 0
            return {
                **self._stats,
                'size': len(self._cache),
                'hit_rate': round(hit_rate, 2)
            }


def with_retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorador para reintentar operaciones que fallan
    
    Args:
        max_attempts: Número máximo de intentos
        delay: Tiempo de espera inicial entre intentos (segundos)
        backoff: Factor de multiplicación para el delay en cada intento
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Intento {attempt + 1}/{max_attempts} falló para {func.__name__}: {e}"
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"Todos los intentos fallaron para {func.__name__}: {e}"
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


def with_timeout(seconds: float):
    """
    Decorador para establecer timeout en operaciones
    Nota: Requiere que la función sea compatible con threading
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = [None]
            exception = [None]
            
            def target():
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    exception[0] = e
            
            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            thread.join(seconds)
            
            if thread.is_alive():
                logger.warning(f"Timeout de {seconds}s alcanzado para {func.__name__}")
                raise TimeoutError(f"La operación {func.__name__} excedió {seconds} segundos")
            
            if exception[0]:
                raise exception[0]
            
            return result[0]
        
        return wrapper
    return decorator


def measure_time(func: Callable) -> Callable:
    """Decorador para medir tiempo de ejecución"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            elapsed = time.perf_counter() - start
            if elapsed > 1.0:  # Solo loguear si tarda más de 1 segundo
                logger.info(f"{func.__name__} tomó {elapsed:.3f}s")
    
    return wrapper


class ConnectionPool:
    """Pool simple de conexiones para MySQL"""
    
    def __init__(self, create_connection: Callable, size: int = 5):
        self.create_connection = create_connection
        self.size = size
        self._pool = []
        self._lock = threading.Lock()
        self._stats = {
            'created': 0,
            'reused': 0,
            'closed': 0
        }
    
    def get_connection(self):
        """Obtiene conexión del pool o crea una nueva"""
        with self._lock:
            # Intentar reusar conexión existente
            while self._pool:
                conn = self._pool.pop()
                try:
                    # Verificar que la conexión está activa
                    if conn.is_connected():
                        self._stats['reused'] += 1
                        return conn
                    else:
                        conn.close()
                        self._stats['closed'] += 1
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    self._stats['closed'] += 1
            
            # Crear nueva conexión
            conn = self.create_connection()
            self._stats['created'] += 1
            return conn
    
    def return_connection(self, conn):
        """Devuelve conexión al pool"""
        with self._lock:
            if len(self._pool) < self.size:
                try:
                    if conn.is_connected():
                        self._pool.append(conn)
                        return
                except Exception:
                    pass
            
            # Pool lleno o conexión inválida, cerrarla
            try:
                conn.close()
                self._stats['closed'] += 1
            except Exception:
                pass
    
    def close_all(self):
        """Cierra todas las conexiones del pool"""
        with self._lock:
            while self._pool:
                conn = self._pool.pop()
                try:
                    conn.close()
                    self._stats['closed'] += 1
                except Exception:
                    pass
    
    def get_stats(self) -> Dict[str, int]:
        """Obtiene estadísticas del pool"""
        with self._lock:
            return {
                **self._stats,
                'pool_size': len(self._pool)
            }


class BatchProcessor:
    """Procesador de operaciones en lotes para mejor rendimiento"""
    
    def __init__(self, batch_size: int = 50, flush_interval: float = 5.0):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._batch = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._stats = {
            'items_added': 0,
            'batches_processed': 0,
            'items_processed': 0
        }
    
    def add(self, item: Any) -> bool:
        """
        Agrega item al lote
        Retorna True si se procesó el lote
        """
        with self._lock:
            self._batch.append(item)
            self._stats['items_added'] += 1
            
            # Verificar si es momento de procesar
            should_flush = (
                len(self._batch) >= self.batch_size or
                time.time() - self._last_flush >= self.flush_interval
            )
            
            return should_flush
    
    def get_batch(self) -> list:
        """Obtiene y limpia el lote actual"""
        with self._lock:
            batch = self._batch[:]
            self._batch.clear()
            self._last_flush = time.time()
            
            if batch:
                self._stats['batches_processed'] += 1
                self._stats['items_processed'] += len(batch)
            
            return batch
    
    def get_stats(self) -> Dict[str, int]:
        """Obtiene estadísticas del procesador"""
        with self._lock:
            avg_batch_size = 0
            if self._stats['batches_processed'] > 0:
                avg_batch_size = round(
                    self._stats['items_processed'] / self._stats['batches_processed'],
                    2
                )
            
            return {
                **self._stats,
                'pending': len(self._batch),
                'avg_batch_size': avg_batch_size
            }


# Instancias globales para usar en dual_db
query_cache = CacheManager(ttl=300)  # 5 minutos
metrics_cache = CacheManager(ttl=60)  # 1 minuto
