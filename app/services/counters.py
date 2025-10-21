"""
Sistema de contadores en tiempo real para líneas de producción
Evita consultas constantes a MySQL manteniendo contadores en memoria
"""
import threading
from datetime import datetime, date
from typing import Dict, Optional
from collections import defaultdict

from ..config import settings
from ..db import get_db


class ProductionCounters:
    """Sistema de contadores en tiempo real por línea de producción"""
    
    def __init__(self):
        self._lock = threading.Lock()
        # Contadores por línea para hoy
        self._daily_counts: Dict[str, int] = defaultdict(int)
        # Contadores por línea para la sesión actual
        self._session_counts: Dict[str, int] = defaultdict(int)
        # Último escaneo por línea (para mostrar info adicional)
        self._last_scan: Dict[str, Dict] = {}
        # Fecha actual para detectar cambio de día
        self._current_date = date.today()
        # Flag para indicar si se inicializó desde BD
        self._initialized = False
        
    def initialize_from_db(self):
        """Inicializa contadores desde MySQL solo una vez al arranque con timeout"""
        if self._initialized:
            return
            
        try:
            db = get_db()
            today = date.today()
            
            # ✅ Timeout de 5 segundos para evitar bloqueos
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("Timeout inicializando contadores")
            
            # En Windows, usar threading como alternativa
            import threading
            timeout_occurred = [False]
            
            def init_with_timeout():
                try:
                    with db.get_connection() as conn:
                        with conn.cursor() as cursor:
                            # Obtener conteos de hoy por línea
                            sql = """
                            SELECT linea, COUNT(*) as count, 
                                   MAX(ts) as last_scan_ts,
                                   MAX(nparte) as last_nparte
                            FROM input_main 
                            WHERE DATE(fecha) = %s 
                            GROUP BY linea
                            """
                            cursor.execute(sql, (today,))
                            rows = cursor.fetchall()
                            
                            with self._lock:
                                self._daily_counts.clear()
                                self._last_scan.clear()
                                
                                for row in rows:
                                    linea = row['linea']
                                    count = row['count']
                                    self._daily_counts[linea] = count
                                    self._last_scan[linea] = {
                                        'timestamp': row['last_scan_ts'],
                                        'nparte': row['last_nparte']
                                    }
                                    
                                self._current_date = today
                                self._initialized = True
                except Exception as e:
                    if not timeout_occurred[0]:
                        print(f"Error en query de inicialización: {e}")
            
            # Ejecutar con timeout de 5 segundos
            thread = threading.Thread(target=init_with_timeout, daemon=True)
            thread.start()
            thread.join(timeout=5.0)
            
            if thread.is_alive():
                timeout_occurred[0] = True
                print("⚠️ Timeout inicializando contadores desde DB, usando valores por defecto")
                with self._lock:
                    self._initialized = True
            elif not self._initialized:
                # Error en la inicialización pero no timeout
                with self._lock:
                    self._initialized = True
                        
        except Exception as e:
            # En caso de error, continuar con contadores en 0
            print(f"❌ Error inicializando contadores: {e}")
            with self._lock:
                self._initialized = True
    
    def increment_line(self, linea: str, nparte: str, cantidad: int = 1) -> Dict[str, int]:
        """
        Incrementa contadores para una línea específica
        Retorna los contadores actualizados
        """
        # Verificar cambio de día
        today = date.today()
        
        with self._lock:
            if today != self._current_date:
                # Nuevo día, resetear contadores diarios
                self._daily_counts.clear()
                self._current_date = today
            
            # Incrementar contadores
            self._daily_counts[linea] += cantidad
            self._session_counts[linea] += cantidad
            
            # Actualizar último escaneo
            self._last_scan[linea] = {
                'timestamp': datetime.now(),
                'nparte': nparte
            }
            
            return {
                'daily': self._daily_counts[linea],
                'session': self._session_counts[linea]
            }
    
    def get_line_count(self, linea: str) -> Dict[str, int]:
        """Obtiene contadores para una línea específica"""
        with self._lock:
            return {
                'daily': self._daily_counts.get(linea, 0),
                'session': self._session_counts.get(linea, 0)
            }
    
    def get_all_counts(self) -> Dict[str, Dict]:
        """Obtiene todos los contadores organizados por línea"""
        with self._lock:
            result = {}
            all_lines = set(self._daily_counts.keys()) | set(self._session_counts.keys())
            
            for linea in all_lines:
                result[linea] = {
                    'daily': self._daily_counts.get(linea, 0),
                    'session': self._session_counts.get(linea, 0),
                    'last_scan': self._last_scan.get(linea, {})
                }
                
            return result
    
    def reset_session_counters(self):
        """Resetea solo los contadores de sesión"""
        with self._lock:
            self._session_counts.clear()
    
    def get_total_daily(self) -> int:
        """Obtiene el total de escaneos del día en todas las líneas"""
        with self._lock:
            return sum(self._daily_counts.values())
    
    def get_total_session(self) -> int:
        """Obtiene el total de escaneos de la sesión en todas las líneas"""
        with self._lock:
            return sum(self._session_counts.values())


# Instancia global del sistema de contadores
_counters_instance: Optional[ProductionCounters] = None
_counters_lock = threading.Lock()


def get_counters() -> ProductionCounters:
    """Obtiene la instancia global del sistema de contadores (singleton)"""
    global _counters_instance
    
    with _counters_lock:
        if _counters_instance is None:
            _counters_instance = ProductionCounters()
            # Inicializar desde BD en un hilo separado para no bloquear
            threading.Thread(target=_counters_instance.initialize_from_db, daemon=True).start()
        
        return _counters_instance


def reset_counters():
    """Resetea la instancia de contadores (útil para pruebas)"""
    global _counters_instance
    
    with _counters_lock:
        _counters_instance = None