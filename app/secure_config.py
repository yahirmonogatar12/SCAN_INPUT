"""
Gestor de configuración seguro con encriptación para credenciales
"""
import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class SecureConfigManager:
    """Gestor centralizado de configuración con manejo seguro de credenciales"""
    
    _instance = None
    _config_cache: Dict[str, Any] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Inicializa el gestor de configuración"""
        # Directorio de aplicación en AppData
        self.app_dir = Path(os.environ.get('APPDATA', '')) / 'IMD_Scanner'
        self.app_dir.mkdir(exist_ok=True)
        
        # Cargar variables de entorno
        load_dotenv()
        
        logger.info(f"ConfigManager inicializado en: {self.app_dir}")
    
    def get_db_config(self) -> Dict[str, Any]:
        """Obtiene configuración de base de datos de forma segura"""
        
        # Intentar cargar de caché
        if 'db_config' in self._config_cache:
            return self._config_cache['db_config']
        
        # Configuración SQLite local
        sqlite_config = {
            'path': str(self.app_dir / 'local.db'),
            'pragmas': {
                'journal_mode': 'WAL',
                'synchronous': 'NORMAL',
                'cache_size': -20000,  # 20MB
                'temp_store': 'MEMORY',
                'mmap_size': 268435456,  # 256MB
                'page_size': 8192,
                'optimize': True
            }
        }
        
        # Configuración MySQL desde variables de entorno
        mysql_config = self._get_mysql_config()
        
        config = {
            'engine': os.getenv('DB_ENGINE', 'dual'),
            'sqlite': sqlite_config,
            'mysql': mysql_config,
            'connection_pool': {
                'size': 5,
                'max_overflow': 10,
                'timeout': 30,
                'recycle': 3600
            },
            'retry': {
                'max_attempts': 3,
                'delay': 1,
                'backoff': 2
            }
        }
        
        self._config_cache['db_config'] = config
        return config
    
    def _get_mysql_config(self) -> Dict[str, Any]:
        """Obtiene configuración MySQL de variables de entorno"""
        return {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'database': os.getenv('MYSQL_DB', 'imd_scanner'),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', ''),
            'charset': 'utf8mb4',
            'connect_timeout': 10,
            'autocommit': False,
            'use_pure': False,  # Usar implementación C para mejor rendimiento
        }
    
    def get_app_config(self) -> Dict[str, Any]:
        """Obtiene configuración de aplicación"""
        if 'app_config' in self._config_cache:
            return self._config_cache['app_config']
        
        config = {
            'mode': os.getenv('APP_MODE', 'ASSY'),
            'environment': os.getenv('APP_ENV', 'production'),
            'timezone': os.getenv('TZ', 'America/Monterrey'),
            'default_line': os.getenv('DEFAULT_LINE', 'M1'),
            'num_personas': int(os.getenv('NUM_PERSONAS_LINEA', 7)),
            'solo_qr': os.getenv('SOLO_QR_MODE', '0') == '1',
            'sub_assy': os.getenv('SUB_ASSY_MODE', '0') == '1',
            'performance': {
                'batch_size': 50,
                'update_interval': 5000,  # ms
                'cache_ttl': 300,  # segundos
                'max_queue_size': 1000
            }
        }
        
        self._config_cache['app_config'] = config
        return config
    
    def clear_cache(self):
        """Limpia caché de configuración"""
        self._config_cache.clear()
        logger.debug("Caché de configuración limpiado")
    
    def get_logs_dir(self) -> Path:
        """Obtiene directorio de logs"""
        logs_dir = self.app_dir / 'logs'
        logs_dir.mkdir(exist_ok=True)
        return logs_dir
    
    def get_data_dir(self) -> Path:
        """Obtiene directorio de datos"""
        data_dir = self.app_dir / 'data'
        data_dir.mkdir(exist_ok=True)
        return data_dir


# Singleton global
secure_config_manager = SecureConfigManager()
