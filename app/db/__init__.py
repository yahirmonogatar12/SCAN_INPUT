from typing import Protocol, List, Optional
from ..models import ModeloRef, ScanRecord, ResumenProduccion
from ..config import settings


class DB(Protocol):
    # Modelos
    def get_modelo_by_nparte(self, nparte: str) -> Optional[ModeloRef]: ...
    def insert_modelo(self, modelo: ModeloRef) -> None: ...
    def list_modelos(self) -> List[ModeloRef]: ...
    def delete_modelo(self, nparte: str) -> None: ...

    # Escaneos
    def insert_scan(self, scan: ScanRecord) -> int: ...
    def get_last_scans(self, limit: int = 100) -> List[ScanRecord]: ...
    def get_today_totals(self) -> List[ResumenProduccion]: ...
    def queue_size(self) -> int: ...
    
    # Control de planes
    def actualizar_estado_plan(self, part_no: str, nuevo_estado: str) -> bool: ...
    
    # Esquema
    def init_schema(self) -> None: ...


# Instancia única de base de datos (singleton)
_db_instance = None

def get_db() -> DB:
    """Retorna la instancia de base de datos configurada (singleton)"""
    global _db_instance
    
    if _db_instance is None:
        if settings.DB_ENGINE == "mysql":
            from .mysql_db import MySQLDatabase
            _db_instance = MySQLDatabase()
            _db_instance.init_schema()  # Solo una vez al crear la instancia
        else:
            # Fallback a SQLite si se necesita
            from .sqlite_db import SQLiteDB
            _db_instance = SQLiteDB(sqlite_path=settings.SQLITE_PATH)
    
    return _db_instance
