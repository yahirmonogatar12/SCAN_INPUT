from dataclasses import dataclass
from typing import Optional
from datetime import datetime, date


@dataclass
class ParsedScan:
    raw: str
    tipo: str
    fecha_iso: str  # YYYY-MM-DD
    lote: str
    secuencia: int
    estacion: str
    nparte: str
    cantidad: int
    scan_format: str = "QR"  # "QR" o "BARCODE"
    barcode_sequence: Optional[int] = None  # Secuencial del barcode

    @property
    def secuencia_int(self) -> int:
        """Compatibilidad con código existente"""
        return self.secuencia


@dataclass
class ParsedBarcode:
    """Estructura específica para códigos de barras"""
    raw: str
    nparte: str
    isemm_data: str  # Los 2 dígitos ISEMM
    fecha_iso: str   # Convertido de DDMMYY a YYYY-MM-DD
    secuencia: int   # Secuencial único del barcode
    scan_format: str = "BARCODE"


@dataclass
class ModeloRef:
    nparte: str
    modelo: str
    uph: int
    ct: Optional[float]
    activo: bool


@dataclass
class ScanRecord:
    id: Optional[int]
    ts: datetime
    raw: str
    tipo: str
    fecha: date
    lote: str
    secuencia: int
    estacion: str
    nparte: str
    modelo: Optional[str]
    cantidad: int
    linea: str
    scan_format: str = "QR"  # "QR" o "BARCODE"
    barcode_sequence: Optional[int] = None  # Secuencial del barcode si aplica
    linked_scan_id: Optional[int] = None  # ID del escaneo vinculado


@dataclass
class ResumenProduccion:
    fecha: date
    linea: str
    nparte: Optional[str]
    modelo: Optional[str]
    cantidad_total: int
    uph_target: Optional[int]
    uph_real: Optional[float] = None


# Mantener DailyTotal para compatibilidad
@dataclass
class DailyTotal:
    fecha: str
    linea: str
    nparte: Optional[str]
    modelo: Optional[str]
    cantidad_total: int
    uph_target: Optional[int]
    uph_real: Optional[int] = None
