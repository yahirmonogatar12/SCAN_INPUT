import re
from typing import Match, Union
from datetime import datetime
from ..models import ParsedScan, ParsedBarcode


# ========== REGEX PARA FORMATO QR (ESPAÑOL - DUAL) ==========
# Soporta AMBOS formatos de separador:
# 1. FORMATO NUEVO (apóstrofe + ñ): I20251001'0004'00005ñMAINñEBR33105305ñ1ñ
# 2. FORMATO ANTIGUO (guión + punto y coma): I20250226-0019-0224;MAIN;EBR24212304;1;
#
# Ejemplos válidos:
#  - I20251001'0004'00005ñMAINñEBR33105305ñ1ñ   (NUEVO - español con ñ)
#  - I20250226-0013-00002;MAIN;EBR41039117;1;    (ANTIGUO - punto y coma)
#  - II20251024-0025-0596;MAIN;EBR30299363;1;    (ANTIGUO - tipo "II")
#  - I20250922-0032-0441;MAIN;EBR41039119;1;     (secuencia de 4 dígitos)
#  - I20250924-0030-0511;DISPLAY TB;EBR85856023;1; (estación con espacio)

# Pattern para formato NUEVO con ñ (acepta minúscula y mayúscula)
# Tipo: Uno o más caracteres (soporta I, II, III, etc.)
QR_PATTERN_NEW = re.compile(
    r"^(?P<tipo>[A-Z]+)(?P<fecha>\d{8})'(?P<lote_num>\d{4})'(?P<secuencia>\d{4,5})[ñÑ]"
    r"(?P<estacion>[A-ZÑ0-9_ ]+)[ñÑ](?P<nparte>[A-Z0-9]+)[ñÑ](?P<cantidad>\d+)[ñÑ]$"
)

# Pattern para formato ANTIGUO con ;
# Tipo: Uno o más caracteres (soporta I, II, III, etc.)
QR_PATTERN_OLD = re.compile(
    r"^(?P<tipo>[A-Z]+)(?P<fecha>\d{8})-(?P<lote_num>\d{4})-(?P<secuencia>\d{4,5});"
    r"(?P<estacion>[A-Z0-9_ ]+);(?P<nparte>[A-Z0-9]+);(?P<cantidad>\d+);$"
)

# Regex para formato Barcode: EBR41039117922509201292
# Estructura: [NPARTE_VARIABLE][ISO_2_DIGITS][DDMMYY_6_DIGITS][SECUENCIA_4_DIGITS]
# Longitud total: Variable N°Parte + 2 + 6 + 4 = N°Parte + 12
BARCODE_PATTERN = re.compile(
    r"^(?P<nparte>[A-Z0-9]+?)(?P<iso>\d{2})(?P<fecha>\d{6})(?P<secuencia>\d{4})$"
)


def is_complete_qr(raw: str) -> bool:
    """Verifica si el texto es un QR completo (NUEVO con ñ o ANTIGUO con ;)
    
    Esta función es para detectar cuando un QR ha sido escaneado completamente,
    sin importar el formato. Reemplaza las verificaciones de semicolons.
    
    Args:
        raw: Texto a verificar
        
    Returns:
        True si es un QR completo (nuevo o antiguo), False en caso contrario
    """
    raw = (raw or "").strip()
    
    # Formato NUEVO: Termina en ñ y tiene patrón completo
    if QR_PATTERN_NEW.match(raw):
        return True
    
    # Formato ANTIGUO: Termina en ; y tiene patrón completo  
    if QR_PATTERN_OLD.match(raw):
        return True
        
    return False


def detect_scan_format(raw: str) -> str:
    """Detecta el formato del escaneo (soporta QR nuevo y antiguo)"""
    raw = (raw or "").strip()
    
    # DEBUG: Logging para diagnóstico
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"🔍 Detectando formato de: '{raw}' (longitud: {len(raw)})")    
    
    # Primero verificar QR NUEVO (con ñ)
    if QR_PATTERN_NEW.match(raw):
        logger.info("✅ Detectado como QR (formato NUEVO con ñ)")
        return "QR"
    
    # Luego verificar QR ANTIGUO (con ;)
    if QR_PATTERN_OLD.match(raw):
        logger.info("✅ Detectado como QR (formato ANTIGUO con ;)")
        return "QR"
    
    # Verificar Barcode por estructura y longitud
    # Debe tener al menos 13 chars (1 N°parte + 12 sufijo) y ser solo alfanumérico
    # Verificación de formato BARCODE (longitud >= 13, alfanumérico, últimos 12 dígitos)
    
    # Detección más precisa para casos especiales como EBR41039117922509201294
    # Si el texto empieza con 'EBR' y tiene sufijo de 12 dígitos, es casi seguro un BARCODE
    if len(raw) >= 15 and raw.startswith('EBR') and raw[-12:].isdigit():
        logger.info("✅ Detectado como BARCODE (formato EBR)")
        return "BARCODE"
    
    if (len(raw) >= 13 and 
        raw.replace('_', '').replace('-', '').isalnum() and 
        raw[-12:].isdigit()):  # Últimos 12 deben ser dígitos
        try:
            # Intentar parsear para validar estructura
            sufijo = raw[-12:]
            fecha_raw = sufijo[2:8]
            logger.info(f"🔍 Analizando sufijo: '{sufijo}', fecha_raw: '{fecha_raw}'")
            # Validar que la fecha tenga sentido (día 01-31, mes 01-12)
            day, month = int(fecha_raw[:2]), int(fecha_raw[2:4])
            logger.info(f"🔍 Validando fecha: día={day}, mes={month}")
            if 1 <= day <= 31 and 1 <= month <= 12:
                logger.info("✅ Detectado como BARCODE")
                return "BARCODE"
            else:
                logger.warning(f"❌ Fecha inválida en BARCODE: día={day}, mes={month}")
        except (ValueError, IndexError) as e:
            logger.warning(f"❌ Error parseando BARCODE: {e}")
    
    logger.warning(f"❓ Formato DESCONOCIDO: '{raw}'")
    return "UNKNOWN"


def parse_qr_scan(raw: str) -> ParsedScan:
    """Parsea formato QR (soporta NUEVO con ñ y ANTIGUO con ;)"""
    
    # Intentar primero con formato NUEVO (ñ)
    m: Match[str] | None = QR_PATTERN_NEW.match(raw)
    
    # Si no coincide, intentar con formato ANTIGUO (;)
    if not m:
        m = QR_PATTERN_OLD.match(raw)
    
    # Si ninguno coincide, error
    if not m:
        raise ValueError(f"Formato QR inválido: '{raw}'")

    tipo = m.group("tipo")
    fecha_raw = m.group("fecha")  # YYYYMMDD
    fecha_iso = f"{fecha_raw[0:4]}-{fecha_raw[4:6]}-{fecha_raw[6:8]}"
    lote_num = m.group("lote_num")
    lote = f"{tipo}{fecha_raw}-{lote_num}"
    secuencia = int(m.group("secuencia"))
    estacion = m.group("estacion")
    nparte = m.group("nparte")
    cantidad = int(m.group("cantidad"))

    return ParsedScan(
        raw=raw,
        tipo=tipo,
        fecha_iso=fecha_iso,
        lote=lote,
        secuencia=secuencia,
        estacion=estacion,
        nparte=nparte,
        cantidad=cantidad,
        scan_format="QR"
    )



def parse_barcode_scan(raw: str) -> ParsedBarcode:
    """Parsea formato Barcode nuevo: EBR41039117922509201292
    Estructura: [NPARTE_VARIABLE][ISO_2][DDMMYY_6][SECUENCIA_4]
    Ejemplo: EBR41039117 + 92 + 250920 + 1292"""
    
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"🔍 Parseando BARCODE: '{raw}'")
    
    # Para N° parte variable, usamos enfoque más inteligente
    # Sabemos que los últimos 12 caracteres son: ISO(2) + Fecha(6) + Secuencia(4)
    if len(raw) < 13:  # Mínimo: 1 char N°parte + 12 sufijo
        raise ValueError("Barcode demasiado corto")
    
    # Extraer partes desde el final
    sufijo = raw[-12:]  # Últimos 12: ISO + Fecha + Secuencia
    nparte = raw[:-12]  # Todo lo anterior es el N° parte
    
    if len(sufijo) != 12:
        raise ValueError("Formato Barcode inválido: sufijo incorrecto")
    
    iso_data = sufijo[:2]        # 92
    fecha_raw = sufijo[2:8]      # 250920
    secuencia = int(sufijo[8:])  # 1292
    
    # Convertir fecha DDMMYY a YYYY-MM-DD
    day = fecha_raw[:2]
    month = fecha_raw[2:4] 
    year_2digit = int(fecha_raw[4:6])
    
    # CRÍTICO: Corregir año basado en contexto de la fecha actual
    import datetime as dt
    current_year = dt.datetime.now().year
    current_century = current_year // 100 * 100
    
    # Si el año de 2 dígitos es mayor que el año actual, probablemente es del siglo anterior
    full_year = current_century + year_2digit
    if full_year > current_year + 10:  # Si es más de 10 años en futuro, usar siglo anterior
        full_year -= 100
    
    year = str(full_year)
    
    logger.info(f"🔍 BARCODE: año_2digitos={year_2digit}, año_calculado={year}")
    fecha_iso = f"{year}-{month}-{day}"
    
    # Validar fecha
    try:
        dt.datetime.strptime(fecha_iso, "%Y-%m-%d")
        logger.info(f"✅ Fecha válida en BARCODE: {fecha_iso}")
    except ValueError as e:
        logger.error(f"❌ Fecha inválida en BARCODE: {fecha_raw} -> {fecha_iso}, error: {e}")
        raise ValueError(f"Fecha inválida en barcode: {fecha_raw}")

    return ParsedBarcode(
        raw=raw,
        nparte=nparte,
        isemm_data=iso_data,
        fecha_iso=fecha_iso,
        secuencia=secuencia,
        scan_format="BARCODE"
    )


def parse_scan(raw: str) -> Union[ParsedScan, ParsedBarcode]:
    """Parser universal que detecta y procesa ambos formatos"""
    raw = (raw or "").strip()
    scan_format = detect_scan_format(raw)
    
    if scan_format == "QR":
        return parse_qr_scan(raw)
    elif scan_format == "BARCODE":
        return parse_barcode_scan(raw)
    else:
        raise ValueError(f"Formato de escaneo no reconocido: {raw}")


def convert_barcode_to_scan_record_format(barcode: ParsedBarcode) -> ParsedScan:
    """Convierte ParsedBarcode a formato compatible con ParsedScan para el sistema legacy"""
    return ParsedScan(
        raw=barcode.raw,
        tipo="B",  # B para Barcode
        fecha_iso=barcode.fecha_iso,
        lote=f"B{barcode.fecha_iso.replace('-', '')}-{barcode.secuencia:04d}",
        secuencia=barcode.secuencia,
        estacion="BARCODE",
        nparte=barcode.nparte,
        cantidad=1,  # Barcode siempre cantidad 1
        scan_format="BARCODE",
        barcode_sequence=barcode.secuencia
    )

