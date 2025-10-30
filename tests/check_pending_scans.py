"""Script para revisar los registros pendientes de sincronización"""
import sqlite3
import sys
from pathlib import Path

# Agregar el directorio padre al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings

def check_pending():
    conn = sqlite3.connect(str(settings.LOCAL_SQLITE_PATH))
    
    # Contar totales
    cursor = conn.execute("SELECT COUNT(*) FROM scans_local WHERE synced_to_mysql = 0")
    total_pending = cursor.fetchone()[0]
    
    print(f"📊 Total de registros pendientes: {total_pending}")
    print("="*100)
    
    # Ver los primeros 15 registros pendientes
    cursor = conn.execute("""
        SELECT id, tipo, LENGTH(tipo) as len_tipo, raw, scan_format
        FROM scans_local 
        WHERE synced_to_mysql = 0 
        ORDER BY id 
        LIMIT 15
    """)
    
    print("\nID   | TIPO       | LEN | FORMAT  | RAW (primeros 60 chars)")
    print("-"*100)
    
    for row in cursor.fetchall():
        id_val = row[0]
        tipo = row[1] if row[1] is not None else "NULL"
        len_tipo = row[2] if row[2] is not None else 0
        raw = row[3] if row[3] is not None else ""
        scan_format = row[4] if row[4] is not None else "NULL"
        
        print(f"{id_val:4d} | {tipo:10s} | {len_tipo:3d} | {scan_format:7s} | {raw[:60]}")
    
    # Ver distribución de longitudes de 'tipo'
    print("\n" + "="*100)
    print("📈 Distribución de longitudes de 'tipo' en pendientes:")
    cursor = conn.execute("""
        SELECT LENGTH(tipo) as len, COUNT(*) as count
        FROM scans_local 
        WHERE synced_to_mysql = 0
        GROUP BY LENGTH(tipo)
        ORDER BY len
    """)
    
    for row in cursor.fetchall():
        len_val = row[0] if row[0] is not None else 0
        count = row[1]
        print(f"  Longitud {len_val}: {count} registros")
    
    conn.close()

if __name__ == "__main__":
    check_pending()
