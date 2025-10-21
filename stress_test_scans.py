"""
Script de prueba de estrés para reproducir bug de producción
Simula 300+ escaneos alternando QR y BARCODE para detectar el punto de falla
"""
import sys
import time
import sqlite3
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo

# Agregar directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent))

from app.config import settings
from app.services.dual_db import DualDatabaseSystem

# Configuración de la prueba
TEST_LINE = "M1"
TEST_PART = "EBR42005001"
NUM_SCANS = 350  # 350 piezas = 700 escaneos (QR + BARCODE)
SCAN_DELAY = 0.1  # 100ms entre escaneos (similar a operador real)

def generate_qr_code(piece_num: int) -> str:
    """Genera un QR único para cada pieza en formato NUEVO (con ñ)"""
    today = date.today().strftime("%Y%m%d")
    lote_num = "9999"  # Lote de prueba
    secuencia = f"{piece_num:05d}"  # Secuencia de 5 dígitos
    # Formato NUEVO: I20251001'0004'00005ñMAINñEBR33105305ñ1ñ
    return f"I{today}'{lote_num}'{secuencia}ñ{TEST_LINE}ñ{TEST_PART}ñ1ñ"

def generate_barcode(piece_num: int) -> str:
    """Genera un BARCODE único para cada pieza"""
    # Formato: [NPARTE][ISO_2][DDMMYY_6][SEQ_4]
    # Ejemplo: EBR41039117 92 250920 1292
    today_ddmmyy = date.today().strftime("%d%m%y")
    iso_code = "92"  # ISO de prueba
    secuencia = f"{piece_num:04d}"  # Secuencia de 4 dígitos
    return f"{TEST_PART}{iso_code}{today_ddmmyy}{secuencia}"

def check_database_state(db_system: DualDatabaseSystem, scan_num: int):
    """Verifica el estado de las bases de datos"""
    try:
        # SQLite local
        with db_system._get_sqlite_connection(timeout=1.0) as conn:
            # Contar escaneos completos
            cur_complete = conn.execute("""
                SELECT COUNT(*) FROM scans_local WHERE is_complete = 1
            """)
            complete_count = cur_complete.fetchone()[0]
            
            # Contar staging pendiente
            cur_pending = conn.execute("""
                SELECT COUNT(*), scan_format FROM pending_scans GROUP BY scan_format
            """)
            pending = cur_pending.fetchall()
            
            # Producción del plan
            cur_plan = conn.execute("""
                SELECT produced_count, plan_count, status FROM plan_local
                WHERE line = ? AND part_no = ? AND working_date = ?
            """, (TEST_LINE, TEST_PART, date.today().isoformat()))
            plan_row = cur_plan.fetchone()
            
            if plan_row:
                produced = plan_row[0] or 0
                plan_count = plan_row[1] or 0
                status = plan_row[2]
                
                print(f"\n{'='*80}")
                print(f"📊 ESTADO DESPUÉS DE {scan_num} ESCANEOS:")
                print(f"{'='*80}")
                print(f"  SQLite - Completos:    {complete_count} piezas")
                print(f"  SQLite - Staging:      {pending}")
                print(f"  Plan - Producido:      {produced}/{plan_count} ({status})")
                print(f"  Buffer pendiente:      {db_system._plan_produced_buffer}")
                
                # DETECTAR DISCREPANCIA
                expected_pieces = scan_num // 2  # Cada 2 escaneos = 1 pieza
                if produced < expected_pieces - 5:  # Tolerancia de 5 piezas por timing
                    print(f"\n🚨 ¡DISCREPANCIA DETECTADA!")
                    print(f"   Esperado:  {expected_pieces} piezas")
                    print(f"   Producido: {produced} piezas")
                    print(f"   FALTANTES: {expected_pieces - produced} piezas")
                    return False
                
                print(f"{'='*80}\n")
                return True
            else:
                print(f"\n⚠️ No se encontró plan para {TEST_LINE} / {TEST_PART}")
                return False
                
    except Exception as e:
        print(f"\n❌ Error verificando estado: {e}")
        return False

def run_stress_test():
    """Ejecuta la prueba de estrés"""
    print("="*80)
    print("🔥 INICIANDO PRUEBA DE ESTRÉS - SIMULACIÓN DE 300+ ESCANEOS")
    print("="*80)
    print(f"Línea:     {TEST_LINE}")
    print(f"Part No:   {TEST_PART}")
    print(f"Piezas:    {NUM_SCANS} (= {NUM_SCANS * 2} escaneos)")
    print(f"Delay:     {SCAN_DELAY}s por escaneo")
    print("="*80)
    
    # Inicializar sistema
    print("\n1️⃣ Inicializando DualDatabaseSystem...")
    db_system = DualDatabaseSystem()
    time.sleep(2)  # Esperar que sync worker inicie
    
    # Verificar plan existe y está EN PROGRESO
    print(f"\n2️⃣ Verificando plan {TEST_PART} en línea {TEST_LINE}...")
    with db_system._get_sqlite_connection(timeout=5.0) as conn:
        cur = conn.execute("""
            SELECT id, plan_count, produced_count, status FROM plan_local
            WHERE line = ? AND part_no = ? AND working_date = ?
        """, (TEST_LINE, TEST_PART, date.today().isoformat()))
        plan = cur.fetchone()
        
        if not plan:
            print(f"❌ No existe plan para {TEST_LINE} / {TEST_PART} hoy")
            print("   Ejecuta primero: python check_plan_data.py")
            return
        
        plan_id, plan_count, produced, status = plan
        print(f"   ✅ Plan encontrado:")
        print(f"      ID:        {plan_id}")
        print(f"      Plan:      {plan_count}")
        print(f"      Producido: {produced}")
        print(f"      Status:    {status}")
        
        if status != "EN PROGRESO":
            print(f"\n   ⚠️ Plan NO está EN PROGRESO, cambiando estado...")
            db_system.actualizar_estado_plan(plan_id, "EN PROGRESO")
            print(f"   ✅ Estado cambiado a EN PROGRESO")
    
    # Iniciar prueba
    print(f"\n3️⃣ INICIANDO SIMULACIÓN DE {NUM_SCANS} PIEZAS...")
    print("   (Ctrl+C para detener)\n")
    
    start_time = time.time()
    scan_count = 0
    error_count = 0
    success_count = 0
    
    try:
        for piece_num in range(1, NUM_SCANS + 1):
            # Generar QR y BARCODE para esta pieza
            qr_code = generate_qr_code(piece_num)
            barcode = generate_barcode(piece_num)
            
            # Escanear QR
            try:
                result_qr = db_system.add_scan_fast(qr_code, TEST_LINE)
                scan_count += 1
                
                if result_qr > 0:
                    success_count += 1
                    print(f"✅ [{scan_count:4d}] QR #{piece_num:04d} → staging_id={result_qr}")
                elif result_qr == -2:
                    print(f"⚠️ [{scan_count:4d}] QR #{piece_num:04d} → DUPLICADO")
                elif result_qr == -3:
                    print(f"❌ [{scan_count:4d}] QR #{piece_num:04d} → FUERA DE PLAN")
                    error_count += 1
                elif result_qr == -4:
                    print(f"🏁 [{scan_count:4d}] QR #{piece_num:04d} → PLAN COMPLETO")
                    break
                elif result_qr == -5:
                    print(f"⚠️ [{scan_count:4d}] QR #{piece_num:04d} → MISMO FORMATO CONSECUTIVO")
                else:
                    print(f"❌ [{scan_count:4d}] QR #{piece_num:04d} → ERROR {result_qr}")
                    error_count += 1
                
                time.sleep(SCAN_DELAY)
                
            except Exception as e:
                print(f"💥 [{scan_count:4d}] ERROR en QR #{piece_num:04d}: {e}")
                error_count += 1
            
            # Escanear BARCODE
            try:
                result_bc = db_system.add_scan_fast(barcode, TEST_LINE)
                scan_count += 1
                
                if result_bc > 0:
                    success_count += 1
                    print(f"✅ [{scan_count:4d}] BC #{piece_num:04d} → PAR COMPLETO")
                elif result_bc == -2:
                    print(f"⚠️ [{scan_count:4d}] BC #{piece_num:04d} → DUPLICADO")
                elif result_bc == -3:
                    print(f"❌ [{scan_count:4d}] BC #{piece_num:04d} → FUERA DE PLAN")
                    error_count += 1
                elif result_bc == -4:
                    print(f"🏁 [{scan_count:4d}] BC #{piece_num:04d} → PLAN COMPLETO")
                    break
                elif result_bc == -5:
                    print(f"⚠️ [{scan_count:4d}] BC #{piece_num:04d} → MISMO FORMATO CONSECUTIVO")
                else:
                    print(f"❌ [{scan_count:4d}] BC #{piece_num:04d} → ERROR {result_bc}")
                    error_count += 1
                
                time.sleep(SCAN_DELAY)
                
            except Exception as e:
                print(f"💥 [{scan_count:4d}] ERROR en BC #{piece_num:04d}: {e}")
                error_count += 1
            
            # Verificar estado cada 50 piezas
            if piece_num % 50 == 0:
                check_database_state(db_system, scan_count)
                
                # Esperar un poco más para que el sync worker procese
                print(f"\n⏸️  Esperando 5s para que sync worker procese buffer...")
                time.sleep(5)
    
    except KeyboardInterrupt:
        print(f"\n\n⚠️ Prueba interrumpida por usuario")
    
    # Resumen final
    elapsed = time.time() - start_time
    print("\n" + "="*80)
    print("📊 RESUMEN DE PRUEBA")
    print("="*80)
    print(f"Tiempo total:       {elapsed:.2f}s")
    print(f"Escaneos totales:   {scan_count}")
    print(f"Exitosos:           {success_count}")
    print(f"Errores:            {error_count}")
    print(f"Tasa:               {scan_count/elapsed:.2f} escaneos/s")
    print("="*80)
    
    # Verificación final completa
    print("\n4️⃣ VERIFICACIÓN FINAL (esperando 10s para sync)...")
    time.sleep(10)
    
    final_ok = check_database_state(db_system, scan_count)
    
    if final_ok:
        print("\n✅ PRUEBA COMPLETADA - Sin discrepancias detectadas")
    else:
        print("\n🚨 PRUEBA COMPLETADA - ¡BUG REPRODUCIDO!")
        print("\n📝 Siguiente paso:")
        print("   1. Revisa logs/app.log para mensajes de '📦 BUFFER', '📤 PUSH', '⚠️ SYNC'")
        print("   2. Ejecuta: python diagnose_sync_issue.py")
        print("   3. Compara SQLite vs MySQL para ver dónde están los datos perdidos")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    try:
        run_stress_test()
    except Exception as e:
        print(f"\n💥 Error fatal en prueba: {e}")
        import traceback
        traceback.print_exc()
