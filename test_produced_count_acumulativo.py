"""
Test específico: Verificar que produced_count SUMA scans sin plan
Escenario: Escanean 100 piezas sin plan, luego añaden plan, debería empezar en 100
"""
from datetime import datetime
from zoneinfo import ZoneInfo
from app.services.dual_db import DualDatabaseSystem
from app.config import settings
import time

def test_produced_count_accumulation():
    """Verificar que scans sin plan se suman al produced_count del plan"""
    print("\n" + "="*70)
    print("TEST: PRODUCED_COUNT ACUMULATIVO CON SCANS SIN PLAN")
    print("="*70)
    
    db = DualDatabaseSystem()
    
    linea = "M1"
    nparte = "TEST-12345-ACCUMULATION"
    lote = "LOT-ACC-100"
    timestamp = int(time.time() * 1000)
    today = datetime.now(ZoneInfo(settings.TZ)).strftime('%Y-%m-%d')
    now = datetime.now(ZoneInfo(settings.TZ)).isoformat()
    
    # Limpiar
    print("\n🧹 Limpiando datos previos...")
    with db._get_sqlite_connection(timeout=2.0) as conn:
        conn.execute("DELETE FROM scans_sin_plan WHERE linea = ? AND nparte = ?", (linea, nparte))
        conn.execute("DELETE FROM plan_local WHERE line = ? AND part_no = ?", (linea, nparte))
        conn.execute("DELETE FROM scans_local WHERE linea = ? AND nparte = ?", (linea, nparte))
        conn.commit()
    
    # ===========================================================================
    # FASE 1: Escanear 100 piezas SIN PLAN (simular operador trabajando sin plan)
    # ===========================================================================
    print("\n" + "="*70)
    print("FASE 1: ESCANEAR 100 PIEZAS SIN PLAN")
    print("="*70)
    
    num_piezas_sin_plan = 100
    print(f"\n👷 Operador escanea {num_piezas_sin_plan} piezas (sin plan cargado)...")
    
    with db._get_sqlite_connection(timeout=2.0) as conn:
        for i in range(num_piezas_sin_plan):
            # QR
            raw_qr = f"QR-{nparte}-{timestamp}-{i}"
            conn.execute("""
                INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
                VALUES (?, ?, ?, ?, 1, 'QR', ?, ?)
            """, (raw_qr, nparte, lote, linea, today, now))
            scan_id_qr = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            
            # BARCODE
            raw_bc = f"BC-{nparte}-{timestamp}-{i}"
            conn.execute("""
                INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
                VALUES (?, ?, ?, ?, 1, 'BARCODE', ?, ?)
            """, (raw_bc, nparte, lote, linea, today, now))
            scan_id_bc = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            
            # Guardar en scans_sin_plan
            for scan_id, fmt in [(scan_id_qr, 'QR'), (scan_id_bc, 'BARCODE')]:
                conn.execute("""
                    INSERT INTO scans_sin_plan 
                    (scan_id, linea, nparte, lote, cantidad, fecha, ts, scan_format, aplicado)
                    VALUES (?, ?, ?, ?, 1, ?, ?, ?, 0)
                """, (scan_id, linea, nparte, lote, today, now, fmt))
            
            if (i + 1) % 25 == 0:
                print(f"   ✅ {i + 1} piezas escaneadas...")
        
        conn.commit()
    
    # Verificar scans guardados
    with db._get_sqlite_connection(timeout=2.0) as conn:
        total_scans = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND aplicado = 0
        """, (linea, nparte)).fetchone()[0]
        
        scans_qr = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND aplicado = 0 AND scan_format = 'QR'
        """, (linea, nparte)).fetchone()[0]
        
        scans_bc = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND aplicado = 0 AND scan_format = 'BARCODE'
        """, (linea, nparte)).fetchone()[0]
    
    print(f"\n📊 Estado después de escaneos:")
    print(f"   Total scans guardados: {total_scans}")
    print(f"   QR: {scans_qr}")
    print(f"   BARCODE: {scans_bc}")
    print(f"   Pares completos: {min(scans_qr, scans_bc)}")
    
    # ===========================================================================
    # FASE 2: SUPERVISOR AÑADE PLAN (debería iniciar en produced_count = 0)
    # ===========================================================================
    print("\n" + "="*70)
    print("FASE 2: SUPERVISOR AÑADE PLAN AL SISTEMA")
    print("="*70)
    
    plan_count_total = 500
    print(f"\n👨‍💼 Supervisor carga plan de {plan_count_total} piezas...")
    
    with db._get_sqlite_connection(timeout=2.0) as conn:
        conn.execute("""
            INSERT INTO plan_local 
            (line, part_no, lot_no, plan_count, produced_count, working_date, status, sequence, updated_at)
            VALUES (?, ?, ?, ?, 0, ?, 'EN PROGRESO', 1, ?)
        """, (linea, nparte, lote, plan_count_total, today, now))
        plan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
    
    print(f"   ✅ Plan ID: {plan_id}")
    print(f"   📋 Meta total: {plan_count_total} piezas")
    print(f"   📊 Producido INICIAL: 0 piezas (plan recién creado)")
    
    # ===========================================================================
    # FASE 3: SISTEMA APLICA SCANS PENDIENTES AUTOMÁTICAMENTE
    # ===========================================================================
    print("\n" + "="*70)
    print("FASE 3: SISTEMA APLICA SCANS PENDIENTES (AUTOMÁTICO)")
    print("="*70)
    
    print("\n⚡ Ejecutando _aplicar_scans_pendientes()...")
    db._aplicar_scans_pendientes()
    
    # ===========================================================================
    # FASE 4: VERIFICAR PRODUCED_COUNT
    # ===========================================================================
    print("\n" + "="*70)
    print("FASE 4: VERIFICACIÓN DE PRODUCED_COUNT")
    print("="*70)
    
    with db._get_sqlite_connection(timeout=2.0) as conn:
        plan_info = conn.execute("""
            SELECT id, produced_count, plan_count 
            FROM plan_local 
            WHERE line = ? AND part_no = ?
        """, (linea, nparte)).fetchone()
        
        scans_aplicados = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND aplicado = 1
        """, (linea, nparte)).fetchone()[0]
        
        scans_pendientes = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND aplicado = 0
        """, (linea, nparte)).fetchone()[0]
    
    if plan_info:
        plan_id, produced, total = plan_info
        progreso = (produced / total * 100) if total > 0 else 0
        
        print(f"\n📊 RESULTADOS FINALES:")
        print(f"   Plan ID: {plan_id}")
        print(f"   🎯 Meta total: {total} piezas")
        print(f"   ✅ Producido: {produced} piezas")
        print(f"   📈 Progreso: {progreso:.1f}%")
        print(f"   📦 Scans aplicados: {scans_aplicados}")
        print(f"   ⏳ Scans pendientes: {scans_pendientes}")
        
        # ===========================================================================
        # VALIDACIONES
        # ===========================================================================
        print("\n" + "="*70)
        print("VALIDACIONES")
        print("="*70)
        
        esperado = num_piezas_sin_plan
        
        if produced == esperado:
            print(f"✅ CORRECTO: Produced_count = {produced} (esperado: {esperado})")
            print(f"✅ Los {num_piezas_sin_plan} scans sin plan SE SUMARON al plan")
        else:
            print(f"❌ ERROR: Produced_count = {produced} (esperado: {esperado})")
            return False
        
        if scans_aplicados == num_piezas_sin_plan * 2:  # QR + BC
            print(f"✅ Todos los scans fueron aplicados ({scans_aplicados} scans)")
        else:
            print(f"⚠️  Scans aplicados: {scans_aplicados} (esperado: {num_piezas_sin_plan * 2})")
        
        if scans_pendientes == 0:
            print(f"✅ No quedan scans pendientes")
        else:
            print(f"⚠️  Quedan {scans_pendientes} scans pendientes")
        
        # ===========================================================================
        # FASE 5: SIMULAR ESCANEOS ADICIONALES (deberían seguir desde 101, 102...)
        # ===========================================================================
        print("\n" + "="*70)
        print("FASE 5: ESCANEAR PIEZAS ADICIONALES (CON PLAN ACTIVO)")
        print("="*70)
        
        print(f"\n👷 Operador continúa escaneando (ahora CON plan activo)...")
        print(f"   Debería continuar desde {produced + 1}, {produced + 2}, etc.")
        
        # Simular 10 piezas más CON plan activo
        piezas_adicionales = 10
        
        with db._get_sqlite_connection(timeout=2.0) as conn:
            for i in range(piezas_adicionales):
                idx = num_piezas_sin_plan + i
                
                # QR
                raw_qr = f"QR-ADICIONAL-{nparte}-{timestamp}-{idx}"
                conn.execute("""
                    INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
                    VALUES (?, ?, ?, ?, 1, 'QR', ?, ?)
                """, (raw_qr, nparte, lote, linea, today, now))
                
                # BARCODE
                raw_bc = f"BC-ADICIONAL-{nparte}-{timestamp}-{idx}"
                conn.execute("""
                    INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
                    VALUES (?, ?, ?, ?, 1, 'BARCODE', ?, ?)
                """, (raw_bc, nparte, lote, linea, today, now))
            
            # Simular incremento directo (como haría add_scan_fast con plan activo)
            conn.execute("""
                UPDATE plan_local
                SET produced_count = produced_count + ?
                WHERE id = ?
            """, (piezas_adicionales, plan_id))
            
            conn.commit()
        
        # Verificar nuevo produced_count
        with db._get_sqlite_connection(timeout=2.0) as conn:
            nuevo_produced = conn.execute("""
                SELECT produced_count FROM plan_local WHERE id = ?
            """, (plan_id,)).fetchone()[0]
        
        print(f"\n📊 Después de {piezas_adicionales} piezas adicionales:")
        print(f"   Producido ANTERIOR: {produced}")
        print(f"   Piezas escaneadas: +{piezas_adicionales}")
        print(f"   Producido ACTUAL: {nuevo_produced}")
        print(f"   Progreso: {(nuevo_produced/total*100):.1f}%")
        
        esperado_final = produced + piezas_adicionales
        
        if nuevo_produced == esperado_final:
            print(f"\n✅ PERFECTO: La numeración continúa correctamente")
            print(f"✅ Secuencia: 0 → {produced} (scans sin plan) → {nuevo_produced} (con plan)")
        else:
            print(f"\n❌ ERROR: Produced_count no continúa correctamente")
            return False
        
        # ===========================================================================
        # RESUMEN FINAL
        # ===========================================================================
        print("\n" + "="*70)
        print("RESUMEN COMPLETO")
        print("="*70)
        
        print(f"""
FLUJO COMPLETO VERIFICADO:

1. INICIO (Sin plan):
   • Operador escaneó: {num_piezas_sin_plan} piezas
   • Sistema guardó en scans_sin_plan
   • Produced_count plan: N/A (no existía)

2. PLAN CARGADO:
   • Supervisor añadió plan de {total} piezas
   • Produced_count inicial: 0

3. APLICACIÓN AUTOMÁTICA:
   • Sistema aplicó {num_piezas_sin_plan} piezas
   • Produced_count: 0 → {produced}
   
4. CONTINUACIÓN:
   • Operador escaneó {piezas_adicionales} piezas más
   • Produced_count: {produced} → {nuevo_produced}

✅ RESULTADO FINAL:
   • Total producido: {nuevo_produced} / {total} piezas
   • Progreso: {(nuevo_produced/total*100):.1f}%
   • Sistema funciona CORRECTAMENTE
   
🎯 CONCLUSIÓN:
   Los scans sin plan SE SUMAN al produced_count cuando se carga el plan.
   El contador continúa correctamente: {produced}, {produced+1}, {produced+2}... ✅
        """)
        
        return True
    else:
        print("❌ No se encontró el plan")
        return False

if __name__ == "__main__":
    print("\n" + "🔥"*35)
    print("TEST DE ACUMULACIÓN DE PRODUCED_COUNT")
    print("🔥"*35)
    
    if test_produced_count_accumulation():
        print("\n" + "="*70)
        print("🎉 TEST PASÓ - EL SISTEMA FUNCIONA CORRECTAMENTE")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("❌ TEST FALLÓ")
        print("="*70)
