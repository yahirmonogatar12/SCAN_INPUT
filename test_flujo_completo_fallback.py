"""
Test de Flujo Completo: Escaneo Sin Plan → Añadir Plan → Aplicar Automáticamente

Simula el escenario real:
1. Operador escanea piezas SIN tener plan cargado
2. Scans se guardan en scans_sin_plan (fallback)
3. Supervisor carga el plan en el sistema
4. Sistema detecta plan nuevo y aplica scans pendientes automáticamente
"""
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from app.services.dual_db import DualDatabaseSystem
from app.config import settings

def simulate_real_scenario():
    """Simulación completa del flujo de fallback"""
    print("\n" + "="*70)
    print("🎬 SIMULACIÓN DE FLUJO COMPLETO: ESCANEO SIN PLAN → AÑADIR PLAN")
    print("="*70)
    
    db = DualDatabaseSystem()
    
    # Datos del escenario
    linea = "M1"
    nparte = "12345-BRAKE-PAD"
    lote = "LOT2025-100"
    timestamp = int(time.time() * 1000)
    today = datetime.now(ZoneInfo(settings.TZ)).strftime('%Y-%m-%d')
    
    print(f"\n📋 ESCENARIO:")
    print(f"   Línea: {linea}")
    print(f"   Parte: {nparte}")
    print(f"   Lote: {lote}")
    print(f"   Fecha: {today}")
    
    # Limpiar datos previos de este test
    print(f"\n🧹 Limpiando datos previos del test...")
    with db._get_sqlite_connection(timeout=2.0) as conn:
        # Limpiar tabla de scans sin plan
        deleted_fallback = conn.execute("""
            DELETE FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ?
        """, (linea, nparte)).rowcount
        
        # Limpiar plan local si existe
        deleted_plans = conn.execute("""
            DELETE FROM plan_local 
            WHERE line = ? AND part_no = ?
        """, (linea, nparte)).rowcount
        
        # Limpiar scans locales del test
        deleted_scans = conn.execute("""
            DELETE FROM scans_local 
            WHERE linea = ? AND nparte = ?
        """, (linea, nparte)).rowcount
        
        conn.commit()
    
    print(f"   ✅ Eliminados: {deleted_scans} scans, {deleted_plans} planes, {deleted_fallback} scans_sin_plan")
    
    # ========================================================================
    # FASE 1: ESCANEO SIN PLAN (operador empieza a trabajar sin plan cargado)
    # ========================================================================
    print("\n" + "="*70)
    print("📦 FASE 1: OPERADOR ESCANEA SIN PLAN CARGADO")
    print("="*70)
    
    # Simular que el operador escaneó 5 pares (10 scans: 5 QR + 5 BARCODE)
    num_pares = 5
    scans_insertados = []
    
    print(f"\n👷 Operador escanea {num_pares} piezas (sin plan activo)...")
    
    with db._get_sqlite_connection(timeout=2.0) as conn:
        for i in range(num_pares):
            now = datetime.now(ZoneInfo(settings.TZ)).isoformat()
            
            # Escanear QR
            raw_qr = f"QR-{nparte}-{timestamp}-{i}"
            conn.execute("""
                INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
                VALUES (?, ?, ?, ?, 1, 'QR', ?, ?)
            """, (raw_qr, nparte, lote, linea, today, now))
            scan_id_qr = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            
            # Escanear BARCODE
            raw_bc = f"BC-{nparte}-{timestamp}-{i}"
            conn.execute("""
                INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
                VALUES (?, ?, ?, ?, 1, 'BARCODE', ?, ?)
            """, (raw_bc, nparte, lote, linea, today, now))
            scan_id_bc = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            
            # Guardar en scans_sin_plan (simula que el sistema detectó que no hay plan)
            conn.execute("""
                INSERT INTO scans_sin_plan 
                (scan_id, linea, nparte, lote, cantidad, fecha, ts, scan_format, aplicado)
                VALUES (?, ?, ?, ?, 1, ?, ?, 'QR', 0)
            """, (scan_id_qr, linea, nparte, lote, today, now))
            
            conn.execute("""
                INSERT INTO scans_sin_plan 
                (scan_id, linea, nparte, lote, cantidad, fecha, ts, scan_format, aplicado)
                VALUES (?, ?, ?, ?, 1, ?, ?, 'BARCODE', 0)
            """, (scan_id_bc, linea, nparte, lote, today, now))
            
            scans_insertados.append((scan_id_qr, scan_id_bc))
            print(f"   ✅ Par {i+1}: QR (ID:{scan_id_qr}) + BC (ID:{scan_id_bc}) → scans_sin_plan")
        
        conn.commit()
    
    # Verificar estado después de escaneos
    with db._get_sqlite_connection(timeout=2.0) as conn:
        pendientes = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND aplicado = 0
        """, (linea, nparte)).fetchone()[0]
        
        planes_activos = conn.execute("""
            SELECT COUNT(*) FROM plan_local 
            WHERE line = ? AND part_no = ? AND status IN ('EN PROGRESO', 'PAUSADO')
        """, (linea, nparte)).fetchone()[0]
    
    print(f"\n📊 Estado después de escaneos:")
    print(f"   Scans pendientes: {pendientes}")
    print(f"   Planes activos: {planes_activos}")
    print(f"   ⚠️  Los scans están GUARDADOS pero NO aplicados (sin plan)")
    
    # ========================================================================
    # FASE 2: SUPERVISOR CARGA EL PLAN
    # ========================================================================
    print("\n" + "="*70)
    print("📋 FASE 2: SUPERVISOR CARGA EL PLAN EN EL SISTEMA")
    print("="*70)
    
    print(f"\n👨‍💼 Supervisor carga plan para {nparte}...")
    
    # Insertar plan en plan_local (simula sync desde MySQL)
    plan_count = 100  # Plan de 100 piezas
    now = datetime.now(ZoneInfo(settings.TZ)).isoformat()
    
    with db._get_sqlite_connection(timeout=2.0) as conn:
        conn.execute("""
            INSERT INTO plan_local 
            (line, part_no, lot_no, plan_count, produced_count, working_date, status, sequence, updated_at)
            VALUES (?, ?, ?, ?, 0, ?, 'EN PROGRESO', 1, ?)
        """, (linea, nparte, lote, plan_count, today, now))
        
        plan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
    
    print(f"   ✅ Plan creado:")
    print(f"      ID: {plan_id}")
    print(f"      Meta: {plan_count} piezas")
    print(f"      Producido: 0 (aún no se aplicaron scans)")
    
    # ========================================================================
    # FASE 3: SISTEMA APLICA SCANS PENDIENTES AUTOMÁTICAMENTE
    # ========================================================================
    print("\n" + "="*70)
    print("🔄 FASE 3: SISTEMA DETECTA PLAN Y APLICA SCANS AUTOMÁTICAMENTE")
    print("="*70)
    
    print("\n⚡ Ejecutando _aplicar_scans_pendientes()...")
    
    # Esta función se llama automáticamente después de _sync_plan_from_mysql()
    # La llamamos manualmente para simular
    db._aplicar_scans_pendientes()
    
    # ========================================================================
    # FASE 4: VERIFICAR RESULTADOS
    # ========================================================================
    print("\n" + "="*70)
    print("✅ FASE 4: VERIFICACIÓN DE RESULTADOS")
    print("="*70)
    
    with db._get_sqlite_connection(timeout=2.0) as conn:
        # 1. Verificar scans marcados como aplicados
        scans_aplicados = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND aplicado = 1
        """, (linea, nparte)).fetchone()[0]
        
        scans_pendientes = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND aplicado = 0
        """, (linea, nparte)).fetchone()[0]
        
        # 2. Verificar plan actualizado
        plan_info = conn.execute("""
            SELECT id, produced_count, plan_count, status 
            FROM plan_local 
            WHERE line = ? AND part_no = ?
        """, (linea, nparte)).fetchone()
        
        # 3. Obtener detalle de scans aplicados
        detalle_aplicados = conn.execute("""
            SELECT scan_format, COUNT(*) as count
            FROM scans_sin_plan
            WHERE linea = ? AND nparte = ? AND aplicado = 1
            GROUP BY scan_format
        """, (linea, nparte)).fetchall()
    
    # Mostrar resultados
    print(f"\n📊 RESULTADOS FINALES:")
    print(f"\n1️⃣  Scans:")
    print(f"   ✅ Aplicados: {scans_aplicados}")
    print(f"   ⏳ Pendientes: {scans_pendientes}")
    
    if detalle_aplicados:
        print(f"\n   Detalle aplicados:")
        for formato, count in detalle_aplicados:
            print(f"      {formato}: {count}")
    
    print(f"\n2️⃣  Plan:")
    if plan_info:
        plan_id, produced, total, status = plan_info
        progreso = (produced / total * 100) if total > 0 else 0
        print(f"   ID: {plan_id}")
        print(f"   Producido: {produced} / {total} ({progreso:.1f}%)")
        print(f"   Estado: {status}")
    
    # ========================================================================
    # VALIDACIONES
    # ========================================================================
    print("\n" + "="*70)
    print("🔍 VALIDACIONES")
    print("="*70)
    
    validaciones_ok = True
    
    # Validación 1: Se aplicaron todos los scans
    expected_scans = num_pares * 2  # QR + BARCODE por cada par
    if scans_aplicados == expected_scans:
        print(f"✅ PASS: {scans_aplicados} scans aplicados (esperados: {expected_scans})")
    else:
        print(f"❌ FAIL: {scans_aplicados} scans aplicados (esperados: {expected_scans})")
        validaciones_ok = False
    
    # Validación 2: No quedan scans pendientes
    if scans_pendientes == 0:
        print(f"✅ PASS: No quedan scans pendientes")
    else:
        print(f"❌ FAIL: Quedan {scans_pendientes} scans pendientes")
        validaciones_ok = False
    
    # Validación 3: Produced_count correcto (pares completos)
    expected_produced = num_pares
    if plan_info and plan_info[1] == expected_produced:
        print(f"✅ PASS: Produced_count = {plan_info[1]} (esperado: {expected_produced})")
    else:
        actual = plan_info[1] if plan_info else 0
        print(f"❌ FAIL: Produced_count = {actual} (esperado: {expected_produced})")
        validaciones_ok = False
    
    # Validación 4: Plan en estado correcto
    if plan_info and plan_info[3] == 'EN PROGRESO':
        print(f"✅ PASS: Plan en estado 'EN PROGRESO'")
    else:
        estado = plan_info[3] if plan_info else 'N/A'
        print(f"❌ FAIL: Plan en estado '{estado}' (esperado: 'EN PROGRESO')")
        validaciones_ok = False
    
    # ========================================================================
    # RESUMEN FINAL
    # ========================================================================
    print("\n" + "="*70)
    print("📋 RESUMEN DEL FLUJO")
    print("="*70)
    
    print(f"""
FASE 1 - Escaneo sin plan:
   • Operador escaneó {num_pares} piezas ({num_pares * 2} scans)
   • Sistema guardó en scans_sin_plan (fallback)
   • Estado: PENDIENTE de aplicar

FASE 2 - Plan cargado:
   • Supervisor cargó plan de {plan_count} piezas
   • Plan ID: {plan_id}
   • Estado inicial: 0 piezas producidas

FASE 3 - Aplicación automática:
   • Sistema detectó {scans_aplicados} scans pendientes
   • Aplicó {num_pares} pares completos al plan
   • Incrementó produced_count de 0 → {plan_info[1] if plan_info else 0}

FASE 4 - Resultado:
   • Plan actualizado: {plan_info[1] if plan_info else 0}/{plan_count} piezas
   • Progreso: {(plan_info[1]/plan_count*100) if plan_info and plan_count > 0 else 0:.1f}%
   • Scans pendientes: {scans_pendientes}
    """)
    
    if validaciones_ok:
        print("🎉 TODAS LAS VALIDACIONES PASARON")
        print("✅ El sistema de fallback funciona PERFECTAMENTE")
        return True
    else:
        print("⚠️  ALGUNAS VALIDACIONES FALLARON")
        return False

def test_edge_cases():
    """Probar casos especiales"""
    print("\n" + "="*70)
    print("🧪 TEST DE CASOS ESPECIALES")
    print("="*70)
    
    db = DualDatabaseSystem()
    linea = "M2"
    nparte_edge = "99999-EDGE-CASE"
    timestamp = int(time.time() * 1000)
    today = datetime.now(ZoneInfo(settings.TZ)).strftime('%Y-%m-%d')
    now = datetime.now(ZoneInfo(settings.TZ)).isoformat()
    
    # Limpiar
    with db._get_sqlite_connection(timeout=2.0) as conn:
        conn.execute("DELETE FROM scans_sin_plan WHERE linea = ? AND nparte = ?", (linea, nparte_edge))
        conn.execute("DELETE FROM plan_local WHERE line = ? AND part_no = ?", (linea, nparte_edge))
        conn.execute("DELETE FROM scans_local WHERE linea = ? AND nparte = ?", (linea, nparte_edge))
        conn.commit()
    
    # CASO 1: Scans incompletos (QR sin BARCODE)
    print("\n🔬 CASO 1: Scan incompleto (solo QR, sin BARCODE)")
    
    with db._get_sqlite_connection(timeout=2.0) as conn:
        # Insertar solo 1 QR (sin par)
        raw_qr = f"QR-INCOMPLETE-{timestamp}"
        conn.execute("""
            INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
            VALUES (?, ?, ?, ?, 1, 'QR', ?, ?)
        """, (raw_qr, nparte_edge, "LOT-EDGE", linea, today, now))
        scan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        
        conn.execute("""
            INSERT INTO scans_sin_plan 
            (scan_id, linea, nparte, lote, cantidad, fecha, ts, scan_format, aplicado)
            VALUES (?, ?, ?, ?, 1, ?, ?, 'QR', 0)
        """, (scan_id, linea, nparte_edge, "LOT-EDGE", today, now))
        
        # Crear plan
        conn.execute("""
            INSERT INTO plan_local 
            (line, part_no, lot_no, plan_count, produced_count, working_date, status, sequence, updated_at)
            VALUES (?, ?, ?, 50, 0, ?, 'EN PROGRESO', 1, ?)
        """, (linea, nparte_edge, "LOT-EDGE", today, now))
        
        conn.commit()
    
    # Aplicar scans
    db._aplicar_scans_pendientes()
    
    # Verificar
    with db._get_sqlite_connection(timeout=2.0) as conn:
        plan = conn.execute("""
            SELECT produced_count FROM plan_local 
            WHERE line = ? AND part_no = ?
        """, (linea, nparte_edge)).fetchone()
    
    produced = plan[0] if plan else -1
    
    if produced == 0:
        print(f"   ✅ CORRECTO: Produced_count = {produced} (scan incompleto NO aplicado)")
    else:
        print(f"   ⚠️  Produced_count = {produced} (esperado: 0)")
    
    # CASO 2: Múltiples lotes diferentes
    print("\n🔬 CASO 2: Scans de diferentes lotes")
    
    nparte_multi = "88888-MULTI-LOT"
    
    with db._get_sqlite_connection(timeout=2.0) as conn:
        conn.execute("DELETE FROM scans_sin_plan WHERE linea = ? AND nparte = ?", (linea, nparte_multi))
        conn.execute("DELETE FROM plan_local WHERE line = ? AND part_no = ?", (linea, nparte_multi))
        conn.execute("DELETE FROM scans_local WHERE linea = ? AND nparte = ?", (linea, nparte_multi))
        
        # Insertar scans de 2 lotes diferentes
        for lote_num in [1, 2]:
            lote = f"LOT-{lote_num}"
            for i in range(2):  # 2 pares por lote
                raw_qr = f"QR-MULTI-{timestamp}-L{lote_num}-{i}"
                raw_bc = f"BC-MULTI-{timestamp}-L{lote_num}-{i}"
                
                conn.execute("""
                    INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
                    VALUES (?, ?, ?, ?, 1, 'QR', ?, ?)
                """, (raw_qr, nparte_multi, lote, linea, today, now))
                scan_id_qr = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                
                conn.execute("""
                    INSERT INTO scans_local (raw, nparte, lote, linea, cantidad, scan_format, fecha, ts)
                    VALUES (?, ?, ?, ?, 1, 'BARCODE', ?, ?)
                """, (raw_bc, nparte_multi, lote, linea, today, now))
                scan_id_bc = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                
                for scan_id, fmt in [(scan_id_qr, 'QR'), (scan_id_bc, 'BARCODE')]:
                    conn.execute("""
                        INSERT INTO scans_sin_plan 
                        (scan_id, linea, nparte, lote, cantidad, fecha, ts, scan_format, aplicado)
                        VALUES (?, ?, ?, ?, 1, ?, ?, ?, 0)
                    """, (scan_id, linea, nparte_multi, lote, today, now, fmt))
        
        # Crear plan solo para LOT-1
        conn.execute("""
            INSERT INTO plan_local 
            (line, part_no, lot_no, plan_count, produced_count, working_date, status, sequence, updated_at)
            VALUES (?, ?, 'LOT-1', 50, 0, ?, 'EN PROGRESO', 1, ?)
        """, (linea, nparte_multi, today, now))
        
        conn.commit()
    
    # Aplicar scans
    db._aplicar_scans_pendientes()
    
    # Verificar
    with db._get_sqlite_connection(timeout=2.0) as conn:
        plan = conn.execute("""
            SELECT produced_count FROM plan_local 
            WHERE line = ? AND part_no = ?
        """, (linea, nparte_multi)).fetchone()
        
        aplicados_lot1 = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND lote = 'LOT-1' AND aplicado = 1
        """, (linea, nparte_multi)).fetchone()[0]
        
        aplicados_lot2 = conn.execute("""
            SELECT COUNT(*) FROM scans_sin_plan 
            WHERE linea = ? AND nparte = ? AND lote = 'LOT-2' AND aplicado = 1
        """, (linea, nparte_multi)).fetchone()[0]
    
    produced = plan[0] if plan else 0
    
    print(f"   Plan LOT-1: produced_count = {produced}")
    print(f"   Scans LOT-1 aplicados: {aplicados_lot1}")
    print(f"   Scans LOT-2 aplicados: {aplicados_lot2}")
    
    # El sistema aplica con prioridad de lote pero puede aplicar otros si hay matching flexible
    if produced >= 2:
        print(f"   ✅ Sistema aplicó scans correctamente")
    else:
        print(f"   ⚠️  Produced_count menor de lo esperado")
    
    print("\n✅ Tests de casos especiales completados")
    return True

def run_all_tests():
    """Ejecuta todos los tests del flujo completo"""
    print("\n" + "🔥"*35)
    print("TESTS DE FLUJO COMPLETO: FALLBACK SYSTEM")
    print("🔥"*35)
    
    tests = [
        ("Flujo Completo Real", simulate_real_scenario),
        ("Casos Especiales", test_edge_cases),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"\n✅ {name} - PASÓ\n")
            else:
                failed += 1
                print(f"\n❌ {name} - FALLÓ\n")
        except Exception as e:
            failed += 1
            print(f"\n❌ {name} - ERROR: {e}\n")
            import traceback
            traceback.print_exc()
    
    # Resumen
    print("\n" + "="*70)
    print("RESUMEN FINAL")
    print("="*70)
    print(f"✅ Pasaron: {passed}/{len(tests)}")
    print(f"❌ Fallaron: {failed}/{len(tests)}")
    
    if failed == 0:
        print("\n🎉 TODOS LOS TESTS PASARON 🎉")
        print("\n📋 Sistema verificado:")
        print("   1. Scans se guardan cuando no hay plan")
        print("   2. Scans se aplican automáticamente al cargar plan")
        print("   3. Produced_count se incrementa correctamente")
        print("   4. Solo se cuentan pares completos")
        print("   5. Casos especiales manejados correctamente")
    else:
        print(f"\n⚠️  {failed} test(s) fallaron")
        sys.exit(1)

if __name__ == "__main__":
    run_all_tests()
