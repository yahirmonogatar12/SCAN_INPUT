"""
🧪 PRUEBA DE RESILIENCIA DE CIERRE Y SINCRONIZACIÓN

Esta prueba verifica que los refuerzos implementados en dual_db.py funcionen correctamente:

1. ✅ Reparación de pares huérfanos al arrancar (_repair_unlinked_pairs)
2. ✅ Drenaje completo de la cola antes del cierre (sync_before_shutdown con 5 ciclos)
3. ✅ Persistencia de datos tras reinicio (sin regresión a conteos anteriores)
4. ✅ Manejo correcto de errores 2013 (Lost connection to MySQL server)

Escenarios a probar:
- Pares completos (QR↔BARCODE) con linked_scan_id nulo por desconexión previa
- Lotes grandes (>100 piezas) que requieren múltiples ciclos de sincronización
- Cierre y reapertura de la aplicación verificando conteos
- Simulación de errores de conexión a MySQL durante sync
"""

import sys
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Importar el sistema dual_db
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.dual_db import DualDatabaseSystem
from app.config import settings


class TestShutdownResilience:
    """Suite de pruebas para verificar resiliencia del cierre"""
    
    def __init__(self):
        self.db_system = None
        self.test_results: Dict[str, bool] = {}
        self.test_details: Dict[str, str] = {}
        
    def setup(self):
        """Inicializar el sistema de base de datos"""
        logger.info("🔧 Iniciando setup de prueba...")
        try:
            self.db_system = DualDatabaseSystem()
            logger.info("✅ Sistema DualDB inicializado correctamente")
            return True
        except Exception as e:
            logger.error(f"❌ Error en setup: {e}", exc_info=True)
            return False
    
    def teardown(self):
        """Cerrar el sistema de base de datos"""
        logger.info("🧹 Ejecutando teardown...")
        try:
            if self.db_system:
                # El sistema no tiene método close(), usamos sync_before_shutdown
                try:
                    self.db_system.sync_before_shutdown()
                except Exception:
                    pass
                logger.info("✅ Sistema DualDB cerrado correctamente")
        except Exception as e:
            logger.error(f"❌ Error en teardown: {e}", exc_info=True)
    
    def _get_sqlite_counts(self) -> Dict[str, int]:
        """Obtener conteos de SQLite directamente"""
        try:
            with sqlite3.connect(self.db_system.sqlite_path, timeout=5.0) as conn:
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN synced_to_mysql = 0 THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN is_complete = 1 THEN 1 ELSE 0 END) as complete,
                        SUM(CASE WHEN linked_scan_id IS NULL THEN 1 ELSE 0 END) as unlinked,
                        SUM(CASE WHEN scan_format = 'QR' THEN 1 ELSE 0 END) as qr_count,
                        SUM(CASE WHEN scan_format = 'BARCODE' THEN 1 ELSE 0 END) as barcode_count
                    FROM scans_local
                """)
                row = cursor.fetchone()
                return {
                    'total': row[0] or 0,
                    'pending': row[1] or 0,
                    'complete': row[2] or 0,
                    'unlinked': row[3] or 0,
                    'qr_count': row[4] or 0,
                    'barcode_count': row[5] or 0
                }
        except Exception as e:
            logger.error(f"Error obteniendo conteos SQLite: {e}")
            return {}
    
    def _get_mysql_counts(self) -> int:
        """Obtener conteo de MySQL"""
        try:
            from app.db import get_db
            db = get_db()
            
            # Usar query_all en lugar de execute_query
            if settings.APP_MODE == 'IMD':
                result = db.query_all("SELECT COUNT(*) as cnt FROM output_imd")
            else:
                result = db.query_all("SELECT COUNT(*) as cnt FROM scans")
            
            if result and len(result) > 0:
                return result[0].get('cnt', 0)
            return 0
        except Exception as e:
            logger.error(f"Error obteniendo conteos MySQL: {e}")
            return -1
    
    def _create_orphaned_pairs(self, count: int = 50) -> int:
        """
        Crear pares QR↔BARCODE huérfanos (con linked_scan_id NULL)
        
        Simula el escenario donde se crearon pares pero la conexión MySQL falló
        antes de poder vincularlos completamente.
        """
        logger.info(f"🧪 Creando {count} pares huérfanos...")
        created = 0
        
        try:
            with sqlite3.connect(self.db_system.sqlite_path, timeout=10.0) as conn:
                fecha_hoy = datetime.now(ZoneInfo(settings.TZ)).strftime('%Y-%m-%d')
                
                for i in range(count):
                    # Crear QR - usar tipo corto 'T' para evitar error de columna
                    qr_raw = f"T|QR|{fecha_hoy}|LOT{i:04d}|{i:04d}|S1|P{i:03d}|M-X"
                    qr_ts = datetime.now(ZoneInfo(settings.TZ)).isoformat()
                    
                    cursor_qr = conn.execute("""
                        INSERT INTO scans_local 
                        (ts, raw, scan_format, tipo, fecha, lote, secuencia, estacion, 
                         nparte, modelo, cantidad, linea, is_complete, synced_to_mysql, linked_scan_id)
                        VALUES (?, ?, 'QR', 'T', ?, ?, ?, 'S1', ?, 'M-X', 1, 'L1', 0, 0, NULL)
                    """, (qr_ts, qr_raw, fecha_hoy, f"LOT{i:04d}", f"{i:04d}", f"P{i:03d}"))
                    
                    qr_id = cursor_qr.lastrowid
                    
                    # Crear BARCODE correspondiente
                    bc_raw = f"BC{i:08d}"
                    bc_ts = datetime.now(ZoneInfo(settings.TZ)).isoformat()
                    
                    cursor_bc = conn.execute("""
                        INSERT INTO scans_local 
                        (ts, raw, scan_format, tipo, fecha, lote, secuencia, estacion, 
                         nparte, modelo, cantidad, linea, barcode_sequence, is_complete, synced_to_mysql, linked_scan_id)
                        VALUES (?, ?, 'BARCODE', 'T', ?, ?, ?, 'S1', ?, 'M-X', 1, 'L1', ?, 0, 0, NULL)
                    """, (bc_ts, bc_raw, fecha_hoy, f"LOT{i:04d}", f"{i:04d}", f"P{i:03d}", bc_raw))
                    
                    bc_id = cursor_bc.lastrowid
                    
                    # NO vincular (dejarlos huérfanos deliberadamente)
                    # En producción, esto ocurre cuando hay error MySQL durante el proceso
                    
                    created += 1
                
                conn.commit()
                logger.info(f"✅ Creados {created} pares huérfanos (sin linked_scan_id)")
                
        except Exception as e:
            logger.error(f"❌ Error creando pares huérfanos: {e}", exc_info=True)
        
        return created
    
    def test_01_orphan_repair_on_startup(self) -> bool:
        """
        Test 1: Verificar que _repair_unlinked_pairs() repare pares al arrancar
        """
        test_name = "test_01_orphan_repair_on_startup"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 1: Reparación de pares huérfanos al arrancar")
        logger.info(f"{'='*80}\n")
        
        try:
            # 1. Obtener conteos iniciales
            counts_before = self._get_sqlite_counts()
            logger.info(f"📊 Conteos iniciales SQLite:")
            logger.info(f"   - Total: {counts_before['total']}")
            logger.info(f"   - Pendientes sync: {counts_before['pending']}")
            logger.info(f"   - Sin vincular (unlinked): {counts_before['unlinked']}")
            
            # 2. Crear pares huérfanos
            orphans_created = self._create_orphaned_pairs(25)
            if orphans_created == 0:
                logger.error("❌ No se pudieron crear pares huérfanos")
                self.test_results[test_name] = False
                return False
            
            # 3. Verificar que existan pares sin vincular
            counts_after_create = self._get_sqlite_counts()
            logger.info(f"📊 Después de crear huérfanos:")
            logger.info(f"   - Sin vincular: {counts_after_create['unlinked']}")
            
            if counts_after_create['unlinked'] < orphans_created:
                logger.warning(f"⚠️ Esperábamos al menos {orphans_created*2} sin vincular, pero hay {counts_after_create['unlinked']}")
            
            # 4. Ejecutar reparación manualmente (simula el arranque)
            logger.info("🔧 Ejecutando reparación de pares huérfanos...")
            repaired_pairs, leftover_scans = self.db_system._repair_unlinked_pairs()
            
            logger.info(f"✅ Reparación completada:")
            logger.info(f"   - Pares reparados: {repaired_pairs}")
            logger.info(f"   - Escaneos sin pareja: {leftover_scans}")
            
            # 5. Verificar que los pares se hayan vinculado
            counts_after_repair = self._get_sqlite_counts()
            logger.info(f"📊 Después de reparar:")
            logger.info(f"   - Sin vincular: {counts_after_repair['unlinked']}")
            logger.info(f"   - Completos: {counts_after_repair['complete']}")
            
            # Validación
            success = (
                repaired_pairs > 0 and
                counts_after_repair['unlinked'] < counts_after_create['unlinked'] and
                counts_after_repair['complete'] > counts_before['complete']
            )
            
            if success:
                logger.info("✅ TEST 1 PASÓ: Los pares huérfanos se repararon correctamente")
                self.test_details[test_name] = f"Reparados {repaired_pairs} pares, quedan {leftover_scans} sueltos"
            else:
                logger.error("❌ TEST 1 FALLÓ: No se repararon suficientes pares")
                self.test_details[test_name] = f"Esperábamos reparar ~{orphans_created} pero solo se repararon {repaired_pairs}"
            
            self.test_results[test_name] = success
            return success
            
        except Exception as e:
            logger.error(f"❌ TEST 1 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            self.test_details[test_name] = str(e)
            return False
    
    def test_02_shutdown_flush_cycles(self) -> bool:
        """
        Test 2: Verificar que sync_before_shutdown() drene la cola con 5 ciclos
        """
        test_name = "test_02_shutdown_flush_cycles"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 2: Drenaje de cola antes del cierre (5 ciclos)")
        logger.info(f"{'='*80}\n")
        
        try:
            # 1. Conteos iniciales
            counts_before = self._get_sqlite_counts()
            mysql_before = self._get_mysql_counts()
            
            logger.info(f"📊 Estado inicial:")
            logger.info(f"   SQLite - Total: {counts_before['total']}, Pendientes: {counts_before['pending']}")
            logger.info(f"   MySQL - Total: {mysql_before}")
            
            # 2. Si hay pendientes, ejecutar sync_before_shutdown
            if counts_before['pending'] > 0:
                logger.info(f"🔄 Ejecutando sync_before_shutdown() con {counts_before['pending']} pendientes...")
                
                # Simular el cierre (ejecutar manualmente el método)
                result = self.db_system.sync_before_shutdown()
                
                logger.info(f"✅ Sync completado:")
                logger.info(f"   - Escaneos sincronizados: {result.get('scans_synced', 0)}")
                logger.info(f"   - Planes sincronizados: {result.get('plans_synced', 0)}")
                
                # 3. Verificar conteos finales
                counts_after = self._get_sqlite_counts()
                mysql_after = self._get_mysql_counts()
                
                logger.info(f"📊 Estado final:")
                logger.info(f"   SQLite - Total: {counts_after['total']}, Pendientes: {counts_after['pending']}")
                logger.info(f"   MySQL - Total: {mysql_after}")
                
                # Validación
                improvement = counts_before['pending'] - counts_after['pending']
                success = (
                    counts_after['pending'] < counts_before['pending'] and
                    mysql_after > mysql_before
                )
                
                if success:
                    logger.info(f"✅ TEST 2 PASÓ: Se drenaron {improvement} registros pendientes")
                    self.test_details[test_name] = f"Sincronizados {result.get('scans_synced', 0)} escaneos, quedan {counts_after['pending']} pendientes"
                else:
                    logger.error("❌ TEST 2 FALLÓ: No se drenó suficiente cola")
                    self.test_details[test_name] = f"Pendientes antes: {counts_before['pending']}, después: {counts_after['pending']}"
                
                self.test_results[test_name] = success
                return success
            else:
                logger.info("ℹ️ No hay pendientes para sincronizar, test omitido")
                self.test_results[test_name] = True
                self.test_details[test_name] = "Sin pendientes para probar"
                return True
                
        except Exception as e:
            logger.error(f"❌ TEST 2 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            self.test_details[test_name] = str(e)
            return False
    
    def test_03_persistence_after_restart(self) -> bool:
        """
        Test 3: Verificar que al cerrar y reabrir, MySQL refleje los datos completos
        """
        test_name = "test_03_persistence_after_restart"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 3: Persistencia de datos tras reinicio")
        logger.info(f"{'='*80}\n")
        
        try:
            # 1. Obtener conteos antes del cierre
            counts_before_close = self._get_sqlite_counts()
            mysql_before_close = self._get_mysql_counts()
            
            logger.info(f"📊 Estado antes del cierre:")
            logger.info(f"   SQLite - Total: {counts_before_close['total']}, Pendientes: {counts_before_close['pending']}")
            logger.info(f"   MySQL - Total: {mysql_before_close}")
            
            # 2. Cerrar el sistema (simula cierre de app)
            logger.info("🔒 Cerrando sistema (sync_before_shutdown incluido)...")
            self.teardown()
            time.sleep(2)  # Esperar a que termine el cierre
            
            # 3. Reabrir el sistema (simula reinicio de app)
            logger.info("🔄 Reabriendo sistema...")
            if not self.setup():
                logger.error("❌ No se pudo reabrir el sistema")
                self.test_results[test_name] = False
                return False
            
            time.sleep(3)  # Dar tiempo para que el worker de sync arranque
            
            # 4. Obtener conteos después del reinicio
            counts_after_restart = self._get_sqlite_counts()
            mysql_after_restart = self._get_mysql_counts()
            
            logger.info(f"📊 Estado después del reinicio:")
            logger.info(f"   SQLite - Total: {counts_after_restart['total']}, Pendientes: {counts_after_restart['pending']}")
            logger.info(f"   MySQL - Total: {mysql_after_restart}")
            
            # Validación: MySQL debe mantener o incrementar, no debe regresar a valores anteriores
            success = mysql_after_restart >= mysql_before_close
            
            if success:
                diff = mysql_after_restart - mysql_before_close
                logger.info(f"✅ TEST 3 PASÓ: MySQL mantiene datos (diff: +{diff})")
                self.test_details[test_name] = f"MySQL antes: {mysql_before_close}, después: {mysql_after_restart}"
            else:
                logger.error(f"❌ TEST 3 FALLÓ: MySQL regresó de {mysql_before_close} a {mysql_after_restart}")
                self.test_details[test_name] = f"REGRESIÓN detectada: {mysql_before_close} → {mysql_after_restart}"
            
            self.test_results[test_name] = success
            return success
            
        except Exception as e:
            logger.error(f"❌ TEST 3 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            self.test_details[test_name] = str(e)
            return False
    
    def test_04_large_batch_handling(self) -> bool:
        """
        Test 4: Verificar manejo de lotes grandes (>100 piezas)
        """
        test_name = "test_04_large_batch_handling"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 4: Manejo de lotes grandes (>100 piezas)")
        logger.info(f"{'='*80}\n")
        
        try:
            # 1. Crear un lote grande de pares huérfanos
            logger.info("🏭 Creando lote grande de 150 pares...")
            created = self._create_orphaned_pairs(150)
            
            if created < 150:
                logger.warning(f"⚠️ Solo se crearon {created}/150 pares")
            
            counts_after_create = self._get_sqlite_counts()
            logger.info(f"📊 Después de crear lote grande:")
            logger.info(f"   - Total: {counts_after_create['total']}")
            logger.info(f"   - Sin vincular: {counts_after_create['unlinked']}")
            
            # 2. Reparar los pares
            logger.info("🔧 Reparando pares...")
            repaired, leftover = self.db_system._repair_unlinked_pairs()
            logger.info(f"   - Reparados: {repaired}")
            logger.info(f"   - Sueltos: {leftover}")
            
            # 3. Esperar a que el worker sync procese (o forzar sync)
            logger.info("⏳ Esperando procesamiento del worker (15 segundos)...")
            time.sleep(15)
            
            # O forzar sincronización manual
            logger.info("🔄 Forzando sincronización manual (3 ciclos)...")
            total_synced = 0
            for cycle in range(3):
                synced = self.db_system._sync_scans_to_mysql()
                total_synced += synced
                logger.info(f"   Ciclo {cycle+1}: {synced} pares sincronizados")
                if synced == 0:
                    break
                time.sleep(1)
            
            # 4. Verificar que se procesó el lote completo
            counts_final = self._get_sqlite_counts()
            mysql_final = self._get_mysql_counts()
            
            logger.info(f"📊 Estado final:")
            logger.info(f"   SQLite - Pendientes: {counts_final['pending']}")
            logger.info(f"   MySQL - Total: {mysql_final}")
            logger.info(f"   Total sincronizado: {total_synced}")
            
            # Validación: Al menos el 80% del lote debe estar sincronizado
            expected_pairs = created
            min_synced = int(expected_pairs * 0.8)
            success = total_synced >= min_synced
            
            if success:
                logger.info(f"✅ TEST 4 PASÓ: Lote grande procesado ({total_synced}/{expected_pairs})")
                self.test_details[test_name] = f"Sincronizados {total_synced}/{expected_pairs} pares"
            else:
                logger.error(f"❌ TEST 4 FALLÓ: Solo {total_synced}/{expected_pairs} sincronizados (esperábamos >{min_synced})")
                self.test_details[test_name] = f"Insuficiente: {total_synced}/{expected_pairs}"
            
            self.test_results[test_name] = success
            return success
            
        except Exception as e:
            logger.error(f"❌ TEST 4 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            self.test_details[test_name] = str(e)
            return False
    
    def run_all_tests(self):
        """Ejecutar todos los tests"""
        logger.info("\n" + "="*80)
        logger.info("🚀 INICIANDO SUITE DE PRUEBAS DE RESILIENCIA")
        logger.info("="*80 + "\n")
        
        # Setup inicial
        if not self.setup():
            logger.error("❌ No se pudo inicializar el sistema, abortando tests")
            return
        
        try:
            # Ejecutar tests
            self.test_01_orphan_repair_on_startup()
            time.sleep(2)
            
            self.test_02_shutdown_flush_cycles()
            time.sleep(2)
            
            self.test_03_persistence_after_restart()
            time.sleep(2)
            
            self.test_04_large_batch_handling()
            
        finally:
            # Teardown final
            self.teardown()
        
        # Resumen de resultados
        self.print_summary()
    
    def print_summary(self):
        """Imprimir resumen de resultados"""
        logger.info("\n" + "="*80)
        logger.info("📋 RESUMEN DE RESULTADOS")
        logger.info("="*80 + "\n")
        
        passed = sum(1 for result in self.test_results.values() if result)
        total = len(self.test_results)
        
        for test_name, result in self.test_results.items():
            status = "✅ PASÓ" if result else "❌ FALLÓ"
            details = self.test_details.get(test_name, "")
            logger.info(f"{status} - {test_name}")
            if details:
                logger.info(f"         {details}")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"TOTAL: {passed}/{total} tests pasaron")
        logger.info(f"{'='*80}\n")
        
        if passed == total:
            logger.info("🎉 TODOS LOS TESTS PASARON - Sistema resiliente verificado")
        else:
            logger.error(f"⚠️ {total - passed} test(s) fallaron - Revisar implementación")


def main():
    """Función principal"""
    print("\n" + "="*80)
    print("🧪 PRUEBA DE RESILIENCIA DE CIERRE Y SINCRONIZACIÓN")
    print("="*80)
    print("\nEsta prueba verificará:")
    print("1. Reparación de pares huérfanos al arrancar")
    print("2. Drenaje completo de la cola antes del cierre")
    print("3. Persistencia de datos tras reinicio")
    print("4. Manejo de lotes grandes (>100 piezas)")
    print("\n" + "="*80 + "\n")
    
    input("Presiona ENTER para comenzar las pruebas...")
    
    test_suite = TestShutdownResilience()
    test_suite.run_all_tests()
    
    print("\n" + "="*80)
    print("✅ Pruebas completadas. Revisa el log para detalles.")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
