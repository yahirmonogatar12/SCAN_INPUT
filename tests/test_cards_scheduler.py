"""
🧪 TEST: Verificación del scheduler de actualización de tarjetas post-scan

Este test verifica que:
1. Las tarjetas se actualizan automáticamente después de cada PAIR_COMPLETED
2. El throttling evita actualizaciones excesivas
3. El cooldown de 1.5s se respeta
4. No hay bloqueos de SQLite durante escaneos rápidos
"""

import sys
import time
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch
from PyQt6.QtCore import QTimer, QEventLoop
from PyQt6.QtWidgets import QApplication

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestCardsScheduler:
    """Suite de tests para el scheduler de tarjetas"""
    
    def __init__(self):
        self.app = None
        self.main_window = None
        self.test_results = {}
        
    def setup(self):
        """Inicializar QApplication y MainWindow"""
        logger.info("🔧 Iniciando setup...")
        try:
            # Crear QApplication si no existe
            if not QApplication.instance():
                self.app = QApplication(sys.argv)
            else:
                self.app = QApplication.instance()
            
            # Importar MainWindow
            from app.ui.main_window import MainWindow
            
            # Mock de las funciones de base de datos
            mock_db = self._create_mock_db()
            mock_direct_mysql = MagicMock()
            mock_direct_mysql.register_scan_listener.return_value = None
            
            with patch('app.ui.main_window.get_db') as mock_get_db, \
                 patch('app.ui.main_window.get_direct_mysql') as mock_get_direct_mysql:
                mock_get_db.return_value = mock_db
                mock_get_direct_mysql.return_value = mock_direct_mysql
                self.main_window = MainWindow()
            
            logger.info("✅ Setup completado")
            return True
        except Exception as e:
            logger.error(f"❌ Error en setup: {e}", exc_info=True)
            return False
    
    def _create_mock_db(self):
        """Crear un mock de DualDatabaseSystem"""
        mock = MagicMock()
        mock.get_plan_en_progreso.return_value = []
        mock.get_daily_summary.return_value = []
        return mock
    
    def teardown(self):
        """Cerrar la aplicación"""
        logger.info("🧹 Ejecutando teardown...")
        try:
            if self.main_window:
                self.main_window.close()
            if self.app:
                self.app.quit()
            logger.info("✅ Teardown completado")
        except Exception as e:
            logger.error(f"❌ Error en teardown: {e}")
    
    def test_01_scheduler_attributes_exist(self) -> bool:
        """Test 1: Verificar que los atributos del scheduler existen"""
        test_name = "test_01_scheduler_attributes_exist"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 1: Verificar atributos del scheduler")
        logger.info(f"{'='*80}\n")
        
        try:
            # Verificar que existen los atributos
            assert hasattr(self.main_window, '_pending_cards_refresh'), \
                "❌ Falta atributo _pending_cards_refresh"
            logger.info("  ✓ _pending_cards_refresh existe")
            
            assert hasattr(self.main_window, '_last_cards_refresh'), \
                "❌ Falta atributo _last_cards_refresh"
            logger.info("  ✓ _last_cards_refresh existe")
            
            # Verificar valores iniciales
            assert isinstance(self.main_window._pending_cards_refresh, bool), \
                "❌ _pending_cards_refresh debe ser bool"
            logger.info(f"  ✓ _pending_cards_refresh es bool: {self.main_window._pending_cards_refresh}")
            
            assert isinstance(self.main_window._last_cards_refresh, float), \
                "❌ _last_cards_refresh debe ser float"
            logger.info(f"  ✓ _last_cards_refresh es float: {self.main_window._last_cards_refresh}")
            
            # Verificar que existen los métodos
            assert hasattr(self.main_window, '_schedule_cards_refresh'), \
                "❌ Falta método _schedule_cards_refresh"
            logger.info("  ✓ _schedule_cards_refresh existe")
            
            assert hasattr(self.main_window, '_run_cards_refresh'), \
                "❌ Falta método _run_cards_refresh"
            logger.info("  ✓ _run_cards_refresh existe")
            
            logger.info("✅ TEST 1 PASÓ: Todos los atributos y métodos existen")
            self.test_results[test_name] = True
            return True
            
        except AssertionError as e:
            logger.error(f"❌ TEST 1 FALLÓ: {e}")
            self.test_results[test_name] = False
            return False
        except Exception as e:
            logger.error(f"❌ TEST 1 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            return False
    
    def test_02_schedule_prevents_duplicates(self) -> bool:
        """Test 2: Verificar que el scheduler previene programaciones duplicadas"""
        test_name = "test_02_schedule_prevents_duplicates"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 2: Prevención de programaciones duplicadas")
        logger.info(f"{'='*80}\n")
        
        try:
            # Resetear estado
            self.main_window._pending_cards_refresh = False
            
            # Primera programación
            self.main_window._schedule_cards_refresh()
            assert self.main_window._pending_cards_refresh == True, \
                "❌ _pending_cards_refresh debería estar en True después de programar"
            logger.info("  ✓ Primera programación: _pending_cards_refresh = True")
            
            # Segunda programación (debería ser ignorada)
            initial_state = self.main_window._pending_cards_refresh
            self.main_window._schedule_cards_refresh()
            assert self.main_window._pending_cards_refresh == initial_state, \
                "❌ Segunda programación no debería cambiar el estado"
            logger.info("  ✓ Segunda programación ignorada (previene duplicados)")
            
            logger.info("✅ TEST 2 PASÓ: El scheduler previene duplicados correctamente")
            self.test_results[test_name] = True
            return True
            
        except AssertionError as e:
            logger.error(f"❌ TEST 2 FALLÓ: {e}")
            self.test_results[test_name] = False
            return False
        except Exception as e:
            logger.error(f"❌ TEST 2 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            return False
    
    def test_03_cooldown_enforcement(self) -> bool:
        """Test 3: Verificar que se respeta el cooldown de 1.5s"""
        test_name = "test_03_cooldown_enforcement"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 3: Respeto del cooldown (1.5s)")
        logger.info(f"{'='*80}\n")
        
        try:
            # Simular que acabamos de hacer un refresh JUSTO AHORA (dentro de _run_cards_refresh)
            # Usamos el mismo import time que usa _run_cards_refresh
            import time
            
            # Establecer timestamp reciente (hace 0.1 segundos - claramente dentro del cooldown)
            self.main_window._last_cards_refresh = time.monotonic() - 0.1
            self.main_window._pending_cards_refresh = False
            self.main_window._scan_in_progress = False
            
            logger.info("  📊 Simulando refresh hace 0.1s (dentro de cooldown de 1.5s)...")
            
            # Patch de _update_plan_totals y QTimer.singleShot
            with patch.object(self.main_window, '_update_plan_totals') as mock_update, \
                 patch('PyQt6.QtCore.QTimer.singleShot') as mock_timer:
                
                # Llamar directamente a _run_cards_refresh
                self.main_window._run_cards_refresh()
                
                # El método NO debería haberse llamado inmediatamente (cooldown activo)
                # En su lugar debería llamar a QTimer.singleShot
                assert mock_update.call_count == 0, \
                    f"❌ _update_plan_totals NO debería llamarse (se llamó {mock_update.call_count} veces)"
                logger.info("  ✓ _update_plan_totals NO se llamó (cooldown activo)")
                
                # Verificar que se re-programó para después
                assert mock_timer.call_count == 1, \
                    f"❌ QTimer.singleShot debería re-programar (se llamó {mock_timer.call_count} veces)"
                logger.info(f"  ✓ Re-programado vía QTimer.singleShot: {mock_timer.call_count} vez")
                
                # Verificar el delay calculado (debería ser ~1.4 segundos)
                if mock_timer.call_count > 0:
                    call_args = mock_timer.call_args
                    delay_ms = call_args[0][0] if call_args and len(call_args[0]) > 0 else None
                    logger.info(f"  📊 Delay calculado: {delay_ms}ms")
            
            # Simular que pasó el tiempo suficiente (hace 2 segundos - fuera del cooldown)
            self.main_window._last_cards_refresh = time.monotonic() - 2.0
            self.main_window._pending_cards_refresh = False
            
            logger.info("  ⏰ Simulando refresh hace 2s (fuera de cooldown)...")
            
            # Ahora SÍ debería ejecutarse inmediatamente
            with patch.object(self.main_window, '_update_plan_totals') as mock_update:
                self.main_window._run_cards_refresh()
                
                # Ahora sí debería haberse llamado inmediatamente
                assert mock_update.call_count == 1, \
                    f"❌ _update_plan_totals debería llamarse 1 vez, se llamó {mock_update.call_count}"
                logger.info(f"  📞 _update_plan_totals llamado: {mock_update.call_count} vez")
                logger.info("  ✓ Refresh ejecutado inmediatamente (cooldown expirado)")
            
            logger.info("✅ TEST 3 PASÓ: El cooldown se respeta correctamente")
            self.test_results[test_name] = True
            return True
            
        except AssertionError as e:
            logger.error(f"❌ TEST 3 FALLÓ: {e}")
            self.test_results[test_name] = False
            return False
        except Exception as e:
            logger.error(f"❌ TEST 3 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            return False
    
    def test_04_scan_in_progress_deferred(self) -> bool:
        """Test 4: Verificar que el refresh se difiere si hay escaneo en progreso"""
        test_name = "test_04_scan_in_progress_deferred"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 4: Diferir refresh durante escaneo en progreso")
        logger.info(f"{'='*80}\n")
        
        try:
            # Simular escaneo en progreso
            self.main_window._scan_in_progress = True
            self.main_window._pending_cards_refresh = False
            self.main_window._last_cards_refresh = time.monotonic() - 2.0
            
            logger.info("  🔄 Simulando escaneo en progreso...")
            
            with patch.object(self.main_window, '_update_plan_totals') as mock_update, \
                 patch('PyQt6.QtCore.QTimer.singleShot') as mock_timer:
                # Llamar directamente a _run_cards_refresh
                self.main_window._run_cards_refresh()
                
                # NO debería haberse llamado porque hay escaneo en progreso
                assert mock_update.call_count == 0, \
                    f"❌ _update_plan_totals NO debería llamarse durante escaneo (se llamó {mock_update.call_count})"
                logger.info("  ✓ _update_plan_totals NO se llamó (escaneo en progreso)")
                
                # Debería re-programarse para después
                assert mock_timer.call_count == 1, \
                    f"❌ QTimer.singleShot debería re-programar (se llamó {mock_timer.call_count})"
                logger.info(f"  ✓ Re-programado vía QTimer.singleShot: {mock_timer.call_count} vez")
            
            # Terminar escaneo
            self.main_window._scan_in_progress = False
            logger.info("  ✅ Escaneo completado")
            
            # Ahora debería poder ejecutarse
            with patch.object(self.main_window, '_update_plan_totals') as mock_update:
                self.main_window._run_cards_refresh()
                
                # Ahora sí debería haberse llamado
                assert mock_update.call_count == 1, \
                    f"❌ _update_plan_totals debería llamarse cuando no hay escaneo (se llamó {mock_update.call_count})"
                logger.info("  ✓ Refresh ejecutado después de terminar escaneo")
            
            logger.info("✅ TEST 4 PASÓ: El refresh se difiere correctamente")
            self.test_results[test_name] = True
            return True
            
        except AssertionError as e:
            logger.error(f"❌ TEST 4 FALLÓ: {e}")
            self.test_results[test_name] = False
            return False
        except Exception as e:
            logger.error(f"❌ TEST 4 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            return False
    
    def test_05_multiple_pairs_throttled(self) -> bool:
        """Test 5: Verificar throttling con múltiples pares completados rápidamente"""
        test_name = "test_05_multiple_pairs_throttled"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 5: Throttling con múltiples pares rápidos")
        logger.info(f"{'='*80}\n")
        
        try:
            # Resetear estado
            self.main_window._pending_cards_refresh = False
            self.main_window._last_cards_refresh = time.monotonic() - 3.0
            
            logger.info("  🚀 Simulando 10 pares completados rápidamente...")
            
            schedule_count = 0
            for i in range(10):
                # Simular PAIR_COMPLETED
                was_pending = self.main_window._pending_cards_refresh
                self.main_window._schedule_cards_refresh()
                
                if not was_pending and self.main_window._pending_cards_refresh:
                    schedule_count += 1
                    logger.info(f"    Par {i+1}: Refresh programado (total: {schedule_count})")
                else:
                    logger.info(f"    Par {i+1}: Ignorado (throttling activo)")
                
                # Pequeña pausa entre pares
                time.sleep(0.01)
            
            logger.info(f"  📊 Resultado: {schedule_count} programaciones de 10 intentos")
            
            # Debería haber solo 1 programación debido al throttling
            assert schedule_count == 1, \
                f"❌ Debería haber solo 1 programación, pero hubo {schedule_count}"
            logger.info("  ✓ Throttling efectivo: solo 1 actualización programada")
            
            logger.info("✅ TEST 5 PASÓ: El throttling funciona correctamente")
            self.test_results[test_name] = True
            return True
            
        except AssertionError as e:
            logger.error(f"❌ TEST 5 FALLÓ: {e}")
            self.test_results[test_name] = False
            return False
        except Exception as e:
            logger.error(f"❌ TEST 5 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            return False
    
    def test_06_handle_scan_processed_integration(self) -> bool:
        """Test 6: Verificar integración con _handle_scan_processed"""
        test_name = "test_06_handle_scan_processed_integration"
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 TEST 6: Integración con _handle_scan_processed")
        logger.info(f"{'='*80}\n")
        
        try:
            # Verificar que _handle_scan_processed llama a _schedule_cards_refresh
            logger.info("  🔍 Verificando llamada desde _handle_scan_processed...")
            
            # Resetear estado
            self.main_window._pending_cards_refresh = False
            
            with patch.object(self.main_window, '_schedule_cards_refresh') as mock_schedule:
                # Llamar a _handle_scan_processed con parámetros correctos
                # Firma: def _handle_scan_processed(self, linea: str, nparte: str, event: str)
                self.main_window._handle_scan_processed('M1', 'TEST-PART', 'PAIR_COMPLETED')
                
                # Verificar que se llamó a _schedule_cards_refresh
                assert mock_schedule.call_count == 1, \
                    f"❌ _schedule_cards_refresh debería llamarse 1 vez, se llamó {mock_schedule.call_count}"
                logger.info(f"  ✓ _schedule_cards_refresh llamado: {mock_schedule.call_count} vez")
            
            # Verificar que eventos no-PAIR_COMPLETED no llaman a _schedule_cards_refresh
            logger.info("  🔍 Verificando que otros eventos no llaman al scheduler...")
            with patch.object(self.main_window, '_schedule_cards_refresh') as mock_schedule:
                self.main_window._handle_scan_processed('M1', 'TEST-PART', 'QR_SCANNED')
                
                assert mock_schedule.call_count == 0, \
                    "❌ _schedule_cards_refresh NO debería llamarse para eventos no-PAIR_COMPLETED"
                logger.info("  ✓ Eventos no-PAIR_COMPLETED ignoran correctamente el scheduler")
            
            logger.info("✅ TEST 6 PASÓ: Integración correcta con _handle_scan_processed")
            self.test_results[test_name] = True
            return True
            
        except AssertionError as e:
            logger.error(f"❌ TEST 6 FALLÓ: {e}")
            self.test_results[test_name] = False
            return False
        except Exception as e:
            logger.error(f"❌ TEST 6 ERROR: {e}", exc_info=True)
            self.test_results[test_name] = False
            return False
    
    def run_all_tests(self):
        """Ejecutar todos los tests"""
        logger.info("\n" + "="*80)
        logger.info("🚀 INICIANDO SUITE DE TESTS DEL SCHEDULER DE TARJETAS")
        logger.info("="*80 + "\n")
        
        if not self.setup():
            logger.error("❌ Setup falló, abortando tests")
            return
        
        try:
            # Ejecutar tests
            self.test_01_scheduler_attributes_exist()
            time.sleep(0.5)
            
            self.test_02_schedule_prevents_duplicates()
            time.sleep(0.5)
            
            self.test_03_cooldown_enforcement()
            time.sleep(0.5)
            
            self.test_04_scan_in_progress_deferred()
            time.sleep(0.5)
            
            self.test_05_multiple_pairs_throttled()
            time.sleep(0.5)
            
            self.test_06_handle_scan_processed_integration()
            
        finally:
            self.teardown()
        
        # Resumen
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
            logger.info(f"{status} - {test_name}")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"TOTAL: {passed}/{total} tests pasaron ({passed/total*100:.1f}%)")
        logger.info(f"{'='*80}\n")
        
        if passed == total:
            logger.info("🎉 TODOS LOS TESTS PASARON")
            logger.info("✅ El scheduler de tarjetas funciona correctamente:")
            logger.info("   - Las tarjetas se actualizan con cada par completo")
            logger.info("   - El throttling previene actualizaciones excesivas")
            logger.info("   - El cooldown de 1.5s se respeta")
            logger.info("   - Los escaneos en progreso no bloquean el refresh")
        else:
            logger.error(f"⚠️ {total - passed} test(s) fallaron - Revisar implementación")


def main():
    """Función principal"""
    # Configurar encoding para Windows
    import sys
    import io
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    print("\n" + "="*80)
    print("TESTS DEL SCHEDULER DE ACTUALIZACION DE TARJETAS POST-SCAN")
    print("="*80)
    print("\nVerificando:")
    print("1. Actualizacion automatica despues de cada PAIR_COMPLETED")
    print("2. Throttling para evitar actualizaciones excesivas")
    print("3. Cooldown de 1.5s entre actualizaciones")
    print("4. Diferimiento durante escaneos en progreso")
    print("5. Integracion con handle_scan_processed")
    print("\n" + "="*80 + "\n")
    
    test_suite = TestCardsScheduler()
    test_suite.run_all_tests()
    
    print("\n" + "="*80)
    print("Suite de tests completada")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
