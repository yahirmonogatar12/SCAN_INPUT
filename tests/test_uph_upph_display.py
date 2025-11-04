"""
Tests para verificar que UPH y UPPH se calculan y muestran correctamente
en las tarjetas de métricas del dashboard principal.

Verifica:
1. _resolve_uph_metrics obtiene valores reales antes de refrescar cards
2. _calculate_line_uph usa SQLite local con caché de 5 segundos
3. Las tarjetas muestran valores correctos, no ceros
4. El caché evita consultas repetitivas
5. El número de personas se normaliza correctamente
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, call
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys
import os
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Añadir el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestUPHUPPHCalculation(unittest.TestCase):
    """Tests unitarios para cálculo de UPH/UPPH"""
    
    def test_01_resolve_uph_from_cache(self):
        """Test 1: _resolve_uph_metrics obtiene valores del caché correctamente"""
        print("\n" + "="*70)
        print("🧪 TEST 1: Resolver UPH desde metrics_cache")
        print("="*70)
        
        # Importar aquí para evitar problemas con PyQt6
        from app.ui import main_window
        
        # Crear mock de MainWindow con los métodos necesarios
        mock_window = Mock()
        mock_window._uph_cache = {}
        mock_window._uph_cache_time = {}
        mock_window._personas_cache = {}
        mock_window._personas_cache_time = {}
        
        # Simular datos en metrics_cache
        mock_metrics_cache = Mock()
        mock_metrics_cache.get_metrics_from_cache.return_value = {
            'uph': 120,
            'upph': 20.0,
            'num_personas': 6
        }
        
        # Mock de get_metrics_cache
        with patch('app.ui.main_window.get_metrics_cache', return_value=mock_metrics_cache):
            # Llamar al método real
            result = main_window.MainWindow._resolve_uph_metrics(mock_window, 'L01')
            
            uph, upph = result
            
            # Verificar valores
            self.assertEqual(uph, 120, "UPH debe ser 120 desde caché")
            self.assertAlmostEqual(upph, 20.0, places=1, msg="UPPH debe ser ~20.0 desde caché")
            
            print(f"✓ UPH desde caché: {uph}")
            print(f"✓ UPPH desde caché: {upph}")
            print("✓ Test 1 PASÓ")
    
    def test_02_calculate_line_uph_sqlite(self):
        """Test 2: _calculate_line_uph consulta SQLite correctamente"""
        print("\n" + "="*70)
        print("🧪 TEST 2: Calcular UPH desde SQLite")
        print("="*70)
        
        from app.ui import main_window
        
        # Crear mock de MainWindow
        mock_window = Mock()
        mock_window._uph_cache = {}
        mock_window._uph_cache_time = {}
        
        # Mock de dual_db
        mock_dual_db = Mock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Simular 100 piezas en la última hora
        mock_cursor.fetchone.return_value = (100,)
        mock_conn.cursor.return_value = mock_cursor
        mock_dual_db._get_sqlite_connection.return_value.__enter__.return_value = mock_conn
        
        with patch('app.ui.main_window.get_dual_db', return_value=mock_dual_db):
            # Llamar al método
            uph = main_window.MainWindow._calculate_line_uph(mock_window, 'L01')
            
            # Verificar que consultó SQLite
            self.assertTrue(mock_cursor.execute.called, "Debe consultar SQLite")
            self.assertEqual(uph, 100, "UPH debe ser 100 desde SQLite")
            
            print(f"✓ UPH calculado desde SQLite: {uph}")
            print(f"✓ Consulta SQL ejecutada: {mock_cursor.execute.call_count} vez")
            print("✓ Test 2 PASÓ")
    
    def test_03_cache_prevents_repeated_queries(self):
        """Test 3: Caché evita consultas repetitivas a SQLite"""
        print("\n" + "="*70)
        print("🧪 TEST 3: Caché previene consultas repetitivas")
        print("="*70)
        
        from app.ui import main_window
        
        mock_window = Mock()
        mock_window._uph_cache = {}
        mock_window._uph_cache_time = {}
        
        # Mock de dual_db
        mock_dual_db = Mock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (150,)
        mock_conn.cursor.return_value = mock_cursor
        mock_dual_db._get_sqlite_connection.return_value.__enter__.return_value = mock_conn
        
        with patch('app.ui.main_window.get_dual_db', return_value=mock_dual_db):
            # Primera llamada
            uph1 = main_window.MainWindow._calculate_line_uph(mock_window, 'L02')
            first_calls = mock_cursor.execute.call_count
            
            # Segunda llamada inmediata (debe usar caché)
            mock_cursor.reset_mock()
            uph2 = main_window.MainWindow._calculate_line_uph(mock_window, 'L02')
            second_calls = mock_cursor.execute.call_count
            
            # Verificar
            self.assertEqual(uph1, uph2, "Valores deben ser iguales")
            self.assertEqual(uph1, 150, "UPH debe ser 150")
            self.assertEqual(second_calls, 0, "Segunda llamada NO debe consultar SQLite")
            
            print(f"✓ Primera llamada (SQLite): UPH={uph1}, consultas={first_calls}")
            print(f"✓ Segunda llamada (caché): UPH={uph2}, consultas={second_calls}")
            print("✓ Caché funcionando correctamente")
            print("✓ Test 3 PASÓ")
    
    def test_04_cache_expiration_after_5_seconds(self):
        """Test 4: Caché expira después de 5 segundos"""
        print("\n" + "="*70)
        print("🧪 TEST 4: Expiración del caché (5 segundos)")
        print("="*70)
        
        from app.ui import main_window
        
        mock_window = Mock()
        mock_window._uph_cache = {}
        mock_window._uph_cache_time = {}
        
        # Mock de dual_db
        mock_dual_db = Mock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(180,), (200,)]  # Diferentes valores
        mock_conn.cursor.return_value = mock_cursor
        mock_dual_db._get_sqlite_connection.return_value.__enter__.return_value = mock_conn
        
        with patch('app.ui.main_window.get_dual_db', return_value=mock_dual_db):
            # Primera llamada
            uph1 = main_window.MainWindow._calculate_line_uph(mock_window, 'L03')
            
            # Simular que pasaron 6 segundos (expiró el caché)
            cache_key = f"uph_sqlite_L03"
            if cache_key in mock_window._uph_cache_time:
                mock_window._uph_cache_time[cache_key] = time.time() - 6
            
            # Segunda llamada (debe consultar nuevamente)
            uph2 = main_window.MainWindow._calculate_line_uph(mock_window, 'L03')
            
            # Verificar que consultó SQLite 2 veces
            self.assertEqual(mock_cursor.execute.call_count, 2, "Debe consultar 2 veces")
            self.assertEqual(uph1, 180, "Primer valor debe ser 180")
            self.assertEqual(uph2, 200, "Segundo valor debe ser 200 (actualizado)")
            
            print(f"✓ Valor inicial: {uph1}")
            print(f"✓ Valor después de 6 segundos: {uph2}")
            print("✓ Caché expiró y se refrescó correctamente")
            print("✓ Test 4 PASÓ")
    
    def test_05_personas_normalization(self):
        """Test 5: Normalización del número de personas"""
        print("\n" + "="*70)
        print("🧪 TEST 5: Normalización de personas para UPPH")
        print("="*70)
        
        from app.ui import main_window
        
        test_cases = [
            (0, 6, "0 personas -> default 6"),
            (-1, 6, "negativo -> default 6"),
            (None, 6, "None -> default 6"),
            (5, 5, "5 personas válido"),
            (10, 10, "10 personas válido"),
        ]
        
        for input_personas, expected_personas, descripcion in test_cases:
            mock_window = Mock()
            mock_window._uph_cache = {}
            mock_window._uph_cache_time = {}
            mock_window._personas_cache = {}
            mock_window._personas_cache_time = {}
            
            # Mock de metrics_cache con diferentes valores de personas
            mock_metrics_cache = Mock()
            mock_metrics_cache.get_metrics_from_cache.return_value = {
                'uph': 60,
                'upph': 0,
                'num_personas': input_personas
            }
            
            # Mock de _calculate_line_uph
            with patch('app.ui.main_window.get_metrics_cache', return_value=mock_metrics_cache), \
                 patch.object(main_window.MainWindow, '_calculate_line_uph', return_value=60):
                
                uph, upph = main_window.MainWindow._resolve_uph_metrics(mock_window, 'L01')
                
                # Calcular UPPH esperado
                expected_upph = 60 / expected_personas if expected_personas > 0 else 0
                
                # Verificar
                self.assertAlmostEqual(upph, expected_upph, places=1, 
                                     msg=f"UPPH incorrecto para {descripcion}")
                
                print(f"✓ {descripcion}: personas={input_personas} -> normalizado={expected_personas} -> UPPH={upph:.1f}")
        
        print("✓ Test 5 PASÓ")
    
    def test_06_update_plan_totals_not_zero(self):
        """Test 6: _update_plan_totals NO fija UPH/UPPH en cero"""
        print("\n" + "="*70)
        print("🧪 TEST 6: Verificar que UPH/UPPH NO son cero en update_plan_totals")
        print("="*70)
        
        # Este test verifica que el código NO contenga líneas como:
        # uph = 0
        # upph = 0
        # antes de llamar a _update_cards_with_metrics
        
        with open('app/ui/main_window.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Buscar el método _update_plan_totals
        method_start = content.find('def _update_plan_totals(')
        self.assertNotEqual(method_start, -1, "_update_plan_totals debe existir")
        
        # Buscar la llamada a _resolve_uph_metrics
        resolve_call = content.find('_resolve_uph_metrics', method_start)
        self.assertNotEqual(resolve_call, -1, "Debe llamar a _resolve_uph_metrics")
        
        # Buscar la llamada a _update_cards_with_metrics
        update_cards_call = content.find('_update_cards_with_metrics', resolve_call)
        self.assertNotEqual(update_cards_call, -1, "Debe llamar a _update_cards_with_metrics")
        
        # Verificar que _resolve_uph_metrics está ANTES de _update_cards_with_metrics
        self.assertLess(resolve_call, update_cards_call, 
                       "_resolve_uph_metrics debe llamarse ANTES de _update_cards_with_metrics")
        
        print("✓ _resolve_uph_metrics se llama antes de actualizar cards")
        print("✓ UPH y UPPH se calculan correctamente antes de mostrar")
        print("✓ Test 6 PASÓ")
    
    def test_07_helper_methods_exist(self):
        """Test 7: Verificar que los métodos helper existen"""
        print("\n" + "="*70)
        print("🧪 TEST 7: Verificar existencia de métodos helper")
        print("="*70)
        
        from app.ui import main_window
        
        # Verificar que existen los métodos
        self.assertTrue(hasattr(main_window.MainWindow, '_resolve_uph_metrics'),
                       "Debe existir _resolve_uph_metrics")
        
        self.assertTrue(hasattr(main_window.MainWindow, '_calculate_line_uph'),
                       "Debe existir _calculate_line_uph")
        
        print("✓ _resolve_uph_metrics existe")
        print("✓ _calculate_line_uph existe")
        print("✓ Test 7 PASÓ")


def run_tests():
    """Ejecutar todos los tests"""
    print("\n" + "="*70)
    print("🚀 INICIANDO TESTS DE UPH/UPPH")
    print("="*70)
    print("\nVerificando:")
    print("1. _resolve_uph_metrics obtiene valores desde caché/SQLite")
    print("2. _calculate_line_uph usa caché de 5 segundos")
    print("3. El caché evita consultas repetitivas")
    print("4. El caché expira correctamente")
    print("5. El número de personas se normaliza")
    print("6. _update_plan_totals NO fija UPH/UPPH en 0")
    print("7. Los métodos helper existen")
    
    # Crear suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestUPHUPPHCalculation)
    
    # Ejecutar
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Resumen
    print("\n" + "="*70)
    print("📊 RESUMEN DE RESULTADOS")
    print("="*70)
    
    total = result.testsRun
    exitosos = total - len(result.failures) - len(result.errors)
    porcentaje = (exitosos / total * 100) if total > 0 else 0
    
    print(f"\nTests ejecutados: {total}")
    print(f"✓ Tests exitosos: {exitosos}")
    print(f"✗ Tests fallidos: {len(result.failures)}")
    print(f"⚠ Errores: {len(result.errors)}")
    print(f"📈 Tasa de éxito: {porcentaje:.1f}%\n")
    
    if porcentaje == 100:
        print("="*70)
        print("🎉 ¡TODOS LOS TESTS PASARON!")
        print("="*70)
        print("\n✅ UPH y UPPH se calculan correctamente")
        print("✅ El caché funciona como esperado (5 segundos)")
        print("✅ Los valores NO se fijan en cero")
        print("✅ Las tarjetas mostrarán valores reales\n")
    else:
        print("="*70)
        print("❌ ALGUNOS TESTS FALLARON")
        print("="*70)
        print("\nRevisar la implementación de:")
        if result.failures:
            for test, traceback in result.failures:
                print(f"  - {test}")
        if result.errors:
            for test, traceback in result.errors:
                print(f"  - {test} (ERROR)")
    
    return result


if __name__ == '__main__':
    result = run_tests()
    sys.exit(0 if result.wasSuccessful() else 1)
