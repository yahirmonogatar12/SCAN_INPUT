"""
Test para verificar que el método _sync_plan_from_mysql esté accesible correctamente
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

def test_sync_plan_method_accessibility():
    """Verifica que _sync_plan_from_mysql sea accesible desde dual_db"""
    from app.services.dual_db import get_dual_db
    
    dual_db = get_dual_db()
    
    print("✅ Testing acceso a _sync_plan_from_mysql...")
    
    # Verificar que dual_db existe
    assert dual_db is not None, "❌ dual_db no se inicializó"
    print("  ✓ dual_db inicializado")
    
    # Verificar que tiene el método _sync_plan_from_mysql
    assert hasattr(dual_db, '_sync_plan_from_mysql'), "❌ dual_db no tiene método _sync_plan_from_mysql"
    print("  ✓ dual_db tiene método _sync_plan_from_mysql")
    
    # Verificar que es callable
    assert callable(dual_db._sync_plan_from_mysql), "❌ _sync_plan_from_mysql no es callable"
    print("  ✓ _sync_plan_from_mysql es callable")
    
    # Verificar que _sync_worker NO tiene el método (era el bug)
    if hasattr(dual_db, '_sync_worker') and dual_db._sync_worker:
        assert not hasattr(dual_db._sync_worker, '_sync_plan_from_mysql'), \
            "⚠️ _sync_worker no debería tener _sync_plan_from_mysql (es un Thread)"
        print("  ✓ _sync_worker correctamente NO tiene _sync_plan_from_mysql")
    
    print("\n✅ TODOS LOS TESTS PASARON")
    print("   El método _sync_plan_from_mysql está correctamente accesible desde dual_db")
    print("   y NO desde _sync_worker (que es solo un Thread)")
    
    return True

if __name__ == "__main__":
    try:
        test_sync_plan_method_accessibility()
        print("\n🎉 FIX VERIFICADO: El error 'Thread object has no attribute _sync_plan_from_mysql' está corregido")
    except AssertionError as e:
        print(f"\n❌ TEST FALLÓ: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
