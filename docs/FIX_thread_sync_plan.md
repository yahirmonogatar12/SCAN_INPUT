# 🐛 FIX: Error "Thread object has no attribute _sync_plan_from_mysql"

## 📋 Problema
Al cambiar de día (medianoche), la aplicación intentaba recargar el plan de producción pero fallaba con el error:
```
ERROR: ❌ Error recargando plan para nuevo día: 'Thread' object has no attribute '_sync_plan_from_mysql'
```

## 🔍 Causa Raíz
En `app/ui/main_window.py` línea 4292, el código intentaba llamar al método `_sync_plan_from_mysql()` desde el **thread worker** en lugar de desde la instancia de `DualDatabaseSystem`:

```python
# ❌ CÓDIGO INCORRECTO (antes)
if hasattr(dual_db, '_sync_worker') and dual_db._sync_worker:
    dual_db._sync_worker._sync_plan_from_mysql()  # ❌ Thread no tiene este método
```

### ¿Por qué fallaba?
- `dual_db._sync_worker` es un objeto `Thread` (threading.Thread)
- Los threads **NO** tienen el método `_sync_plan_from_mysql()`
- Este método pertenece a la clase `DualDatabaseSystem`, no al thread

## ✅ Solución Implementada
Cambiar la llamada para que acceda al método directamente desde `dual_db`:

```python
# ✅ CÓDIGO CORRECTO (después)
if dual_db and hasattr(dual_db, '_sync_plan_from_mysql'):
    dual_db._sync_plan_from_mysql()  # ✅ Llamar desde dual_db directamente
```

## 📁 Archivo Modificado
- `app/ui/main_window.py` - líneas 4289-4293

## 🧪 Verificación
Se creó un test en `tests/test_sync_plan_fix.py` que verifica:
1. ✅ `dual_db` tiene el método `_sync_plan_from_mysql`
2. ✅ El método es callable
3. ✅ `_sync_worker` (Thread) NO tiene este método

**Resultado del test:** ✅ PASÓ

## 📊 Impacto
- **Antes**: Al cambiar de día, la app crasheaba y no recargaba el plan
- **Después**: Al cambiar de día, la app recarga correctamente el plan desde MySQL
- **Áreas afectadas**: Transición automática de día (medianoche)
- **Severidad corregida**: CRÍTICA (bloqueaba operación diaria)

## 🎯 Prevención Futura
Este tipo de error puede prevenirse con:
1. Type hints más estrictos
2. Tests unitarios para métodos críticos
3. Documentación clara de la arquitectura de threads

---
**Fecha de fix:** 30 de octubre de 2025
**Verificado por:** Test automático en `tests/test_sync_plan_fix.py`
