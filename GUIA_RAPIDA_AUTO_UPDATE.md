# Template de Auto-Actualización - Guía Rápida de Implementación

## Resumen Ultra-Rápido

Este sistema permite que tus aplicaciones PyQt6 se actualicen automáticamente desde una carpeta de red.

**Tiempo de implementación: 15 minutos**

## Archivos Necesarios

```
TU_PROYECTO/
├── app/
│   ├── services/
│   │   └── auto_update.py          ← COPIAR del proyecto INPUT SCAN
│   ├── ui/
│   │   └── update_dialog.py        ← COPIAR del proyecto INPUT SCAN
│   ├── config.py                    ← MODIFICAR (agregar variables)
│   └── main.py                      ← MODIFICAR (agregar check de updates)
├── version.txt                      ← CREAR con "1.0.0"
└── .env                            ← AGREGAR 6 variables
```

## Paso 1: Copiar Archivos Core (2 min)

Copia estos 2 archivos de INPUT SCAN a tu proyecto:

1. `app/services/auto_update.py` → Tu proyecto
2. `app/ui/update_dialog.py` → Tu proyecto

**No necesitas modificarlos, funcionan tal cual.**

## Paso 2: Crear version.txt (30 seg)

```plaintext
1.0.0
```

## Paso 3: Agregar Variables al .env (1 min)

```ini
# Auto-actualización
AUTO_CHECK_UPDATES=True
AUTO_INSTALL_UPDATES=True
SILENT_UPDATE_INSTALL=True
UPDATE_NETWORK_PATH=\\\\TU_SERVIDOR\\Updates\\TuApp
UPDATE_NETWORK_USER=tu_usuario
UPDATE_NETWORK_PASSWORD=tu_password
```

## Paso 4: Modificar config.py (3 min)

```python
from pathlib import Path
import os

ROOT_DIR = Path(__file__).parent.parent

# Leer versión
VERSION_FILE = ROOT_DIR / "version.txt"
APP_VERSION = VERSION_FILE.read_text(encoding='utf-8').strip() if VERSION_FILE.exists() else "1.0.0"

# Auto-actualización
AUTO_CHECK_UPDATES = os.getenv("AUTO_CHECK_UPDATES", "True") == "True"
AUTO_INSTALL_UPDATES = os.getenv("AUTO_INSTALL_UPDATES", "True") == "True"
SILENT_UPDATE_INSTALL = os.getenv("SILENT_UPDATE_INSTALL", "True") == "True"
UPDATE_NETWORK_PATH = os.getenv("UPDATE_NETWORK_PATH", "")
UPDATE_NETWORK_USER = os.getenv("UPDATE_NETWORK_USER", "")
UPDATE_NETWORK_PASSWORD = os.getenv("UPDATE_NETWORK_PASSWORD", "")
```

## Paso 5: Integrar en main.py (5 min)

```python
from PyQt6 import QtWidgets
from PyQt6.QtCore import QTimer
import sys
from .config import (
    APP_VERSION, AUTO_CHECK_UPDATES, AUTO_INSTALL_UPDATES,
    SILENT_UPDATE_INSTALL, UPDATE_NETWORK_PATH,
    UPDATE_NETWORK_USER, UPDATE_NETWORK_PASSWORD
)

def main():
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    
    # ✅ Verificar actualizaciones 1 segundo después de mostrar ventana
    if AUTO_CHECK_UPDATES:
        QTimer.singleShot(1000, check_updates_async)
    
    sys.exit(app.exec())

def check_updates_async():
    """Verifica actualizaciones en background sin bloquear UI"""
    try:
        from .services.auto_update import AutoUpdater
        from .ui.update_dialog import UpdateDialog
        import logging
        
        logging.info("🔍 Verificando actualizaciones...")
        
        updater = AutoUpdater(
            UPDATE_NETWORK_PATH,
            APP_VERSION,
            UPDATE_NETWORK_USER,
            UPDATE_NETWORK_PASSWORD
        )
        
        has_update, new_version, installer_path = updater.check_for_updates()
        
        if has_update and installer_path:
            logging.info(f"📦 Nueva versión disponible: {new_version}")
            
            if AUTO_INSTALL_UPDATES:
                # Mostrar diálogo con countdown
                dialog = UpdateDialog(
                    current_version=APP_VERSION,
                    new_version=new_version,
                    release_notes="Nueva versión disponible",
                    parent=win
                )
                
                # Conectar señal de aceptación
                def on_accept():
                    if updater.install_update(installer_path, silent=SILENT_UPDATE_INSTALL):
                        sys.exit(0)  # Cerrar app
                
                dialog.update_accepted.connect(on_accept)
                dialog.exec()  # Bloquea hasta que usuario acepta o timeout (60s)
        else:
            logging.info("✅ El programa está actualizado")
    
    except Exception as e:
        logging.error(f"❌ Error verificando actualizaciones: {e}")
```

## Paso 6: Incluir version.txt en PyInstaller (2 min)

```python
# tu_app.spec
a = Analysis(
    ['run.py'],
    datas=[
        ('version.txt', '.'),           # ← AGREGAR ESTA LÍNEA
        ('app', 'app'),
        # ... resto de tus archivos
    ],
    # ... resto de configuración
)
```

## Paso 7: Preparar Carpeta de Red (2 min)

Crea esta estructura en tu servidor:

```
\\SERVIDOR\Updates\TuApp\
├── update_info.json
└── TuApp_Setup_v1.0.1.exe
```

**update_info.json:**
```json
{
    "version": "1.0.1",
    "installer": "TuApp_Setup_v1.0.1.exe",
    "release_notes": "• Nueva funcionalidad\n• Correcciones de bugs",
    "release_date": "2025-10-21 10:00:00",
    "minimum_version": "1.0.0"
}
```

## ¡Listo! Cómo Funciona

1. Usuario abre tu app (versión 1.0.0)
2. App verifica carpeta de red (1 segundo después)
3. Encuentra versión 1.0.1 disponible
4. Muestra diálogo moderno con countdown de 60 segundos
5. Usuario puede actualizar inmediatamente o esperar countdown
6. App se cierra, instalador se ejecuta automáticamente
7. Usuario abre app con nueva versión (1.0.1)

## Workflow de Desarrollo

### Cuando liberas nueva versión:

```powershell
# 1. Actualizar versión
echo "1.0.1" > version.txt

# 2. Compilar
pyinstaller tu_app.spec --clean

# 3. Crear instalador (NSIS/Inno Setup)
# Nombre: TuApp_Setup_v1.0.1.exe

# 4. Copiar a red
copy installers\TuApp_Setup_v1.0.1.exe \\SERVIDOR\Updates\TuApp\

# 5. Actualizar JSON
# Editar update_info.json con nueva versión
```

### Los usuarios se actualizan automáticamente

- Al abrir la app (si hay update)
- 60 segundos de countdown
- Instalación silenciosa
- App se reinicia con nueva versión

## Personalización Rápida

### Cambiar tiempo de countdown:
```python
# update_dialog.py, línea ~27
self.auto_update_seconds = 30  # En vez de 60
```

### Hacer actualización opcional (no obligatoria):
```python
# update_dialog.py, línea ~409
def closeEvent(self, event):
    event.accept()  # En vez de event.ignore()
```

### Sin countdown (actualizar inmediatamente):
```python
# update_dialog.py, eliminar líneas 385-390 (start_countdown)
```

## Dependencias

Solo necesitas:
- PyQt6
- Python 3.11+
- Acceso a carpeta de red

**No requiere librerías adicionales** - usa solo módulos estándar de Python.

## Testing Local

Sin carpeta de red, puedes probar con carpeta local:

```ini
# .env para testing
UPDATE_NETWORK_PATH=C:\Users\tu_usuario\Desktop\Updates
UPDATE_NETWORK_USER=
UPDATE_NETWORK_PASSWORD=
```

Crea la carpeta y pon el update_info.json allí.

## Troubleshooting

**"Carpeta no accesible"**
→ Verifica UPDATE_NETWORK_PATH, credenciales y red

**"Instalador no encontrado"**
→ Verifica que nombre en JSON coincida con archivo .exe

**No actualiza**
→ Verifica que version.txt se incluye en compilación (spec file)

**Diálogo no aparece**
→ Verifica logs: `logging.info("🔍 Verificando actualizaciones...")`

## Resumen

**Solo necesitas:**
1. Copiar 2 archivos Python
2. Agregar 6 variables al .env
3. Modificar config.py (8 líneas)
4. Modificar main.py (25 líneas)
5. Agregar version.txt al spec (1 línea)

**Total: ~40 líneas de código + 2 archivos**

**Resultado: Sistema completo de auto-actualización**

---

Para documentación completa, ver: `DOCUMENTACION_AUTO_UPDATE.md`
