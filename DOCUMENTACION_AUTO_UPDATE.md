# Sistema de Auto-Actualización - Documentación Completa

## Descripción General

Sistema completo de auto-actualización para aplicaciones PyQt6 que verifica, descarga e instala actualizaciones desde una carpeta compartida de red. El sistema incluye:

- Verificación automática de actualizaciones al iniciar
- Comparación de versiones semánticas (X.Y.Z)
- Descarga desde carpeta de red (con autenticación)
- Instalación automática con countdown (60 segundos)
- Interfaz moderna con PyQt6
- Instalación silenciosa en background

## Arquitectura

```
┌─────────────────────────────────────────────────────────────┐
│                     main.py (Bootstrap)                      │
│  - Verifica updates al iniciar (async, no bloquea UI)       │
│  - Crea diálogo si hay actualización disponible              │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│              AutoUpdater (auto_update.py)                    │
│  - Verifica versión en carpeta de red                       │
│  - Compara versiones (semántica)                            │
│  - Autentica acceso a red (credenciales)                    │
│  - Copia instalador a temp                                   │
│  - Crea scripts batch/PowerShell para instalar               │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│            UpdateDialog (update_dialog.py)                   │
│  - UI moderna con countdown (60s)                           │
│  - Muestra versiones actual vs nueva                         │
│  - Notas de la versión                                       │
│  - Actualización obligatoria (no se puede cerrar)           │
└─────────────────────────────────────────────────────────────┘
```

## Flujo de Actualización

```
1. INICIO
   ├─> main.py carga
   └─> QTimer ejecuta check_updates_async() (no bloquea)

2. VERIFICACIÓN
   ├─> AutoUpdater.check_for_updates()
   ├─> Autentica carpeta de red (si tiene credenciales)
   ├─> Lee update_info.json
   ├─> Compara versión actual vs disponible
   └─> Retorna: (has_update, new_version, installer_path)

3. SI HAY ACTUALIZACIÓN
   ├─> Muestra UpdateDialog
   ├─> Countdown 60 segundos
   └─> Usuario acepta O timeout

4. INSTALACIÓN
   ├─> Copia instalador a carpeta temporal
   ├─> Crea batch script para instalar después de cerrar
   ├─> Crea PowerShell script para lanzar batch independiente
   ├─> Ejecuta PowerShell launcher
   └─> Cierra aplicación (sys.exit(0))

5. POST-CIERRE
   ├─> Batch espera 2 segundos
   ├─> Ejecuta instalador (silencioso o con UI)
   ├─> Limpia archivos temporales
   └─> Se auto-elimina
```

## Archivos del Sistema

### 1. app/services/auto_update.py (334 líneas)

```python
"""
Sistema de Auto-Actualización
Verifica y descarga actualizaciones desde una carpeta compartida de red
"""

class AutoUpdater:
    """Maneja la verificación y actualización automática del programa"""
    
    def __init__(self, network_path: str, current_version: str, 
                 network_user: str = None, network_password: str = None):
        """
        Args:
            network_path: Ruta UNC (ej: \\\\SERVER\\Updates\\InputScan)
            current_version: Versión actual (ej: "1.5.0")
            network_user: Usuario para autenticación (opcional)
            network_password: Contraseña (opcional)
        """
        self.network_path = Path(network_path)
        self.current_version = current_version.strip()
        self.update_info_file = "update_info.json"
        self.installer_pattern = "Input_Scan_Setup_v*.exe"
        self.network_user = network_user
        self.network_password = network_password
    
    def check_for_updates(self) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Verifica si hay actualización disponible
        
        Returns:
            (has_update, new_version, installer_path)
        """
        # 1. Autenticar carpeta de red
        # 2. Leer update_info.json
        # 3. Comparar versiones
        # 4. Verificar que instalador existe
        # 5. Retornar resultados
    
    def install_update(self, installer_path: str, silent: bool = False) -> bool:
        """
        Instala la actualización
        
        Proceso:
        1. Copia instalador a TEMP
        2. Crea batch script para instalar después de cerrar
        3. Crea PowerShell launcher
        4. Ejecuta launcher (independiente del proceso actual)
        5. Retorna True (app debe cerrarse)
        """
```

**Características clave:**
- Autenticación de red con `net use`
- Comparación de versiones semánticas
- Manejo de errores robusto
- Logging detallado
- Instalación independiente del proceso principal

### 2. app/ui/update_dialog.py (421 líneas)

```python
"""
Diálogo de Actualización - Diseño moderno
"""

class UpdateDialog(QDialog):
    """Ventana de actualización con countdown"""
    
    update_accepted = pyqtSignal()
    update_rejected = pyqtSignal()
    
    def __init__(self, current_version: str, new_version: str, 
                 release_notes: str = "", parent=None):
        """
        Args:
            current_version: Versión actual (ej: "1.4.8")
            new_version: Nueva versión (ej: "1.5.0")
            release_notes: Notas de la versión (texto)
            parent: Ventana padre
        """
        self.auto_update_seconds = 60  # Countdown
        self.timer = QTimer()
        
    def start_countdown(self):
        """Inicia countdown de 60 segundos"""
        self.timer.start(1000)  # Cada segundo
        
    def closeEvent(self, event):
        """Prevenir cierre - actualización obligatoria"""
        event.ignore()  # No permitir cerrar
```

**Características clave:**
- Diseño moderno (tema oscuro)
- Countdown de 60 segundos
- Actualización obligatoria (no se puede cerrar)
- Muestra versión actual vs nueva
- Notas de la versión
- Botón prominente "Actualizar ahora"

### 3. app/main.py (integración)

```python
def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    
    # Verificar actualizaciones DESPUÉS de mostrar ventana
    if settings.AUTO_CHECK_UPDATES:
        QTimer.singleShot(1000, check_updates_async)  # 1 segundo después
    
    sys.exit(app.exec())

def check_updates_async():
    """Verifica en background sin bloquear UI"""
    updater = AutoUpdater(
        settings.UPDATE_NETWORK_PATH,
        settings.APP_VERSION,
        settings.UPDATE_NETWORK_USER,
        settings.UPDATE_NETWORK_PASSWORD
    )
    
    has_update, new_version, installer_path = updater.check_for_updates()
    
    if has_update:
        dialog = UpdateDialog(settings.APP_VERSION, new_version)
        
        dialog.update_accepted.connect(lambda: [
            updater.install_update(installer_path, silent=True),
            sys.exit(0)
        ])
        
        dialog.exec()  # Bloquea hasta que usuario acepta o timeout
```

### 4. installers/update_info.json

```json
{
    "version": "1.5.0",
    "installer": "Input_Scan_Setup_v1.5.0.exe",
    "release_notes": "Notas de la versión",
    "release_date": "2025-10-21 16:17:27",
    "minimum_version": "1.0.0"
}
```

**Estructura:**
- `version`: Versión disponible (semántica)
- `installer`: Nombre exacto del archivo .exe
- `release_notes`: Texto descriptivo
- `release_date`: Timestamp
- `minimum_version`: Versión mínima para actualizar

### 5. Configuración (.env)

```ini
# Auto-actualización
AUTO_CHECK_UPDATES=True
AUTO_INSTALL_UPDATES=True
SILENT_UPDATE_INSTALL=True
UPDATE_NETWORK_PATH=\\\\192.168.1.230\\develop\\INSTALADORES_INPUT
UPDATE_NETWORK_USER=imdinput
UPDATE_NETWORK_PASSWORD=InputUser3030..
APP_VERSION=1.5.0
```

## Instalación en Nuevo Proyecto

### Paso 1: Copiar Archivos

```
mi_proyecto/
├── app/
│   ├── services/
│   │   └── auto_update.py          # COPIAR
│   ├── ui/
│   │   └── update_dialog.py        # COPIAR
│   └── main.py                      # MODIFICAR
├── installers/
│   └── update_info.json            # CREAR
├── version.txt                      # CREAR
└── .env                            # AGREGAR VARIABLES
```

### Paso 2: Configurar .env

```ini
# Auto-actualización
AUTO_CHECK_UPDATES=True
AUTO_INSTALL_UPDATES=True
SILENT_UPDATE_INSTALL=True
UPDATE_NETWORK_PATH=\\\\TU_SERVIDOR\\Updates\\MiApp
UPDATE_NETWORK_USER=usuario
UPDATE_NETWORK_PASSWORD=password
APP_VERSION=1.0.0
```

### Paso 3: Leer versión en config.py

```python
# config.py
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent

# Leer versión desde archivo
VERSION_FILE = ROOT_DIR / "version.txt"
if VERSION_FILE.exists():
    APP_VERSION = VERSION_FILE.read_text(encoding='utf-8').strip()
else:
    APP_VERSION = "1.0.0"

# Auto-actualización
AUTO_CHECK_UPDATES = os.getenv("AUTO_CHECK_UPDATES", "True") == "True"
AUTO_INSTALL_UPDATES = os.getenv("AUTO_INSTALL_UPDATES", "True") == "True"
SILENT_UPDATE_INSTALL = os.getenv("SILENT_UPDATE_INSTALL", "True") == "True"
UPDATE_NETWORK_PATH = os.getenv("UPDATE_NETWORK_PATH", "")
UPDATE_NETWORK_USER = os.getenv("UPDATE_NETWORK_USER", "")
UPDATE_NETWORK_PASSWORD = os.getenv("UPDATE_NETWORK_PASSWORD", "")
```

### Paso 4: Integrar en main.py

```python
# main.py
from PyQt6.QtCore import QTimer

def main():
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    
    # Verificar updates 1 segundo después (no bloquea inicio)
    if settings.AUTO_CHECK_UPDATES:
        QTimer.singleShot(1000, check_updates_async)
    
    sys.exit(app.exec())

def check_updates_async():
    """Verifica actualizaciones en background"""
    from .services.auto_update import AutoUpdater
    from .ui.update_dialog import UpdateDialog
    
    updater = AutoUpdater(
        settings.UPDATE_NETWORK_PATH,
        settings.APP_VERSION,
        settings.UPDATE_NETWORK_USER,
        settings.UPDATE_NETWORK_PASSWORD
    )
    
    has_update, new_version, installer_path = updater.check_for_updates()
    
    if has_update and installer_path:
        # Mostrar diálogo
        dialog = UpdateDialog(
            current_version=settings.APP_VERSION,
            new_version=new_version,
            release_notes="Actualización disponible",
            parent=None
        )
        
        # Conectar señal
        def on_accept():
            if updater.install_update(installer_path, silent=settings.SILENT_UPDATE_INSTALL):
                sys.exit(0)
        
        dialog.update_accepted.connect(on_accept)
        dialog.exec()
```

### Paso 5: Preparar Carpeta de Red

```
\\\\SERVIDOR\\Updates\\MiApp\\
├── update_info.json              # Información de actualización
└── MiApp_Setup_v1.0.1.exe       # Instalador (nombre debe coincidir con JSON)
```

**update_info.json:**
```json
{
    "version": "1.0.1",
    "installer": "MiApp_Setup_v1.0.1.exe",
    "release_notes": "• Nueva funcionalidad X\\n• Corrección de bug Y\\n• Mejora de rendimiento",
    "release_date": "2025-10-21 10:00:00",
    "minimum_version": "1.0.0"
}
```

### Paso 6: Compilar con PyInstaller

```powershell
# Incluir version.txt en el spec file
a = Analysis(
    ['run.py'],
    datas=[
        ('version.txt', '.'),  # Incluir versión
        ('app', 'app'),
        # ... otros archivos
    ],
    # ...
)
```

## Proceso de Despliegue

### Cuando tienes nueva versión:

```powershell
# 1. Actualizar version.txt
echo "1.0.1" > version.txt

# 2. Compilar con PyInstaller
pyinstaller mi_app.spec --clean --noconfirm

# 3. Crear instalador con NSIS/Inno Setup
# El instalador debe llamarse: MiApp_Setup_v1.0.1.exe

# 4. Copiar a carpeta de red
copy dist\\MiApp_Setup_v1.0.1.exe \\\\SERVIDOR\\Updates\\MiApp\\

# 5. Actualizar update_info.json en carpeta de red
# {
#   "version": "1.0.1",
#   "installer": "MiApp_Setup_v1.0.1.exe",
#   ...
# }
```

### Los clientes se actualizarán:

1. Al abrir la app, verificará automáticamente
2. Si hay nueva versión, mostrará diálogo
3. Countdown de 60 segundos
4. Se cierra y ejecuta instalador
5. Instalador actualiza la app
6. Usuario abre la nueva versión

## Ventajas del Sistema

1. **No bloquea inicio**: Verificación asíncrona
2. **Actualización obligatoria**: Countdown + no se puede cerrar
3. **Credenciales integradas**: Autentica automáticamente
4. **Instalación independiente**: Scripts batch/PowerShell
5. **Silenciosa**: Sin interacción del usuario
6. **Robusta**: Manejo completo de errores
7. **Moderna**: UI atractiva con PyQt6
8. **Semántica**: Comparación correcta de versiones
9. **Red corporativa**: Funciona en LAN/VPN
10. **Logging completo**: Trazabilidad total

## Debugging

### Logs importantes:

```python
# Al inicio
"🔍 Verificando actualizaciones en background..."
"Autenticando acceso a: \\\\SERVER\\share"
"✅ Autenticación de red exitosa"

# Durante verificación
"Versión actual: 1.0.0, Versión disponible: 1.0.1"
"📦 Nueva versión disponible: 1.0.1"

# Durante instalación
"📄 Iniciando instalación de actualización..."
"Copiando instalador a: C:\\Users\\...\\Temp\\..."
"✅ Instalador copiado. Tamaño: 56.66 MB"
"📝 Creando script de actualización..."
"✅ Script batch creado."
"Creando PowerShell launcher..."
"✅ PowerShell lanzado con PID: 12345"
"🎬 Instalación programada. Cerrando el programa actual..."
```

### Problemas comunes:

**Error: "Carpeta no accesible"**
- Verificar que UPDATE_NETWORK_PATH sea correcto
- Verificar credenciales (USER/PASSWORD)
- Verificar conectividad de red

**Error: "Instalador no encontrado"**
- Verificar que installer name en JSON coincida con archivo
- Verificar que archivo existe en carpeta de red

**Actualización no se ejecuta**
- Verificar que version.txt se incluye en compilación
- Verificar permisos de escritura en carpeta TEMP
- Verificar que PowerShell no está bloqueado por políticas

## Personalización

### Cambiar tiempo de countdown:

```python
# update_dialog.py
def __init__(self, ...):
    self.auto_update_seconds = 30  # Cambiar de 60 a 30
```

### Hacer actualización opcional:

```python
# update_dialog.py
def closeEvent(self, event):
    event.accept()  # Permitir cerrar (en vez de ignore())

# main.py
dialog.update_rejected.connect(lambda: logging.info("Usuario canceló"))
```

### Cambiar estilos del diálogo:

```python
# update_dialog.py
def load_custom_styles(self):
    self.setStyleSheet("""
        /* Personalizar colores, fuentes, etc. */
    """)
```

## Conclusión

Este sistema de auto-actualización es:
- **Completo**: Maneja todo el ciclo
- **Robusto**: Manejo de errores
- **Moderno**: UI atractiva
- **Configurable**: Variables de entorno
- **Portable**: Fácil de integrar en otros proyectos

Solo necesitas:
1. Copiar 2 archivos (auto_update.py, update_dialog.py)
2. Agregar 6 variables al .env
3. Integrar 20 líneas en main.py
4. Crear update_info.json en carpeta de red
5. ¡Listo! Tu app se actualiza sola

---

**Versión del documento:** 1.0  
**Fecha:** 2025-10-21  
**Autor:** Sistema extraído de INPUT SCAN v1.5.0
