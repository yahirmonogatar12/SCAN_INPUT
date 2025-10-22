# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['run.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('app', 'app'),
        ('control_bom.css', '.'),
        ('logoLogIn.png', '.'),
        ('logoLogIn.ico', '.'),
        ('CHECK.wav', '.'),
        ('ERROR.wav', '.'),
        ('.env.example', '.'),  # ✅ Plantilla de configuración (renombrada a .env en instalación)
        ('version.txt', '.'),  # Archivo de versión para auto-actualización
        ('migrations', 'migrations'),  # ✅ Scripts SQL de migración
        ('README.md', '.'),  # ✅ Documentación
        ('LICENSE.txt', '.'),  # ✅ Licencia
    ],
    # ✅ NOTA: tzdata se incluye automáticamente con Python 3.11+
    # Si hay errores de zona horaria, descomentar la siguiente línea:
    # ('C:\\Users\\yahir\\AppData\\Local\\Programs\\Python\\Python311\\Lib\\site-packages\\tzdata', 'tzdata'),
    hiddenimports=[
        'pkgutil',
        'importlib',
        'importlib.util',
        'importlib.metadata',
        'mysql.connector',
        'mysql.connector.pooling',
        'mysql.connector.cursor',
        'mysql.connector.errors',
        'PyQt6',
        'PyQt6.QtCore',
        'PyQt6.QtGui',
        'PyQt6.QtWidgets',
        'PyQt6.sip',
        'PyQt6.QtMultimedia',  # ✅ Para sonidos CHECK.wav y ERROR.wav
        'dotenv',
        'zoneinfo',
        'zoneinfo._tzpath',
        'tzdata',  # ✅ Base de datos de zonas horarias (backport para Python < 3.9)
        'sqlite3',
        'logging',
        'logging.handlers',
        'threading',
        'queue',
        'collections',
        'collections.abc',
        'datetime',
        'pathlib',
        'json',
        'hashlib',
        'secrets',
        'base64',
        'cryptography',  # ✅ Para secure_config.py
        'cryptography.fernet',
        'urllib',
        'urllib.request',
        'urllib.error',
        'ssl',
        'certifi',  # ✅ Para verificación SSL en auto-update
        'requests',  # ✅ Si usas requests en lugar de urllib
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['rthooks/pyi_rth_qt6_path.py'],  # ✅ Hook para PyQt6 paths
    excludes=[
        'tkinter',
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'IPython',
        'jupyter',
        'notebook',
        'PIL',
        'wx',
        'pydoc',
        'doctest',
        'unittest',
        'test',
        'tests',
        '_pytest',
    ],  # ✅ Excluir módulos innecesarios para reducir tamaño
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Input_Scan_Main',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    uac_admin=True,  # ✅ SIEMPRE EJECUTAR COMO ADMINISTRADOR
    uac_uiaccess=False,
    codesign_identity=None,
    entitlements_file=None,
    icon='logoLogIn.ico',
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Input_Scan_Main',
)

# Copiar archivos adicionales a la raíz del ejecutable para facilitar la lectura
import shutil
from pathlib import Path

try:
    dist_root = Path('dist/Input_Scan_Main')
    if dist_root.exists():
        # Copiar version.txt
        if Path('version.txt').exists():
            shutil.copy2('version.txt', dist_root / 'version.txt')
            print(f"✅ version.txt copiado a {dist_root}")
        
        # Crear directorio data/ si no existe
        data_dir = dist_root / 'data'
        if not data_dir.exists():
            data_dir.mkdir(parents=True)
            print(f"✅ Directorio data/ creado en {dist_root}")
        
        # Verificar que archivos críticos estén presentes
        critical_files = ['CHECK.wav', 'ERROR.wav', 'logoLogIn.png', 'logoLogIn.ico', 'README.md', 'LICENSE.txt']
        for file in critical_files:
            if not (dist_root / file).exists():
                print(f"⚠️ ADVERTENCIA: {file} no encontrado en {dist_root}")
    else:
        print(f"⚠️ ADVERTENCIA: Directorio {dist_root} no existe")
except Exception as e:
    print(f"❌ Error copiando archivos adicionales: {e}")
