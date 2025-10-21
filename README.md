# 🚀 SISTEMA DE SCANEO IMD - INPUT SCANSistema de Escaneo SMD (SQLite primero, listo para MySQL)



**Versión:** 1.3.4+ Optimizado  Descripción

**Estado:** ✅ Producción  - App de escritorio en Python 3.11 con PyQt6 para capturar escaneos de producción, almacenar en SQLite y mostrar últimos escaneos y totales diarios por línea/modelo. Arquitectura preparada para migrar a MySQL sin cambiar la UI/servicios.

**Última actualización:** 2025-10-19

Características clave

---- Backend: SQLite (archivo local), con capa DAO para migración futura a MySQL.

- Login con contraseñas hasheadas (bcrypt si disponible; fallback PBKDF2-HMAC-SHA256).

## 📖 Documentación- Parsing de cadenas escaneadas con validación regex.

- Tablas: input_main, produccion_main_input.

📚 **[Ver Documentación Completa](DOCUMENTACION_COMPLETA.md)** - Toda la información en un solo archivo- Consulta externa: tabla 'raw' (modelos - solo lectura).

- Caché local: queue_scans (en memoria/archivo JSON).

---- UI PyQt6: Login, MainWindow (entrada de escaneo, tabla de últimos, totales del día), Catálogo de Modelos (CRUD, rol admin).

- Logging con rotación en `logs/app.log`.

## ⚡ Inicio Rápido- Zona horaria: America/Monterrey.

- Config por `.env`.

### Ejecutar aplicación:

```powershellRequisitos

python run.py- Python 3.11

```- PyQt6 instalado en el entorno donde se ejecutará la app.

- No se requiere MySQL para esta versión. La migración se facilita vía la capa DB.

### Compilar proyecto:

```powershellInstalación rápida

.\build_all.ps11) Crear entorno (opcional) e instalar PyQt6 (si no lo tienes):

```   - pip install PyQt6

2) Copiar `.env.example` a `.env` y ajustar rutas si deseas:

---   - cp .env.example .env

3) Ejecutar la app:

## 📦 Estructura del Proyecto   - python run.py



```Usuarios iniciales

VISUAL CODEX/- Se crea automáticamente el usuario admin `admin / admin123` en la primera ejecución (puedes cambiarlo después).

├── app/                    # Código fuente principal

│   ├── main.py            # Entry pointEstructura

│   ├── secure_config.py   # ✨ Configuración segura- app/

│   ├── db/                # Capa de base de datos  - main.py (entrada de la app)

│   ├── models/            # Entidades  - config.py (carga de configuración y .env)

│   ├── services/          # Lógica de negocio  - logging_config.py (logging con rotación)

│   │   ├── dual_db.py            # Sistema dual SQLite + MySQL  - models/

│   │   └── db_optimizations.py  # ✨ Utilidades de optimización  - db/ (SQLiteDB y DAO)

│   └── ui/                # Interfaz PyQt6  - services/ (parser, auth, scans, summary)

├── data/                  # Datos y configuración  - ui/ (PyQt6: login, main window, catálogo)

├── migrations/            # Scripts SQL- migrations/

├── installers/            # Instaladores generados  - sqlite.sql (DDL SQLite)

├── run.py                 # ✨ Entry point optimizado  - mysql.sql (DDL de referencia para futura migración)

├── build_all.ps1          # Script de compilación- tests/

└── DOCUMENTACION_COMPLETA.md  # 📚 Documentación completa

Migración futura a MySQL

✨ = Archivos optimizados/nuevos- La capa `app/db/dao.py` define una interfaz mínima que la UI y servicios consumen. Implementa `SQLiteDB`. Para migrar:

```  1) Implementar `MySQLDB` en `app/db/mysql_db.py` usando `PyMySQL` o `mysqlclient`.

  2) Respetar los mismos métodos públicos que `SQLiteDB`.

---  3) Cambiar `get_db()` en `app/db/__init__.py` para devolver `MySQLDB` cuando `DB_ENGINE=mysql` en `.env`.

  4) Ejecutar `migrations/mysql.sql` en tu servidor MySQL.

## 🚀 Características

Pruebas

- ✅ Sistema dual SQLite (local) + MySQL (sincronización)- Parser y flujo de inserción básica con `unittest`:

- ✅ Interfaz PyQt6 moderna  - python -m unittest

- ✅ Login con autenticación segura

- ✅ Escaneo de códigos QR y BarcodeNotas

- ✅ Dashboard con métricas en tiempo real- Esta versión no realiza replicación ni cola hacia MySQL; `queue_scans` queda lista para utilizarse cuando se habilite MySQL.

- ✅ Sistema de actualización automática- Evita subir `.env` con credenciales.

- ✅ Optimizado para PCs lentas

- ✅ Pool de conexiones MySQL
- ✅ Caché inteligente con TTL
- ✅ Reintentos automáticos con backoff

---

## 📋 Requisitos

- Python 3.11+
- PyQt6
- MySQL Connector
- Ver `requirements.txt` para lista completa

---

## 🔧 Configuración

Editar archivo `.env` en la raíz del proyecto:

```env
# Base de datos
DB_ENGINE=dual
MYSQL_HOST=tu_host
MYSQL_PORT=3306
MYSQL_DB=tu_base_de_datos
MYSQL_USER=tu_usuario
MYSQL_PASSWORD=tu_password

# Aplicación
APP_MODE=ASSY
DEFAULT_LINE=M1
NUM_PERSONAS_LINEA=7
```

---

## 📚 Documentación Detallada

Para información completa sobre:
- Compilación e instalación
- Optimizaciones aplicadas
- Arquitectura del sistema
- Troubleshooting
- Y más...

👉 **[Consulta DOCUMENTACION_COMPLETA.md](DOCUMENTACION_COMPLETA.md)**

---

## 🆘 Soporte

Para problemas o preguntas, consulta la documentación completa o revisa los logs en:
- `%APPDATA%\IMD_Scanner\logs\`

---

**Desarrollado para IMD - Sistema de Control de Producción**
