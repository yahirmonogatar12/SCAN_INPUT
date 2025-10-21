# Sistema de Escaneo de Producción IMD - INPUT SCAN# Sistema de Escaneo de Producción IMD - INPUT SCAN



**Versión:** 1.5.0  **Versión:** 1.5.0  

**Estado:** Producción  **Estado:** Producción  

**Última actualización:** 2025-10-21**Última actualización:** 2025-10-21



## Descripción## Descripción



Sistema de escritorio desarrollado en Python 3.11 con PyQt6 para capturar y gestionar escaneos de producción en tiempo real. Arquitectura **Dual Database** (SQLite + MySQL) con sincronización automática, métricas en tiempo real y actualización automática de planes de producción.Sistema de escritorio desarrollado en Python 3.11 con PyQt6 para capturar y gestionar escaneos de producción en tiempo real. Arquitectura **Dual Database** (SQLite + MySQL) con sincronización automática, métricas en tiempo real y actualización automática de planes de producción.



## Características Principales## Características Principales



### Arquitectura Dual Database### Arquitectura Dual Database

- **SQLite local** (< 1ms): Respuesta instantánea en la UI- **SQLite local** (< 1ms): Respuesta instantánea en la UI

- **MySQL remoto**: Persistencia centralizada y sincronización- **MySQL remoto**: Persistencia centralizada y sincronización

- Sync worker automático cada 3 segundos- Sync worker automático cada 3 segundos

- Cola offline inteligente para trabajar sin conexión- Cola offline inteligente para trabajar sin conexión



### Gestión de Producción### Gestión de Producción

- Escaneo de códigos QR/barras en tiempo real- Escaneo de códigos QR/barras en tiempo real

- Detección automática de cambios en plan de producción (cada 15s)- Detección automática de cambios en plan de producción (cada 15s)

- Actualización automática de UI sin reiniciar (máximo 20s)- Actualización automática de UI sin reiniciar (máximo 20s)

- Métricas en tiempo real: Producción, UPH, UPPH, Eficiencia- Métricas en tiempo real: Producción, UPH, UPPH, Eficiencia

- Indicador visual de fecha del plan- Indicador visual de fecha del plan

- Detección automática de medianoche y recarga de plan- Detección automática de medianoche y recarga de plan



### Seguridad### Seguridad

- Login con contraseñas hasheadas (bcrypt + PBKDF2-HMAC-SHA256)- Login con contraseñas hasheadas (bcrypt + PBKDF2-HMAC-SHA256)

- Configuración encriptada con Fernet- Configuración encriptada con Fernet

- Ejecución con privilegios de administrador- Ejecución con privilegios de administrador



### Interfaz de Usuario### Interfaz de Usuario

- Dashboard con métricas en tiempo real- Dashboard con métricas en tiempo real

- Ventana UPH sincronizada automáticamente- Ventana UPH sincronizada automáticamente

- Animaciones visuales para cambios importantes- Animaciones visuales para cambios importantes

- Sonidos de confirmación (CHECK.wav, ERROR.wav)- Sonidos de confirmación (CHECK.wav, ERROR.wav)

- Tema oscuro estilo BOM- Tema oscuro estilo BOM



## Estructura del Proyecto2) Copiar `.env.example` a `.env` y ajustar rutas si deseas:



```---   - cp .env.example .env

VISUAL CODEX/

├── app/                          # Código fuente principal3) Ejecutar la app:

│   ├── main.py                  # Entry point

│   ├── config.py                # Configuración## 📦 Estructura del Proyecto   - python run.py

│   ├── secure_config.py         # Configuración encriptada

│   ├── logging_config.py        # Sistema de logging

│   ├── db/                      # Capa de base de datos

│   │   ├── sqlite_db.py         # SQLite local (cache)```Usuarios iniciales

│   │   └── mysql_db.py          # MySQL remoto (persistencia)

│   ├── models/                  # Entidades del dominio## 📁 Estructura del Proyecto

│   │   └── entities.py

│   ├── services/                # Lógica de negocio```

│   │   ├── dual_db.py          # Sistema dual con sync workerVISUAL CODEX/

│   │   ├── scans.py            # Gestión de escaneos├── app/                          # Código fuente principal

│   │   ├── counters.py         # Contadores de producción│   ├── main.py                  # Entry point

│   │   ├── metrics_cache.py    # Cache de métricas│   ├── config.py                # Configuración

│   │   └── auto_update.py      # Auto-actualización│   ├── secure_config.py         # Configuración encriptada

│   └── ui/                      # Interfaz PyQt6│   ├── logging_config.py        # Sistema de logging

│       ├── login.py             # Pantalla de login│   ├── db/                      # Capa de base de datos

│       ├── main_window.py       # Ventana principal│   │   ├── sqlite_db.py         # SQLite local (cache)

│       ├── metrics_widget.py    # Ventana de UPH│   │   └── mysql_db.py          # MySQL remoto (persistencia)

│       └── style.py             # Estilos│   ├── models/                  # Entidades del dominio

├── migrations/                   # Scripts SQL│   │   └── entities.py

│   ├── sqlite.sql│   ├── services/                # Lógica de negocio

│   ├── mysql.sql│   │   ├── dual_db.py          # ⭐ Sistema dual con sync worker

│   └── mysql_indexes_optimization.sql│   │   ├── scans.py            # Gestión de escaneos

├── data/                        # Datos locales (gitignored)│   │   ├── counters.py         # Contadores de producción

├── logs/                        # Logs de aplicación (gitignored)│   │   ├── metrics_cache.py    # Cache de métricas

├── installers/                  # Instaladores generados│   │   └── auto_update.py      # Auto-actualización

├── run.py                       # Punto de entrada│   └── ui/                      # Interfaz PyQt6

├── Input_Scan_ONEDIR.spec      # Configuración PyInstaller│       ├── login.py             # Pantalla de login

├── build_all.ps1               # Script de compilación│       ├── main_window.py       # Ventana principal

├── requirements.txt            # Dependencias Python│       ├── metrics_widget.py    # Ventana de UPH

└── version.txt                 # Versión actual│       └── style.py             # Estilos

```├── migrations/                   # Scripts SQL

│   ├── sqlite.sql

## Requisitos│   ├── mysql.sql

│   └── mysql_indexes_optimization.sql

- **Python:** 3.11+├── data/                        # Datos locales (gitignored)

- **Dependencias:**├── logs/                        # Logs de aplicación (gitignored)

  - PyQt6 >= 6.4.0├── installers/                  # Instaladores generados

  - python-dotenv >= 1.0.0├── run.py                       # Punto de entrada

  - mysql-connector-python >= 8.0.33├── Input_Scan_ONEDIR.spec      # Configuración PyInstaller

  - cryptography >= 41.0.0├── build_all.ps1               # Script de compilación

  - pyinstaller >= 5.13.0 (para compilar)├── requirements.txt            # Dependencias Python

└── version.txt                 # Versión actual

## Inicio Rápido```



### 1. Clonar repositorio## �️ Requisitos

```powershell

git clone https://github.com/yahirmonogatar12/SCAN_INPUT.git- **Python:** 3.11+

cd SCAN_INPUT- **Dependencias:**

```  - PyQt6 >= 6.4.0

  - python-dotenv >= 1.0.0

### 2. Instalar dependencias  - mysql-connector-python >= 8.0.33

```powershell  - cryptography >= 41.0.0

pip install -r requirements.txt  - pyinstaller >= 5.13.0 (para compilar)

```

## ⚡ Inicio Rápido

### 3. Configurar .env

```powershell### 1. Clonar repositorio

cp .env.example .env```powershell

# Editar .env con tus credenciales MySQLgit clone https://github.com/yahirmonogatar12/SCAN_INPUT.git

```cd SCAN_INPUT

```

### 4. Ejecutar aplicación

```powershell### 2. Instalar dependencias

python run.py```powershell

```pip install -r requirements.txt

```

**Usuario por defecto:** `admin / admin123`

### 3. Configurar .env

## Compilación```powershell

cp .env.example .env

### Generar ejecutable con PyInstaller:# Editar .env con tus credenciales MySQL

```powershell```

.\build_all.ps1

```### 4. Ejecutar aplicación

```powershell

El ejecutable se generará en `dist/Input_Scan_Main/`python run.py

```

## Changelog v1.5.0

**Usuario por defecto:** `admin / admin123`

### Nuevo

- **Auto-actualización de plan**: Detecta cambios en MySQL y actualiza UI automáticamente (máximo 20s)## 🔨 Compilación

- **Indicador visual de fecha**: Muestra prominentemente la fecha del plan actual

- **Detección de medianoche**: Recarga automática del plan al cambiar de día### Generar ejecutable con PyInstaller:

- **Sincronización UPH**: Ventana de métricas se actualiza automáticamente```powershell

- **Timer optimizado**: Reducido a 5s para respuesta más rápida.\build_all.ps1

```

### Correcciones

- Buffer de protección ahora solo modifica remote_pc (no inflaba local_pc)El ejecutable se generará en `dist/Input_Scan_Main/`

- Propagación correcta de excepciones MySQL

- Corrección de timing en detección de cambios de plan## Changelog v1.5.0

- Fix para conexiones SQLite cerradas

### ✨ Nuevo

### Mejoras- **Soporte para múltiples líneas de producción**: Ahora puedes escanear y gestionar múltiples líneas simultáneamente.

- Hash-based change detection para optimizar detección de cambios- **Mejoras en la UI**: Nuevos gráficos y visualizaciones para métricas de producción.

- Mejor manejo de conexiones SQLite cerradas- **Optimización de rendimiento**: Reducción del uso de memoria y mejora en la velocidad de escaneo.

- Animaciones visuales para cambios importantes- **Sincronización UPH**: Ventana de métricas se actualiza automáticamente

- Mensajes de log más descriptivos

### 🐛 Correcciones

## Notas de Desarrollo- Buffer de protección ahora solo modifica remote_pc (no inflaba local_pc)

- Propagación correcta de excepciones MySQL

### Sistema Dual Database- Corrección de timing en detección de cambios de plan

El sistema usa una arquitectura dual optimizada:- Timer de UI reducido a 5s para respuesta más rápida

1. **SQLite**: Cache local de alta velocidad (< 1ms)

2. **MySQL**: Persistencia centralizada### 🔧 Mejoras

3. **Sync Worker**: Sincroniza cada 3 segundos en background- Hash-based change detection para optimizar detección de cambios

4. **Plan Sync**: Descarga plan de MySQL cada 15 segundos- Mejor manejo de conexiones SQLite cerradas

5. **UI Update**: Verifica cambios cada 5 segundos- Animaciones visuales para cambios importantes



### Flujo de Escaneo## 📝 Notas de Desarrollo

```

Escaneo → SQLite local (instantáneo) → Cola de sync → MySQL (background)### Sistema Dual Database

```El sistema usa una arquitectura dual optimizada:

1. **SQLite**: Cache local de alta velocidad (< 1ms)

### Detección de Cambios2. **MySQL**: Persistencia centralizada

```3. **Sync Worker**: Sincroniza cada 3 segundos en background

MySQL cambio → Sync detecta (15s) → UI verifica flag (5s) → Reload automático4. **Plan Sync**: Descarga plan de MySQL cada 15 segundos

```5. **UI Update**: Verifica cambios cada 5 segundos



## Contribuciones### Flujo de Escaneo

```

Las contribuciones son bienvenidas. Por favor:Escaneo → SQLite local (instantáneo) → Cola de sync → MySQL (background)

1. Fork el proyecto```

2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)

3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)### Detección de Cambios

4. Push a la rama (`git push origin feature/AmazingFeature`)```

5. Abre un Pull RequestMySQL cambio → Sync detecta (15s) → UI verifica flag (5s) → Reload automático

```

## Licencia

## 🤝 Contribuciones

Ver archivo `LICENSE.txt`

Las contribuciones son bienvenidas. Por favor:

## Autor1. Fork el proyecto

2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)

**Yahir Monogatar**  3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)

GitHub: [@yahirmonogatar12](https://github.com/yahirmonogatar12)4. Push a la rama (`git push origin feature/AmazingFeature`)

5. Abre un Pull Request

---

## 📄 Licencia

**Si este proyecto te ayuda, considera darle una estrella en GitHub!**

Ver archivo `LICENSE.txt`

## 👤 Autor

**Yahir Monogatar**  
GitHub: [@yahirmonogatar12](https://github.com/yahirmonogatar12)

---

**⭐ Si este proyecto te ayuda, considera darle una estrella en GitHub!**

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
