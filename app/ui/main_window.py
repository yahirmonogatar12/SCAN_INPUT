from PyQt6 import QtWidgets, QtCore, QtGui
from typing import List
import os
import logging
import winsound
from pathlib import Path

from ..services.scans_optimized import process_scan_direct, get_production_counts_cached
from ..services.direct_mysql import get_direct_mysql
from ..services.parser import detect_scan_format, is_complete_qr
from ..services.summary import get_last_scans, get_today_totals
from ..db import get_db
from ..services.local_queue import get_local_queue
from ..config import ROOT_DIR
from .configuracion_dialog import ConfiguracionDialog
from .metrics_widget import MetricsWidget
from ..config import settings

logger = logging.getLogger(__name__)


# ==================== WORKER THREAD PARA CAMBIO DE ESTADO EN BACKGROUND ====================
class EstadoPlanWorker(QtCore.QThread):
    """Worker thread para actualizar SQLite y MySQL en background (NO bloquea UI)"""
    finished = QtCore.pyqtSignal(bool, str)  # (success, message)
    
    def __init__(self, plan_id, nuevo_estado, part_no="", linea=None):
        super().__init__()
        self.plan_id = plan_id
        self.nuevo_estado = nuevo_estado
        self.part_no = part_no
        self.linea = linea
    
    def run(self):
        """Ejecuta actualización de bases de datos en background (SQLite + MySQL)"""
        import time
        try:
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            # SOLO actualiza bases de datos (SQLite + MySQL), NO toca caché/UI
            # El caché ya fue actualizado ANTES de crear este worker
            resultado = dual_db.actualizar_estado_plan_db_only(
                self.plan_id, 
                self.nuevo_estado,
                self.linea
            )
            
            if resultado:
                self.finished.emit(True, f"BD sincronizada: {self.part_no}")
            else:
                self.finished.emit(False, f"Error en BD para {self.part_no}")
        except Exception as e:
            logger.error(f"Error en EstadoPlanWorker (background DB sync): {e}")
            self.finished.emit(False, f"Error BD: {str(e)}")


def _play_success_sound():
    """Reproduce el sonido de éxito (CHECK.wav)."""
    try:
        # Buscar archivo CHECK.wav en el directorio raíz del programa
        sound_path = ROOT_DIR / "CHECK.wav"
        if sound_path.exists():
            # SND_FILENAME = reproduce archivo
            # SND_ASYNC = reproduce de forma asíncrona para que no bloquee
            # SND_NODEFAULT = no reproduce sonido por defecto si falla
            winsound.PlaySound(str(sound_path), winsound.SND_FILENAME | winsound.SND_ASYNC | winsound.SND_NODEFAULT)
        else:
            # Fallback: beep corto de éxito
            winsound.Beep(800, 150)
    except Exception as e:
        logger.debug(f"No se pudo reproducir CHECK.wav: {e}")
        try:
            winsound.Beep(800, 150)
        except Exception:
            pass


def _play_error_sound():
    """Reproduce el sonido de error (ERROR.wav)."""
    try:
        # Buscar archivo ERROR.wav en el directorio raíz del programa
        sound_path = ROOT_DIR / "ERROR.wav"
        if sound_path.exists():
            # SND_FILENAME = reproduce archivo
            # SND_ASYNC = reproduce de forma asíncrona para que no bloquee
            # SND_NODEFAULT = no reproduce sonido por defecto si falla
            winsound.PlaySound(str(sound_path), winsound.SND_FILENAME | winsound.SND_ASYNC | winsound.SND_NODEFAULT)
        else:
            # Fallback: usar sonido del sistema
            winsound.PlaySound("SystemHand", winsound.SND_ALIAS | winsound.SND_ASYNC)
    except Exception as e:
        logger.debug(f"No se pudo reproducir ERROR.wav: {e}")
        try:
            winsound.PlaySound("SystemHand", winsound.SND_ALIAS | winsound.SND_ASYNC)
        except Exception:
            try:
                winsound.Beep(600, 200)
                winsound.Beep(500, 200)
                winsound.Beep(400, 400)
                winsound.Beep(350, 400)
            except Exception:
                pass


def normalize_scanner_text(text: str) -> str:
    """
    Normaliza texto del escáner para compatibilidad con diferentes distribuciones de teclado.
    - Convierte a MAYÚSCULAS
    - Mapea caracteres que cambian entre distribución inglés/español
    """
    if not text:
        return text
    
    # Convertir a MAYÚSCULAS primero
    normalized = text.upper()
    
    # Mapeo de caracteres comunes que cambian entre distribución inglés/español
    # NOTA: NO convertir Ñ porque es separador válido en QR formato nuevo
    char_map = {
        # 'Ñ': 'N',  # ❌ DESHABILITADO - ñ es separador en QR nuevo
        'Ç': 'C',  # Ç -> C  
        'Ü': 'U',  # Ü -> U
        'Á': 'A', 'À': 'A', 'Ä': 'A', 'Â': 'A',  # acentos A -> A
        'É': 'E', 'È': 'E', 'Ë': 'E', 'Ê': 'E',  # acentos E -> E
        'Í': 'I', 'Ì': 'I', 'Ï': 'I', 'Î': 'I',  # acentos I -> I
        'Ó': 'O', 'Ò': 'O', 'Ö': 'O', 'Ô': 'O',  # acentos O -> O
        'Ú': 'U', 'Ù': 'U', 'Ü': 'U', 'Û': 'U',  # acentos U -> U
        '¡': '',   # ¡ -> eliminar
        '¿': '',   # ¿ -> eliminar
        '«': '"',  # « -> "
        '»': '"',  # » -> "
        '–': '-',  # guión largo -> guión normal
        '—': '-',  # guión muy largo -> guión normal
        ''': "'",  # comilla curva -> comilla recta
        ''': "'",  # comilla curva -> comilla recta
        '"': '"',  # comilla curva -> comilla recta
        '"': '"',  # comilla curva -> comilla recta
    }
    
    # Aplicar mapeo de caracteres
    for old_char, new_char in char_map.items():
        normalized = normalized.replace(old_char, new_char)
    
    return normalized


class MainWindow(QtWidgets.QMainWindow):
    scan_processed_signal = QtCore.pyqtSignal(str, str, str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.db = get_db()
        self.scan_processed_signal.connect(self._handle_scan_processed)
        self._direct_mysql = None
        self._direct_mysql_listener_registered = False
        try:
            self._direct_mysql = get_direct_mysql()
            self._direct_mysql.register_scan_listener(self._emit_scan_processed)
            self._direct_mysql_listener_registered = True
        except Exception as listener_err:
            logger.warning(f"No se pudo registrar listener de escaneos: {listener_err}")
        self.setWindowTitle("Input Scan - Sistema de Escaneo")
        self.resize(1000, 650)
        
        # Configurar icono de la ventana para ventana y barra de tareas
        icon_path = ROOT_DIR / "logoLogIn.ico"
        if icon_path.exists():
            app_icon = QtGui.QIcon(str(icon_path))
            self.setWindowIcon(app_icon)
            # Asegurar que se vea en la barra de tareas
            QtWidgets.QApplication.instance().setWindowIcon(app_icon)
        
        # Variables para optimización de velocidad
        self._fast_scan_mode = True  # Modo de escaneo rápido
        self._last_ui_update = 0  # Control de actualizaciones UI
        
        # 🚀 CONTADOR EN MEMORIA para evitar accesos a DB después de escaneos
        self._scan_counter = 0  # Contador visual instantáneo (se resetea al actualizar totales)
        
        # 🚀 OPTIMIZACIÓN PARA PCs LENTAS: Detectar modo de rendimiento
        import os
        perf_mode = os.environ.get('APP_PERFORMANCE_MODE', 'NORMAL')
        if perf_mode == 'OPTIMIZED':
            self._ui_update_interval = 1.0  # Actualizar cada 1 segundo en PCs lentas
            logger.info("🚀 Modo OPTIMIZADO activado para PC lenta")
        else:
            self._ui_update_interval = 0.25  # Actualizar UI máximo cada 250ms (feedback más rápido)
        
        # Variables para modo pantalla completa
        self._fullscreen_mode = False
        self._normal_window_state = None
        
        # Variable para rastrear la línea anterior (para validación de cambio)
        self._linea_anterior = None
        # Debounce base para BARCODE (ms); será ajustado dinámicamente según velocidad del escáner
        self._barcode_debounce_ms = 25
        # Métricas dinámicas de velocidad de tecleo (inter-char)
        self._last_char_time = 0.0
        self._interchar_times: List[float] = []  # ms recientes
        # Umbrales heurísticos (pueden exponerse a config si se requiere)
        self._barcode_end_gap_fast = 16   # ms silencio para cerrar scan si es modo escáner
        self._barcode_end_gap_slow = 55   # ms cierre cuando parece tipeo manual
        self._barcode_scanner_threshold_ms = 30  # debajo de esto se considera flujo de escáner
        
        # Sistema de notificación visual para duplicados
        self.duplicate_overlay = None
        self.duplicate_timer = QtCore.QTimer()
        self.duplicate_timer.setSingleShot(True)
        self.duplicate_timer.timeout.connect(self._hide_duplicate_overlay)
        # Timer de parpadeo deshabilitado (se mantiene para posible reuso futuro)
        self.blink_timer = QtCore.QTimer()
        self.blink_state = False
        self.blink_count = 0

        # Overlay de OK (éxito)
        self.ok_overlay = None
        self.ok_timer = QtCore.QTimer()
        self.ok_timer.setSingleShot(True)
        self.ok_timer.timeout.connect(self._hide_ok_overlay)
        
        # Rastreo de fecha para detectar cambio de día (medianoche)
        from datetime import date
        self._current_date = date.today()

        # Central widget (simple, sin sidebar ni barra adicional)
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(8)

        # Encabezado con logo (opcional)
        header = QtWidgets.QHBoxLayout()
        logo_path = ROOT_DIR / "logoLogIn.png"
        if logo_path.exists():
            self.logo_label = QtWidgets.QLabel()
            pix = QtGui.QPixmap(str(logo_path))
            if not pix.isNull():
                self.logo_label.setPixmap(pix.scaledToHeight(28, QtCore.Qt.TransformationMode.SmoothTransformation))
                header.addWidget(self.logo_label)
        header.addStretch(1)
        layout.addLayout(header)

        # Entrada de escaneo grande
        self.scan_input = QtWidgets.QLineEdit()
        self.scan_input.setPlaceholderText("Escanea y presiona ENTER (o escáner con ENTER automático)…")
        fnt = self.scan_input.font()
        fnt.setPointSize(16)
        self.scan_input.setFont(fnt)
        
        # Selector de línea ultra-compacto
        linea_layout = QtWidgets.QHBoxLayout()
        linea_layout.setSpacing(5)
        linea_layout.setContentsMargins(0, 3, 0, 3)
        
        linea_label = QtWidgets.QLabel("Línea:")
        linea_label.setStyleSheet("font-weight: bold; color: #20688C; font-size: 11px;")
        linea_label.setMaximumWidth(35)
        linea_label.setMinimumWidth(35)
        
        self.linea_selector = QtWidgets.QComboBox()
        self.linea_selector.addItems(self._get_line_options())
        
        # 🔧 Configurar línea por defecto con normalización y logging mejorado
        try:
            # Normalizar línea predeterminada (strip + uppercase)
            default_line = getattr(settings, 'DEFAULT_LINE', 'M1').strip().upper()
            logger.info(f"🎯 DEFAULT_LINE desde settings: '{default_line}'")
            
            allowed = self._get_line_options()
            logger.info(f"📋 Líneas disponibles en selector: {allowed}")
            
            # Buscar coincidencia con normalización
            line_found = False
            for i, line in enumerate(allowed):
                if line.strip().upper() == default_line:
                    self.linea_selector.setCurrentIndex(i)
                    logger.info(f"✅ Línea predeterminada establecida: '{line}' (índice {i})")
                    self._linea_anterior = line
                    line_found = True
                    break
            
            if not line_found:
                logger.warning(f"⚠️ Línea '{default_line}' no encontrada. Usando primera opción.")
                default_line = allowed[0] if allowed else "M1"
                self.linea_selector.setCurrentText(default_line)
                self._linea_anterior = default_line
            
            # 🔄 Notificar al cache de métricas sobre la línea inicial
            from app.services.metrics_cache import get_metrics_cache
            metrics_cache = get_metrics_cache()
            if metrics_cache:
                initial_line = self.linea_selector.currentText()
                metrics_cache.set_active_line(initial_line)
                logger.info(f"🎯 Cache de métricas configurado para línea inicial: '{initial_line}'")
                
        except Exception as e:
            logger.error(f"❌ Error configurando línea predeterminada: {e}")
            opts = self._get_line_options()
            default_opt = opts[0] if opts else "M1"
            self.linea_selector.setCurrentText(default_opt)
            self._linea_anterior = default_opt
            
            # 🔄 Notificar al cache incluso en caso de error
            from app.services.metrics_cache import get_metrics_cache
            metrics_cache = get_metrics_cache()
            if metrics_cache:
                metrics_cache.set_active_line(default_opt)
                logger.info(f"🎯 Cache de métricas configurado para línea fallback: '{default_opt}'")
        self.linea_selector.setObjectName("lineaSelector")
        # Hacer el dropdown mucho más pequeño
        self.linea_selector.setMaximumWidth(55)
        self.linea_selector.setMinimumWidth(50)
        self.linea_selector.setMaximumHeight(24)
        # Estilo más compacto
        self.linea_selector.setStyleSheet("""
            QComboBox {
                padding: 2px 4px;
                font-size: 11px;
                font-weight: bold;
            }
            QComboBox::drop-down {
                width: 15px;
            }
        """)
        
        # Indicador de estado del sistema dual
        self.status_dual = QtWidgets.QLabel("Conectado")
        self.status_dual.setStyleSheet("color: #00aa00; font-size: 10px; font-weight: bold;")
        
        # Botón de actualización manual
        self.refresh_button = QtWidgets.QPushButton("Actualizar")
        self.refresh_button.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 6px 12px;
                font-size: 11px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
            QPushButton:pressed {
                background-color: #1c5985;
            }
        """)
        self.refresh_button.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(self._force_refresh)
        
        # Botón para abrir ventana flotante de métricas
        self.float_metrics_button = QtWidgets.QPushButton("UPHS")
        self.float_metrics_button.setStyleSheet("""
            QPushButton {
                background-color: #9B59B6;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 6px 12px;
                font-size: 11px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #8E44AD;
            }
            QPushButton:pressed {
                background-color: #7D3C98;
            }
        """)
        self.float_metrics_button.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.float_metrics_button.clicked.connect(self._toggle_metrics_widget)
        
        # Guardar referencias específicas para el modo pantalla completa
        self.linea_label = linea_label
        
        # Variable para la ventana flotante
        self.metrics_widget = None
        
        linea_layout.addWidget(linea_label)
        linea_layout.addWidget(self.linea_selector)
        linea_layout.addWidget(self.status_dual)
        linea_layout.addWidget(self.refresh_button)
        linea_layout.addWidget(self.float_metrics_button)
        linea_layout.addStretch()

        # Crear un widget contenedor para el layout de línea para poder ocultarlo fácilmente
        self.linea_container_widget = QtWidgets.QWidget()
        self.linea_container_widget.setLayout(linea_layout)

        # Interfaz simplificada - solo escaneo
        layout.addWidget(self.scan_input)
        layout.addWidget(self.linea_container_widget)

        # Tablas estilo BOM en splitter vertical
        self.table_scans = QtWidgets.QTableWidget(0, 10)
        self.table_scans.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_scans.setHorizontalHeaderLabels([
            "ID", "Fecha/Hora", "Raw", "Lote", "Secuencia", "Estación", "N° Parte", "Modelo", "Cantidad", "Línea"
        ])
        self.table_scans.horizontalHeader().setStretchLastSection(True)
        self.table_scans.setAlternatingRowColors(True)
        # Desactivar sorting en PCs lentas para mejor rendimiento
        sorting_enabled = os.environ.get('APP_PERFORMANCE_MODE') != 'OPTIMIZED'
        self.table_scans.setSortingEnabled(sorting_enabled)
        self.table_scans.verticalHeader().setVisible(False)
        
        # Configurar anchos de columna para mejor visualización
        header = self.table_scans.horizontalHeader()
        header.resizeSection(0, 50)   # ID
        header.resizeSection(1, 130)  # Fecha/Hora
        header.resizeSection(2, 100)  # Raw
        header.resizeSection(3, 80)   # Lote
        header.resizeSection(4, 80)   # Secuencia
        header.resizeSection(5, 60)   # Línea
        header.resizeSection(6, 100)  # N° Parte
        header.resizeSection(7, 100)  # Modelo
        header.resizeSection(8, 70)   # Cantidad
        # Tabla de totales (DESACTIVADA visualmente; mantenida para posible uso futuro)
        self.table_totals = QtWidgets.QTableWidget(0, 7)
        self.table_totals.setHorizontalHeaderLabels(["Fecha", "Línea", "N° Parte", "Modelo", "Cantidad Total", "UPH Target", "UPH Real"])
        self.table_totals.setVisible(False)  # Oculta de la interfaz

        # Ocultar tabla de escaneos individuales - solo mantener totales
        self.table_scans.setVisible(False)  # Oculta pero sigue existiendo en backend
        
        # Tabla de plan ⚡ SIN UPH Real/Proy/CT (ya tenemos cards)
        # Determinar número de columnas según modo SUB ASSY (ahora +1 por columna de Acciones)
        num_columns = 10 if (getattr(settings, 'SUB_ASSY_MODE', False) and 
                            getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY') else 9
        
        self.table_plan = QtWidgets.QTableWidget(0, num_columns)
        self.table_plan.setStyleSheet("""
            QTableWidget {font-size:14px; gridline-color:#2d3e50;}
            QHeaderView::section {background:#1f2d3a; color:#e0e0e0; font-weight:bold; font-size:13px; padding:4px;}
        """)
        self.table_plan.verticalHeader().setDefaultSectionSize(32)
        
        # Headers dinámicos según modo SUB ASSY (con nueva columna Acciones) - ⚡ Sin UPH Real/Proy/CT
        if num_columns == 10:  # SUB ASSY
            self.table_plan.setHorizontalHeaderLabels(["Part No", "Lote", "Modelo", "SUB ASSY", "Plan", "Producido", "% Avance", "UPH Target", "Estado", "Acciones"])
        else:  # Normal (9 columnas)
            self.table_plan.setHorizontalHeaderLabels(["Part No", "Lote", "Modelo", "Plan", "Producido", "% Avance", "UPH Target", "Estado", "Acciones"])
        self.table_plan.horizontalHeader().setStretchLastSection(True)
        self.table_plan.setAlternatingRowColors(True)
        self.table_plan.verticalHeader().setVisible(False)
        plan_header = self.table_plan.horizontalHeader()
        plan_header.resizeSection(0, 120)  # Part No
        plan_header.resizeSection(1, 90)   # Lote
        plan_header.resizeSection(2, 100)  # Modelo
        
        if num_columns == 10:  # Modo SUB ASSY con Acciones - ⚡ Sin UPH Real/Proy/CT
            plan_header.resizeSection(3, 80)   # SUB ASSY
            plan_header.resizeSection(4, 60)   # Plan
            plan_header.resizeSection(5, 70)   # Producido
            plan_header.resizeSection(6, 70)   # % Avance
            plan_header.resizeSection(7, 70)   # UPH Target
            plan_header.resizeSection(8, 80)   # Estado
            plan_header.resizeSection(9, 100)  # Acciones
        else:  # Modo normal con Acciones (9 columnas) - ⚡ Sin UPH Real/Proy/CT
            plan_header.resizeSection(3, 60)   # Plan
            plan_header.resizeSection(4, 70)   # Producido
            plan_header.resizeSection(5, 70)   # % Avance
            plan_header.resizeSection(6, 70)   # UPH Target
            plan_header.resizeSection(7, 80)   # Estado
            plan_header.resizeSection(8, 100)  # Acciones

        # Contenedor para totales y plan
        # Contenedor principal con título
        main_container = QtWidgets.QWidget()
        main_layout = QtWidgets.QVBoxLayout(main_container)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        # ⚡ TARJETAS DE TOTALES ARRIBA (antes del título)
        self.plan_totals_widget = self._create_plan_totals_widget()
        main_layout.addWidget(self.plan_totals_widget)
        
        self.title_plan = QtWidgets.QLabel("Plan de Producción (Línea Seleccionada)")
        self.title_plan.setStyleSheet("font-weight: bold; margin-top:8px;")
        main_layout.addWidget(self.title_plan)
        
        # Tabla de plan (sin splitter, tarjetas ya están arriba)
        main_layout.addWidget(self.table_plan)
        
        layout.addWidget(main_container)

        # Status bar (oculto - info en tarjetas)
        self.status = self.statusBar()
        self.status.hide()  # ⚡ Ocultamos la barra de abajo

        # Menú
        menubar = self.menuBar()
        admin_menu = menubar.addMenu("Opciones")
        
        # Opción para modo pantalla completa
        self.action_fullscreen = admin_menu.addAction("Modo Pantalla Completa (F11)")
        self.action_fullscreen.triggered.connect(self.toggle_fullscreen_mode)
        
        admin_menu.addSeparator()
        
        # Opción para configurar ubicación de DB local
        self.action_db_config = admin_menu.addAction("Configurar DB Local")
        self.action_db_config.triggered.connect(self.configure_db_location)
        
        admin_menu.addSeparator()
        
        # Opción de configuración general
        self.action_configuracion = admin_menu.addAction("Configuración")
        self.action_configuracion.triggered.connect(self.open_configuracion)

        # Conexiones simplificadas
        # ⚡ ESCANEO SIMPLIFICADO: Solo procesa con ENTER (escáner envía ENTER automáticamente)
        # Eliminado reconocimiento automático de QR/BARCODE para evitar procesamiento innecesario
        self._processing_scan = False
        self._scan_in_progress = False  # Flag para pausar actualizaciones durante escaneo
        self.scan_input.returnPressed.connect(self.handle_scan)  # ⚡ Solo ENTER
        self._last_processed_code = ""  # Para evitar duplicados
        self._last_processed_time = 0
        
        # Configuración por defecto - siempre modo rápido y auto-refresh
        self._fast_scan_mode = True
        self._auto_refresh_active = True

        # Atajo F2 para foco
        shortcut = QtGui.QShortcut(QtGui.QKeySequence("F2"), self)
        shortcut.activated.connect(lambda: self.scan_input.setFocus())
        
        # Atajo F11 para modo pantalla completa
        fullscreen_shortcut = QtGui.QShortcut(QtGui.QKeySequence("F11"), self)
        fullscreen_shortcut.activated.connect(self.toggle_fullscreen_mode)

        # ✅ Timer automático para actualizaciones de CARDS (15 SEGUNDOS - SQLite instantáneo!)
        # Ahora es SEGURO actualizar cada 5s porque SQLite local es < 1ms
        # ⚡ Reducido a 5s para detectar cambios de plan más rápido (antes 15s)
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(5_000)  # ✅ 5 SEGUNDOS (5,000 ms) - Lectura instantánea de SQLite
        self.timer.timeout.connect(self._update_tables_and_status)
        self.timer.start()  # Siempre activo - sin bloqueos gracias a Dual DB
        
        # ❌ DESHABILITADO - Timer de plan redundante (ahora _update_tables_and_status hace todo)
        # self.plan_timer = QtCore.QTimer(self)
        # self.plan_timer.setInterval(15_000)
        # self.plan_timer.timeout.connect(lambda: self.refresh_plan_only(force=False))
        # self.plan_timer.start()

        # Cambio de línea actualiza plan (con validación de plan en progreso)
        self.linea_selector.currentTextChanged.connect(self._on_linea_changed)
        
        # Timer para verificar estado del sistema dual
        self.dual_status_timer = QtCore.QTimer(self)
        self.dual_status_timer.setInterval(1_000)  # Cada segundo
        self.dual_status_timer.timeout.connect(self.update_status)
        self.dual_status_timer.start()
        
        # Timer para auto-pausa por inactividad (1.5 minutos sin escaneos)
        self.inactivity_timer = QtCore.QTimer(self)
        self.inactivity_timer.setInterval(30_000)  # Verificar cada 30 segundos (reducir accesos a SQLite)
        self.inactivity_timer.timeout.connect(self._check_inactivity)
        self.inactivity_timer.start()
        
        # Variable para rastrear el último escaneo por línea
        self._last_scan_time_per_line = {}  # {linea: timestamp}
        
        # Caché para plan en progreso (evitar consultas constantes)
        self._plan_en_progreso_cache = {}  # {linea: (plan_id, part_no, timestamp)}

        # Menú Admin siempre visible (sin control de usuarios)

        # Crear overlay de notificación para duplicados
        self._create_duplicate_overlay()
        # Crear overlay de OK
        self._create_ok_overlay()
        
        # Carga inicial - totales y status
        self.refresh_totals_only()
        self.refresh_plan_only()
        self.scan_input.setFocus()

        # Mantener foco en campo de escaneo (intervalo aumentado para PCs lentas)
        self._focus_timer = QtCore.QTimer(self)
        focus_interval = 2000 if os.environ.get('APP_PERFORMANCE_MODE') == 'OPTIMIZED' else 600
        self._focus_timer.setInterval(focus_interval)
        self._focus_timer.timeout.connect(self._ensure_scan_focus)
        self._focus_timer.start()
        
        # Crear botón para salir del modo pantalla completa (inicialmente oculto)
        self._create_exit_fullscreen_button()

    def showEvent(self, event):
        """Evento cuando se muestra la ventana - forzar icono en barra de tareas Windows"""
        super().showEvent(event)
        
        # Windows: Forzar icono en la barra de tareas usando Win32 API
        if hasattr(self, '_icon_set'):
            return  # Ya se estableció
            
        try:
            import sys
            if sys.platform == 'win32':
                import ctypes
                from ctypes import wintypes
                
                # Obtener el HWND de la ventana
                hwnd = int(self.winId())
                
                # Cargar el icono desde el archivo .ico
                icon_path = str(ROOT_DIR / "logoLogIn.ico")
                if os.path.exists(icon_path):
                    # Constantes de Windows
                    IMAGE_ICON = 1
                    LR_LOADFROMFILE = 0x00000010
                    LR_DEFAULTSIZE = 0x00000040
                    ICON_SMALL = 0
                    ICON_BIG = 1
                    WM_SETICON = 0x0080
                    
                    # Cargar icono grande (para barra de tareas)
                    hicon_big = ctypes.windll.user32.LoadImageW(
                        None,
                        icon_path,
                        IMAGE_ICON,
                        0, 0,  # tamaño (0 = usar tamaño predeterminado del icono)
                        LR_LOADFROMFILE | LR_DEFAULTSIZE
                    )
                    
                    # Cargar icono pequeño (para título de ventana)
                    hicon_small = ctypes.windll.user32.LoadImageW(
                        None,
                        icon_path,
                        IMAGE_ICON,
                        16, 16,  # tamaño pequeño
                        LR_LOADFROMFILE
                    )
                    
                    if hicon_big:
                        # Establecer icono grande (barra de tareas)
                        ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, ICON_BIG, hicon_big)
                    
                    if hicon_small:
                        # Establecer icono pequeño (título de ventana)
                        ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, ICON_SMALL, hicon_small)
                    
                    logger.info("✅ Icono de barra de tareas establecido correctamente")
                    self._icon_set = True
                    
        except Exception as e:
            logger.warning(f"No se pudo establecer el icono de la barra de tareas: {e}")

    def _create_plan_totals_widget(self):
        """Crea tarjetas grandes de totales estilo control_bom.css"""
        widget = QtWidgets.QFrame()
        widget.setMinimumHeight(140)  # ⚡ Altura mínima para que se vea
        widget.setStyleSheet("""
            QFrame {
                background-color: #34334E;
                border: 1px solid #20688C;
                padding: 8px;
            }
        """)
        
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)
        
        # Tarjeta 1: Plan Total de la Línea
        self.card_plan = self._create_metric_card("Plan", "0", "#3498DB")
        layout.addWidget(self.card_plan)
        
        # Tarjeta 2: Plan Acumulado (expectativa según tiempo transcurrido)
        self.card_resultado = self._create_metric_card("Plan Acumulado", "0", "#9B59B6")
        layout.addWidget(self.card_resultado)
        
        # Tarjeta 3: Producción acumulado
        self.card_produccion = self._create_metric_card("Producción", "0", "#27AE60")
        layout.addWidget(self.card_produccion)
        
        # Tarjeta 4: Eficiencia (basada en UPH y tiempo)
        self.card_eficiencia = self._create_metric_card("Eficiencia", "0%", "#F39C12")
        layout.addWidget(self.card_eficiencia)
        
        # Tarjeta 5: UPH (Unidades por Hora - Ventana Deslizante de 60 min)
        self.card_uph = self._create_metric_card("UPH (última hora)", "0", "#8E44AD")
        layout.addWidget(self.card_uph)
        
        # Tarjeta 6: UPPH (Unidades por Persona por Hora)
        self.card_uphu = self._create_metric_card("UPPH", "0", "#E74C3C")
        layout.addWidget(self.card_uphu)
        
        # ⚡ BARRA DE PROGRESO DE PRODUCCIÓN (estilo control_bom)
        progress_container = QtWidgets.QWidget()
        progress_container.setStyleSheet("background: transparent;")
        progress_layout = QtWidgets.QVBoxLayout(progress_container)
        progress_layout.setContentsMargins(8, 4, 8, 4)
        progress_layout.setSpacing(4)
        
        # Etiqueta de progreso
        progress_label = QtWidgets.QLabel("Progreso General de Producción")
        progress_label.setStyleSheet("""
            color: lightgray;
            font-size: 11px;
            font-weight: 500;
            background: transparent;
        """)
        progress_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        progress_layout.addWidget(progress_label)
        
        # Barra de progreso
        self.general_progress_bar = QtWidgets.QProgressBar()
        self.general_progress_bar.setMinimum(0)
        self.general_progress_bar.setMaximum(1000)  # x10 para mayor precisión
        self.general_progress_bar.setValue(0)
        self.general_progress_bar.setTextVisible(True)
        self.general_progress_bar.setFormat("%p%")
        self.general_progress_bar.setStyleSheet("""
            QProgressBar {
                border: 2px solid #20688C;
                border-radius: 4px;
                background-color: #3C3940;
                height: 24px;
                text-align: center;
                font-size: 12px;
                font-weight: bold;
                color: lightgray;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #27ae60, stop:0.5 #2ecc71, stop:1 #3be682);
                border-radius: 2px;
            }
        """)
        progress_layout.addWidget(self.general_progress_bar)
        
        # ⏱️ INDICADOR DE TIEMPO TRANSCURRIDO
        self.tiempo_transcurrido_label = QtWidgets.QLabel("⏱️ Tiempo transcurrido: -- min")
        self.tiempo_transcurrido_label.setStyleSheet("""
            color: #95a5a6;
            font-size: 10px;
            font-weight: 500;
            background: transparent;
            padding: 2px 0px;
        """)
        self.tiempo_transcurrido_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        progress_layout.addWidget(self.tiempo_transcurrido_label)
        
        # 📅 INDICADOR VISUAL DE FECHA DEL PLAN (PROMINENTE)
        from datetime import date
        fecha_hoy = date.today().strftime("%d/%m/%Y")
        self.fecha_plan_label = QtWidgets.QLabel(f"PLAN DEL DÍA: {fecha_hoy}")
        self.fecha_plan_label.setStyleSheet("""
            QLabel {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #2c3e50, stop:0.5 #34495e, stop:1 #2c3e50);
                color: #3498db;
                font-size: 14px;
                font-weight: bold;
                border: 2px solid #3498db;
                border-radius: 6px;
                padding: 8px 16px;
                margin: 4px 0px;
            }
        """)
        self.fecha_plan_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        progress_layout.addWidget(self.fecha_plan_label)
        
        # Layout vertical principal: tarjetas arriba + barra abajo
        main_layout = QtWidgets.QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(8)
        main_layout.addWidget(widget)  # Widget con las 6 tarjetas
        main_layout.addWidget(progress_container)  # Barra de progreso + fecha
        
        # Contenedor final
        final_widget = QtWidgets.QWidget()
        final_widget.setLayout(main_layout)
        
        return final_widget
    
    def _create_metric_card(self, title: str, value: str, color: str):
        """Crea una tarjeta estilo control_bom.css"""
        card = QtWidgets.QFrame()
        card.setMinimumHeight(100)  # ⚡ Altura mínima visible
        card.setMinimumWidth(120)   # ⚡ Ancho mínimo visible
        card.setStyleSheet(f"""
            QFrame {{
                background-color: #3C3940;
                border: 2px solid #20688C;
                border-radius: 4px;
                padding: 12px;
            }}
            QFrame:hover {{
                background-color: #45424A;
                border-color: #3498db;
            }}
        """)
        
        card_layout = QtWidgets.QVBoxLayout(card)
        card_layout.setContentsMargins(10, 10, 10, 10)
        card_layout.setSpacing(8)
        card_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        
        # Título (arriba, pequeño, estilo control_bom)
        title_label = QtWidgets.QLabel(title)
        title_label.setStyleSheet(f"""
            color: lightgray;
            font-size: 11px;
            font-weight: 500;
            border: none;
            background: transparent;
        """)
        title_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        card_layout.addWidget(title_label)
        
        # Valor (grande, centrado, con color)
        value_label = QtWidgets.QLabel(value)
        value_label.setStyleSheet(f"""
            color: {color};
            font-size: 32px;
            font-weight: bold;
            border: none;
            background: transparent;
        """)
        value_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        card_layout.addWidget(value_label)
        
        # Guardar referencia al label de valor para actualizar después
        card.value_label = value_label
        card.color = color
        
        return card
        
        # Barra de progreso general compacta
        progress_container = QtWidgets.QVBoxLayout()
        progress_container.setSpacing(4)
        
        progress_title = QtWidgets.QLabel("Progreso General de la Linea")
        progress_title.setStyleSheet("font-size: 10px; color: #95a5a6; font-weight: 500;")
        progress_title.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        progress_container.addWidget(progress_title)
        
        self.general_progress_bar = QtWidgets.QProgressBar()
        self.general_progress_bar.setMinimum(0)
        self.general_progress_bar.setMaximum(1000)  # Usar 1000 para mayor precisión
        self.general_progress_bar.setValue(0)
        self.general_progress_bar.setTextVisible(True)
        self.general_progress_bar.setFormat("%p%")
        self.general_progress_bar.setStyleSheet("""
            QProgressBar {
                border: 2px solid #20688C;
                border-radius: 4px;
                background-color: #34495e;
                height: 28px;
                text-align: center;
                font-size: 13px;
                font-weight: bold;
                color: white;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #27ae60, stop:0.5 #2ecc71, stop:1 #3be682);
                border-radius: 2px;
            }
            QProgressBar::chunk[value="0"] {
                background: transparent;
            }
        """)
        
        progress_container.addWidget(self.general_progress_bar)
        layout.addLayout(progress_container)
        
        return widget
    
    def _create_metric_label(self, title, value, value_color="#ecf0f1"):
        """Crea un label de métrica con título y valor (compacto)"""
        container = QtWidgets.QWidget()
        container.setStyleSheet("background: transparent;")
        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        
        title_label = QtWidgets.QLabel(title)
        title_label.setStyleSheet("font-size: 12px; color: #95a5a6; font-weight: 500;")
        title_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)
        
        value_label = QtWidgets.QLabel(value)
        value_label.setStyleSheet(f"font-size: 32px; font-weight: bold; color: {value_color};")
        value_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        value_label.setObjectName(f"{title}_value")  # Para poder actualizar después
        layout.addWidget(value_label)
        
        return container
    
    def _update_cards_with_metrics(self, plan: int, plan_acum: int, produccion: int, 
                                    eficiencia: float, uph: float, upph: float):
        """
        Actualiza las cards con métricas calculadas
        Método helper para evitar duplicación de código
        """
        try:
            # Actualizar tarjeta Plan Total
            if hasattr(self.card_plan, 'value_label') and self.card_plan.value_label:
                self.card_plan.value_label.setText(f"{int(plan)}")
                self.card_plan.value_label.repaint()  # Forzar refresco visual
            
            # Actualizar tarjeta Plan Acumulado
            if hasattr(self.card_resultado, 'value_label') and self.card_resultado.value_label:
                self.card_resultado.value_label.setText(f"{int(plan_acum)}")
                self.card_resultado.value_label.repaint()
            
            # Actualizar tarjeta Producción
            if hasattr(self.card_produccion, 'value_label') and self.card_produccion.value_label:
                self.card_produccion.value_label.setText(f"{int(produccion)}")
                self.card_produccion.value_label.repaint()
            
            # Actualizar tarjeta Eficiencia
            if hasattr(self.card_eficiencia, 'value_label') and self.card_eficiencia.value_label:
                self.card_eficiencia.value_label.setText(f"{eficiencia:.1f}%")
                self.card_eficiencia.value_label.repaint()
            
            # Actualizar tarjeta UPH
            if hasattr(self.card_uph, 'value_label') and self.card_uph.value_label:
                self.card_uph.value_label.setText(f"{int(uph)}")
                self.card_uph.value_label.repaint()
            
            # Actualizar tarjeta UPPH
            if hasattr(self.card_uphu, 'value_label') and self.card_uphu.value_label:
                self.card_uphu.value_label.setText(f"{upph:.2f}")
                self.card_uphu.value_label.repaint()
            
            # Actualizar barra de progreso
            if hasattr(self, 'general_progress_bar') and self.general_progress_bar:
                progress_value = int(eficiencia * 10)  # x10 para precisión
                progress_value = max(0, min(1000, progress_value))  # Clamp 0-1000
                self.general_progress_bar.setValue(progress_value)
                self.general_progress_bar.repaint()
            
            # Forzar repaint de todas las cards
            if hasattr(self, 'card_plan'):
                self.card_plan.repaint()
            if hasattr(self, 'card_resultado'):
                self.card_resultado.repaint()
            if hasattr(self, 'card_produccion'):
                self.card_produccion.repaint()
            if hasattr(self, 'card_eficiencia'):
                self.card_eficiencia.repaint()
            
            logger.debug(f"✅ Cards actualizadas: Plan={plan}, Prod={produccion}, Efic={eficiencia:.1f}%")
            
        except Exception as e:
            logger.error(f"❌ Error actualizando cards: {e}")
    
    def _update_plan_totals(self, plan_rows):
        """Actualiza las tarjetas con métricas de la línea
        
        � OPTIMIZACIÓN: Lee métricas desde caché SQLite (ultra-rápido, sin bloqueos)
        El worker en background actualiza el caché cada 3 segundos desde MySQL
        
        �📐 FÓRMULAS CANÓNICAS (Single Source of Truth):
        
        1. m_eff_total = Minutos efectivos del turno (duración - breaks del turno completo)
        2. m_eff_trans = Minutos efectivos transcurridos (tiempo corrido - breaks ya ocurridos)
        3. plan_acum = plan_total × (m_eff_trans / m_eff_total)
        4. eficiencia = (prod_real / plan_acum) × 100  [si plan_acum > 0]
        
        ⚠️ IMPORTANTE: UPH (última hora) NO interviene en eficiencia acumulada.
                      La eficiencia SOLO depende del tiempo efectivo transcurrido.
        
        Ejemplo (turno de 8h, 1 break de 30min):
        - m_eff_total = 480 - 30 = 450 min
        - A las 4h transcurridas con 15min de break: m_eff_trans = 240 - 15 = 225 min
        - plan_total = 1000 piezas
        - plan_acum = 1000 × (225/450) = 500 piezas
        - prod_real = 520 piezas
        - eficiencia = (520 / 500) × 100 = 104%
        """
        try:
            # Verificar que las tarjetas existan
            if not hasattr(self, 'card_plan') or not self.card_plan:
                return
            
            # ✅ LEER DIRECTAMENTE DE SQLITE (SIN CACHÉ - DATOS SIEMPRE FRESCOS)
            from ..services.dual_db import get_dual_db
            
            dual_db = get_dual_db()
            linea_seleccionada = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else ''
            
            # Leer datos directos de SQLite
            with dual_db._get_sqlite_connection(timeout=1.0) as conn:
                cursor = conn.cursor()
                
                # Plan total y producción total
                cursor.execute("""
                    SELECT SUM(plan_count), SUM(produced_count)
                    FROM plan_local
                    WHERE line = ?
                """, (linea_seleccionada,))
                
                result = cursor.fetchone()
                plan_total = result[0] or 0
                produccion_total = result[1] or 0
            
            # Calcular eficiencia simple
            if plan_total > 0:
                eficiencia = (produccion_total / plan_total) * 100
            else:
                eficiencia = 0.0
            
            # Actualizar cards con datos reales de SQLite
            self._update_cards_with_metrics(
                plan=plan_total,
                plan_acum=plan_total,
                produccion=produccion_total,
                eficiencia=eficiencia,
                uph=0,
                upph=0.0
            )
            
            logger.debug(f"✅ _update_plan_totals desde SQLite: Plan={plan_total}, Prod={produccion_total}, Efic={eficiencia:.1f}%")
            return
            
            # ===== CÓDIGO VIEJO DEL CACHÉ (DESHABILITADO) =====
            from ..services.metrics_cache import get_metrics_cache
            from datetime import date
            
            fecha_hoy = date.today().isoformat()
            
            metrics_cache = get_metrics_cache()
            cached_metrics = None
            
            if metrics_cache:
                cached_metrics = metrics_cache.get_metrics_from_cache(linea_seleccionada, fecha_hoy)
            
            # Si hay métricas en caché, usarlas (ultra-rápido)
            if cached_metrics:
                logger.debug(f"⚡ Usando métricas desde caché para {linea_seleccionada}")
                
                plan = cached_metrics['plan_total']
                plan_acumulado = cached_metrics['plan_acumulado']
                produccion_acumulada = cached_metrics['produccion_real']
                eficiencia = cached_metrics['eficiencia']
                uph = cached_metrics['uph']
                upph = cached_metrics['upph']
                
                # Actualizar cards
                self._update_cards_with_metrics(
                    plan, plan_acumulado, produccion_acumulada,
                    eficiencia, uph, upph
                )
                
                logger.debug(f"✅ Cards actualizadas desde caché: Eficiencia={eficiencia:.1f}%")
                return
            
            # Si no hay caché, calcular métricas tradicional (fallback)
            logger.debug(f"⚠️ Sin caché, calculando métricas tradicional")
            
            # ========== IMPORTS Y CONSTANTES (SIEMPRE) ==========
            import datetime
            from zoneinfo import ZoneInfo
            
            monterrey_tz = ZoneInfo("America/Monterrey")
            ahora = datetime.datetime.now(monterrey_tz)
            
            # Si no hay planes, mostrar ceros y mensaje
            if not plan_rows:
                plan = resultado = produccion_acumulada = eficiencia = uph = upph = 0
                logger.info("📋 Sin plan cargado - mostrando valores en cero")
            else:
                # ========== BREAKS ESTÁNDAR ==========
                # Breaks estándar (hora, minuto, duración_minutos)
                BREAKS = [
                    (9, 30, 15),   # 09:30-09:45
                    (12, 0, 30),   # 12:00-12:30 (comida)
                    (15, 0, 15),   # 15:00-15:15
                ]
                
                # ========== CALCULAR PLAN TOTAL DE LA LÍNEA ==========
                plan_total_linea = sum(r.get('plan_count', 0) or 0 for r in plan_rows)
                logger.info(f"🔍 DEBUG: Número de planes en línea: {len(plan_rows)}")
                for idx, r in enumerate(plan_rows, 1):
                    logger.info(f"   Plan {idx}: part_no={r.get('part_no')}, plan_count={r.get('plan_count')}, produced={r.get('produced_count')}, status={r.get('status')}")
                logger.info(f"🔍 DEBUG: Plan total línea (suma de plan_count): {plan_total_linea}")
                
                # ========== FUNCIÓN: CALCULAR MINUTOS EFECTIVOS ==========
                def calcular_minutos_efectivos_plan(plan_dict, ahora_dt):
                    """
                    Retorna (m_eff_trans, m_eff_total) para un plan.
                    
                    m_eff_trans = minutos efectivos transcurridos (con breaks descontados)
                    m_eff_total = minutos efectivos totales del plan (con breaks descontados)
                    
                    Lógica:
                    1. Obtener M_tot de effective_minutes (o calcular desde fechas, o default 450min)
                    2. Si no hay fechas → asumir 50% transcurrido
                    3. Si hay fechas:
                       - t_ini = planned_start
                       - t_fin = min(ahora, planned_end)
                       - m_raw = minutos brutos (t_fin - t_ini)
                       - m_brk = suma de overlaps con breaks
                       - m_eff_trans = max(0, m_raw - m_brk), limitado a M_tot
                    """
                    # Intentar obtener M_tot de effective_minutes
                    M_tot = plan_dict.get('effective_minutes', 0) or 0
                    
                    # Obtener planned_start y planned_end
                    planned_start_str = plan_dict.get('planned_start')
                    planned_end_str = plan_dict.get('planned_end')
                    
                    # FALLBACK 1: Si no hay effective_minutes pero HAY fechas, calcular la diferencia
                    if M_tot == 0 and planned_start_str and planned_end_str:
                        try:
                            from zoneinfo import ZoneInfo
                            monterrey_tz = ZoneInfo("America/Monterrey")
                            planned_start = datetime.datetime.fromisoformat(planned_start_str).replace(tzinfo=monterrey_tz)
                            planned_end = datetime.datetime.fromisoformat(planned_end_str).replace(tzinfo=monterrey_tz)
                            
                            # Calcular diferencia total (asumiendo que effective_minutes = tiempo total)
                            M_tot = int((planned_end - planned_start).total_seconds() / 60)
                            logger.debug(f"📊 Plan {plan_dict.get('part_no')}: calculado M_tot={M_tot} min desde fechas")
                        except Exception as e:
                            logger.warning(f"⚠️ Error calculando M_tot desde fechas para {plan_dict.get('part_no')}: {e}")
                            M_tot = 0
                    
                    # FALLBACK 2: Si AÚN no hay M_tot, usar valor por defecto (450 min = 7.5h)
                    if M_tot == 0:
                        M_tot = 450  # Valor por defecto: 7.5 horas efectivas
                        logger.debug(f"📊 Plan {plan_dict.get('part_no')}: usando M_tot por defecto={M_tot} min")
                    
                    # Si no hay fechas de plan, asumir que estamos a mitad del tiempo
                    if not planned_start_str or not planned_end_str:
                        # Sin fechas, asumir que estamos al 50% del tiempo
                        M_trans = M_tot // 2
                        logger.debug(f"📊 Plan {plan_dict.get('part_no')}: sin fechas, asumiendo 50% transcurrido (M_trans={M_trans})")
                        return (M_trans, M_tot)
                    
                    try:
                        # Convertir strings a datetime
                        from zoneinfo import ZoneInfo
                        monterrey_tz = ZoneInfo("America/Monterrey")
                        
                        # Parsear fechas (formato: "YYYY-MM-DD HH:MM:SS")
                        planned_start = datetime.datetime.fromisoformat(planned_start_str).replace(tzinfo=monterrey_tz)
                        planned_end = datetime.datetime.fromisoformat(planned_end_str).replace(tzinfo=monterrey_tz)
                        
                        # ========== VENTANA VIGENTE DEL PLAN ==========
                        t_ini = planned_start
                        t_fin = min(ahora_dt, planned_end)
                        
                        # Si aún no ha empezado el plan, M_trans = 0
                        if ahora_dt < planned_start:
                            return (0, M_tot)
                        
                        # Si ya terminó el plan, M_trans = M_tot
                        if ahora_dt >= planned_end:
                            return (M_tot, M_tot)
                        
                        # ========== CALCULAR m_raw (minutos brutos transcurridos) ==========
                        m_raw = max(0, int((t_fin - t_ini).total_seconds() / 60))
                        
                        # ========== CALCULAR m_brk (minutos de breaks en la ventana [t_ini, t_fin]) ==========
                        m_brk = 0
                        
                        for hora_break, minuto_break, duracion_break in BREAKS:
                            # Crear datetime del break usando el mismo día que t_ini
                            inicio_break = t_ini.replace(hour=hora_break, minute=minuto_break, second=0, microsecond=0)
                            fin_break = inicio_break + datetime.timedelta(minutes=duracion_break)
                            
                            # Calcular overlap entre [inicio_break, fin_break] y [t_ini, t_fin]
                            overlap_start = max(inicio_break, t_ini)
                            overlap_end = min(fin_break, t_fin)
                            
                            if overlap_start < overlap_end:
                                # Hay overlap
                                overlap_minutos = int((overlap_end - overlap_start).total_seconds() / 60)
                                m_brk += overlap_minutos
                                logger.debug(f"🚫 Break {hora_break:02d}:{minuto_break:02d} overlap: {overlap_minutos} min")
                        
                        # ========== CALCULAR m_eff_trans (minutos efectivos transcurridos) ==========
                        m_eff_trans = min(max(0, m_raw - m_brk), M_tot)
                        
                        logger.debug(f"📊 Plan {plan_dict.get('part_no')}: m_raw={m_raw}, m_brk={m_brk}, m_eff_trans={m_eff_trans}, M_tot={M_tot}")
                        
                        return (m_eff_trans, M_tot)
                        
                    except Exception as e:
                        logger.error(f"❌ Error calculando M_trans para plan {plan_dict.get('part_no')}: {e}")
                        # En caso de error, asumir 50% transcurrido
                        return (M_tot // 2, M_tot)
                
                # ========== FUNCIÓN AUXILIAR: CALCULAR PLAN ACUMULADO POR PLAN ==========
                def calcular_plan_acumulado_plan(plan_dict, M_trans, M_tot):
                    """
                    Calcula plan_acum_plan = round(plan_count × (M_trans / M_tot))
                    
                    Args:
                        plan_dict: Diccionario con datos del plan (plan_count)
                        M_trans: Minutos efectivos transcurridos
                        M_tot: Minutos efectivos totales
                    
                    Returns:
                        int: Plan acumulado para este plan específico
                    """
                    plan_count = plan_dict.get('plan_count', 0) or 0
                    
                    if M_tot == 0 or plan_count == 0:
                        return 0
                    
                    # f = M_trans / M_tot (fracción de avance del tiempo)
                    f = M_trans / M_tot
                    
                    # plan_acum_plan = round(plan_count × f)
                    plan_acum_plan = round(plan_count * f)
                    
                    return plan_acum_plan
                
                # ========== FUNCIÓN AUXILIAR: CALCULAR EFICIENCIA POR PLAN ==========
                def calcular_eficiencia_plan(produced_count, plan_acum_plan):
                    """
                    Calcula Efic% = (produced_count / plan_acum_plan) × 100
                    
                    Args:
                        produced_count: Producción real del plan
                        plan_acum_plan: Plan acumulado del plan
                    
                    Returns:
                        float: Eficiencia en porcentaje
                    """
                    if plan_acum_plan == 0:
                        return 0.0
                    
                    eficiencia = (produced_count / plan_acum_plan) * 100
                    return eficiencia
                
                # ========== NUEVAS FÓRMULAS BASADAS EN PLAN_MAIN ==========
                # Ahora trabajamos con cada plan individual y luego sumamos a nivel línea
                
                logger.info(f"⏱️ Hora actual: {ahora.strftime('%H:%M:%S')}")
                
                # Variables acumuladas a nivel línea
                Plan_acum_linea = 0
                Prod_acum_linea = 0
                
                # Procesar TODOS los planes de la línea (no solo el "en progreso")
                for plan_dict in plan_rows:
                    part_no = plan_dict.get('part_no', '')
                    plan_count = plan_dict.get('plan_count', 0) or 0
                    produced_count = plan_dict.get('produced_count', 0) or 0
                    
                    # Calcular M_trans y M_tot para este plan
                    M_trans, M_tot = calcular_minutos_efectivos_plan(plan_dict, ahora)
                    
                    # Calcular plan_acum_plan para este plan
                    plan_acum_plan = calcular_plan_acumulado_plan(plan_dict, M_trans, M_tot)
                    
                    # Acumular a nivel línea
                    Plan_acum_linea += plan_acum_plan
                    Prod_acum_linea += produced_count
                    
                    logger.info(f"🔍 Plan '{part_no}': plan_count={plan_count}, M_trans={M_trans}min, M_tot={M_tot}min, plan_acum={plan_acum_plan}, produced={produced_count}, status={plan_dict.get('status')}")
                
                # Plan total ya se calculó arriba (plan_total_linea)
                plan = plan_total_linea
                
                # Resultado = Plan acumulado a nivel línea
                resultado = Plan_acum_linea
                plan_acumulado = Plan_acum_linea
                
                logger.info(f"🔍 DEBUG FINAL: Plan_acum_linea (suma) = {Plan_acum_linea}, Prod_acum_linea (suma) = {Prod_acum_linea}")
                
                # Producción acumulada = Suma de produced_count de todos los planes
                produccion_acumulada = Prod_acum_linea
                
                # Eficiencia a nivel línea = (Prod_acum_linea / Plan_acum_linea) × 100
                if Plan_acum_linea > 0:
                    eficiencia = (Prod_acum_linea / Plan_acum_linea) * 100
                    # Limitar a 999.9% para evitar valores absurdos
                    if eficiencia > 999.9:
                        logger.warning(f"⚠️ Eficiencia muy alta ({eficiencia:.1f}%), limitando a 999.9%")
                        eficiencia = 999.9
                else:
                    eficiencia = 0.0
                
                logger.info(f"📊 LÍNEA {linea_seleccionada}: Plan_total={plan_total_linea}, Plan_acum={Plan_acum_linea}, Prod_acum={Prod_acum_linea}, Efic={eficiencia:.1f}%")
                
                # ========== CÁLCULO DE UPH (mantener lógica anterior) ==========
                uph = 0
                upph = 0
                
                # UPH se mantiene desde producción real en ventana de 60 min
                # Este cálculo NO cambia con las nuevas fórmulas
                import time
                cache_key = f"uph_{linea_seleccionada}"
                cache_time_key = f"uph_time_{linea_seleccionada}"
                
                if not hasattr(self, '_uph_cache'):
                    self._uph_cache = {}
                    self._uph_cache_time = {}
                
                current_time = time.time()
                cache_valid = (
                    cache_key in self._uph_cache and 
                    cache_time_key in self._uph_cache_time and
                    (current_time - self._uph_cache_time.get(cache_time_key, 0)) < 5
                )
                
                if cache_valid:
                    uph = self._uph_cache[cache_key]
                    logger.debug(f"⚡ UPH desde cache: {uph:.1f}")
                else:
                    try:
                        # ⚡ OPTIMIZADO: Calcular UPH desde SQLite local (no bloquea)
                        from ..services.dual_db import get_dual_db
                        dual_db = get_dual_db()
                        
                        # Obtener UPH de últimos 60 minutos desde SQLite (ultra-rápido)
                        t1 = ahora
                        t0 = t1 - datetime.timedelta(minutes=60)
                        
                        # Usar método optimizado del dual_db (lee de SQLite local)
                        try:
                            with dual_db._get_sqlite_connection(timeout=0.5) as conn:
                                cursor = conn.execute("""
                                    SELECT COUNT(*)/2 as N
                                    FROM scans_local 
                                    WHERE linea = ? 
                                    AND ts >= ? 
                                    AND ts <= ?
                                    AND is_complete = 1
                                """, (linea_seleccionada, t0.isoformat(), t1.isoformat()))
                                result_row = cursor.fetchone()
                                N = int(result_row[0]) if result_row and result_row[0] else 0
                                
                                # UPH = piezas completas en 60 min
                                uph = N
                                
                                # Guardar en cache por 5 segundos
                                self._uph_cache[cache_key] = uph
                                self._uph_cache_time[cache_time_key] = current_time
                                
                                logger.debug(f"⚡ UPH calculado desde SQLite: {uph} piezas")
                        except Exception as e_sqlite:
                            logger.debug(f"⚠️ Error calculando UPH desde SQLite: {e_sqlite}")
                            uph = 0
                    except Exception as e:
                        logger.error(f"❌ Error calculando UPH: {e}")
                        uph = 0
                
                logger.info(f"📊 Resumen final: Plan={plan} | Plan_acum={plan_acumulado} | Prod={produccion_acumulada} | Efic={eficiencia:.1f}% | UPH={uph}")
                
                # ========== OBTENER NÚMERO DE PERSONAS DESDE TABLA RAW DE MYSQL ==========
                num_personas = 6  # Valor por defecto
                
                # Buscar un plan EN PROGRESO para obtener nparte
                nparte = ''
                for r in plan_rows:
                    status = r.get('status', '')
                    if 'PROGRESO' in status.upper():
                        nparte = r.get('part_no', '')
                        break
                
                # Si no hay plan en progreso, tomar el primer plan de la lista
                if not nparte and plan_rows:
                    nparte = plan_rows[0].get('part_no', '')
                
                if nparte:  # Si tenemos número de parte
                    try:
                        # ⚡ OPTIMIZADO: Usar caché para personas (evita consulta MySQL bloqueante)
                        personas_cache_key = f"personas_{nparte}"
                        personas_cache_time_key = f"personas_time_{nparte}"
                        
                        if not hasattr(self, '_personas_cache'):
                            self._personas_cache = {}
                            self._personas_cache_time = {}
                        
                        current_time_personas = time.time()
                        personas_cache_valid = (
                            personas_cache_key in self._personas_cache and 
                            personas_cache_time_key in self._personas_cache_time and
                            (current_time_personas - self._personas_cache_time.get(personas_cache_time_key, 0)) < 60  # Cache por 60 segundos
                        )
                        
                        if personas_cache_valid:
                            num_personas = self._personas_cache[personas_cache_key]
                            logger.debug(f"👥 Personas desde cache: {num_personas}")
                        else:
                            # Consultar MySQL solo si no está en cache (en background no bloquea tanto)
                            with self.db.get_connection() as conn:
                                with conn.cursor() as cursor:
                                    cursor.execute("""
                                        SELECT persona_directo 
                                        FROM raw 
                                        WHERE part_no = %s 
                                        LIMIT 1
                                    """, (nparte,))
                                    
                                    result = cursor.fetchone()
                                    if result and result.get('persona_directo'):
                                        num_personas_raw = result['persona_directo']
                                        if num_personas_raw and num_personas_raw > 0:
                                            num_personas = int(num_personas_raw)
                                            # Guardar en cache
                                            self._personas_cache[personas_cache_key] = num_personas
                                            self._personas_cache_time[personas_cache_time_key] = current_time_personas
                                            logger.debug(f"👥 Personas obtenidas de MySQL para {nparte}: {num_personas}")
                                        else:
                                            num_personas = 6
                                    else:
                                        num_personas = 6
                            
                    except Exception as e:
                        logger.debug(f"⚠️ Error obteniendo personas: {e}")
                        from ..config import settings as _settings
                        num_personas = getattr(_settings, 'NUM_PERSONAS_LINEA', 6)
                else:
                    # Si no hay nparte, usar valor por defecto
                    from ..config import settings as _settings
                    num_personas = getattr(_settings, 'NUM_PERSONAS_LINEA', 6)
                    logger.info(f"👥 Sin part_no, usando personas por defecto: {num_personas}")
                
                # ========== CÁLCULO DE UPPH ==========
                # UPPH = UPH / número_personas
                upph = (uph / num_personas) if num_personas > 0 and uph > 0 else 0
                
                logger.info(f"📊 Métricas finales: CT={num_personas} personas | UPH={uph:.1f} | UPPH={upph:.2f}")
            
            # ⚡ Actualizar TARJETAS (verificar que existan)
            if hasattr(self.card_plan, 'value_label') and self.card_plan.value_label:
                # PLAN TOTAL = Meta del día completo (suma de todos los modelos de la línea)
                if 'plan_total_linea' in locals() and plan_total_linea > 0:
                    self.card_plan.value_label.setText(f"{int(plan_total_linea)}")
                else:
                    self.card_plan.value_label.setText(str(plan))
            
            if hasattr(self.card_resultado, 'value_label') and self.card_resultado.value_label:
                # PLAN ACUMULADO = Cuántas piezas DEBERÍAN llevar desde el inicio del turno
                if 'plan_acumulado' in locals() and plan_acumulado > 0:
                    self.card_resultado.value_label.setText(f"{int(plan_acumulado)}")
                else:
                    self.card_resultado.value_label.setText("0")
            
            if hasattr(self.card_produccion, 'value_label') and self.card_produccion.value_label:
                # PRODUCCIÓN = Cuántas piezas REALMENTE han producido desde el inicio del turno
                if 'produccion_acumulada' in locals():
                    self.card_produccion.value_label.setText(str(produccion_acumulada))
                else:
                    self.card_produccion.value_label.setText(str(resultado))
            if hasattr(self.card_eficiencia, 'value_label') and self.card_eficiencia.value_label:
                self.card_eficiencia.value_label.setText(f"{eficiencia:.1f}%")
            if hasattr(self.card_uph, 'value_label') and self.card_uph.value_label:
                # Mostrar UPH calculado (real)
                self.card_uph.value_label.setText(f"{int(uph)}")
            if hasattr(self.card_uphu, 'value_label') and self.card_uphu.value_label:
                self.card_uphu.value_label.setText(f"{upph:.2f}")
            
            # ⚡ Actualizar BARRA DE PROGRESO
            if hasattr(self, 'general_progress_bar') and self.general_progress_bar:
                progress_value = int(eficiencia * 10)  # x10 para precisión
                self.general_progress_bar.setValue(progress_value)
                
                # Cambiar color según progreso (estilo control_bom)
                if eficiencia >= 100:
                    chunk_color = "stop:0 #27ae60, stop:0.5 #2ecc71, stop:1 #3be682"  # Verde brillante
                elif eficiencia >= 80:
                    chunk_color = "stop:0 #27ae60, stop:1 #2ecc71"  # Verde
                elif eficiencia >= 50:
                    chunk_color = "stop:0 #f39c12, stop:1 #f1c40f"  # Amarillo/Naranja
                elif eficiencia >= 25:
                    chunk_color = "stop:0 #e67e22, stop:1 #d35400"  # Naranja
                else:
                    chunk_color = "stop:0 #e74c3c, stop:1 #c0392b"  # Rojo
                
                self.general_progress_bar.setStyleSheet(f"""
                    QProgressBar {{
                        border: 2px solid #20688C;
                        border-radius: 4px;
                        background-color: #3C3940;
                        height: 24px;
                        text-align: center;
                        font-size: 12px;
                        font-weight: bold;
                        color: lightgray;
                    }}
                    QProgressBar::chunk {{
                        background: qlineargradient(x1:0, y1:0, x2:1, y2:0, {chunk_color});
                        border-radius: 2px;
                    }}
                """)
            
            # ⏱️ Actualizar INDICADOR DE TIEMPO (opcional - puede ser removido ya que usamos planned_start/end)
            if hasattr(self, 'tiempo_transcurrido_label') and self.tiempo_transcurrido_label:
                # Ya no usamos tiempo_transcurrido_min global, pero podemos mostrar hora actual
                tiempo_texto = f"⏱️ Hora actual: {ahora.strftime('%H:%M')}"
                color = "#27ae60"  # Verde
                
                self.tiempo_transcurrido_label.setText(tiempo_texto)
                self.tiempo_transcurrido_label.setStyleSheet(f"""
                    color: {color};
                    font-size: 10px;
                    font-weight: 600;
                    background: transparent;
                    padding: 2px 0px;
                """)
            
            # Sincronizar métricas con ventana flotante (si está abierta)
            self._sync_metrics_to_widget()
            
        except Exception as e:
            print(f"Error actualizando tarjetas de totales: {e}")
    
    def _verificar_plan_en_progreso(self):
        """Verifica si hay algún plan actualmente en progreso"""
        try:
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            # Verificar en la base de datos si hay planes en estado "EN PROGRESO"
            plans_en_progreso = dual_db.verificar_planes_en_progreso()
            return plans_en_progreso
            
        except Exception as e:
            import logging
            logging.error(f"Error verificando planes en progreso: {e}")
            return []

    def _verificar_plan_en_progreso_por_linea(self, linea):
        """⚡ Verifica planes EN PROGRESO leyendo directo de SQLite"""
        try:
            from ..services.dual_db import get_dual_db
            import sqlite3
            
            dual_db = get_dual_db()
            
            # Leer directamente de SQLite (rápido, < 5ms)
            with dual_db._get_sqlite_connection(timeout=1.0) as conn:
                cursor = conn.execute("""
                    SELECT part_no FROM plan_local
                    WHERE line = ? AND status = 'EN PROGRESO'
                """, (linea,))
                
                planes_activos = [row[0] for row in cursor.fetchall()]
                return planes_activos
            
        except Exception as e:
            import logging
            logging.error(f"Error verificando planes en progreso: {e}")
            return []

    def _cambiar_estado_plan(self, plan_id, part_no, nuevo_estado, linea=None):
        """Cambia el estado de un plan específico usando su ID único (OPTIMIZADO - Caché primero)"""
        try:
            # Si se quiere iniciar un plan, verificar que no haya otro en progreso EN LA MISMA LÍNEA
            if nuevo_estado == "EN PROGRESO" and linea:
                planes_en_progreso = self._verificar_plan_en_progreso_por_linea(linea)
                
                if planes_en_progreso:
                    # Filtrar si el plan actual ya está en progreso
                    otros_planes = [plan for plan in planes_en_progreso if plan != part_no]
                    
                    if otros_planes:
                        # Usar notificación estilo overlay en lugar de QMessageBox
                        self._show_success_notification(
                            "Plan en Progreso",
                            f"No se puede iniciar {part_no}\nYa hay plan activo en {linea}: {', '.join(otros_planes)}",
                            "#ffc107"  # Color amarillo/naranja para advertencia
                        )
                        return
            
            # ========== OPTIMIZACIÓN: ACTUALIZAR CACHÉ LOCAL PRIMERO (INSTANTÁNEO) ==========
            # Esto hace que la UI se actualice INMEDIATAMENTE sin esperar SQLite/MySQL
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            # Actualizar caché en memoria (no bloquea, es instantáneo)
            dual_db.actualizar_estado_plan_cache_only(plan_id, nuevo_estado, linea)
            
            # Invalidar caché de plan en progreso para forzar reconsulta
            if hasattr(self, '_plan_en_progreso_cache'):
                self._plan_en_progreso_cache.pop(linea, None)
            
            # ⚡ REFRESCAR UI INMEDIATAMENTE desde caché (0ms, no toca BD)
            self._refresh_plan_from_cache_only()
            
            # Mostrar notificación de éxito INMEDIATA (porque el caché ya está actualizado)
            self._show_success_notification(
                "Estado Actualizado", 
                f"Plan {part_no}: {nuevo_estado}",
                "#28a745" if nuevo_estado == "EN PROGRESO" else 
                "#ffc107" if nuevo_estado == "PAUSADO" else "#dc3545"
            )
            
            # ========== BACKGROUND: Sincronizar con SQLite y MySQL (NO bloquea) ==========
            # Crear worker thread SOLO para actualizar bases de datos
            worker = EstadoPlanWorker(plan_id, nuevo_estado, part_no, linea)
            worker.finished.connect(lambda success, msg: self._on_db_sync_finished(success, msg))
            
            # Guardar referencia al worker para que no se destruya
            if not hasattr(self, '_estado_workers'):
                self._estado_workers = []
            self._estado_workers.append(worker)
            worker.finished.connect(lambda: self._estado_workers.remove(worker) if worker in self._estado_workers else None)
            
            # Iniciar worker en background (SQLite + MySQL se actualizan sin bloquear)
            worker.start()
                
        except Exception as e:
            logger.error(f"Error iniciando cambio de estado del plan {part_no}: {e}")
            self._show_success_notification(
                "Error",
                f"Error al cambiar estado: {str(e)}",
                "#dc3545"
            )
    
    def _on_db_sync_finished(self, success, message):
        """Callback cuando la sincronización de BD termina (actualiza plan después de BD)"""
        if success:
            logger.debug(f"✅ {message}")
            # Ahora SÍ refrescar plan (BD ya fue actualizada, no hay riesgo de lock)
            try:
                self.refresh_plan_only(force=True)
            except Exception as e:
                logger.debug(f"Error refrescando plan después de sync: {e}")
        else:
            logger.warning(f"⚠️ {message}")
            # Aunque falle la BD, el caché ya está actualizado (UI ya muestra el cambio)
            # La sincronización se reintentará automáticamente
    
    def _terminar_plan(self, plan_id, part_no, linea=None):
        """Termina un plan cambiando su estado a TERMINADO (OPTIMIZADO - Caché primero)"""
        try:
            # Confirmar con el usuario
            reply = QtWidgets.QMessageBox.question(
                self,
                "Confirmar Terminación",
                f"¿Está seguro de que desea TERMINAR el plan {part_no}?\n\nEsta acción finalizará el plan actual.",
                QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
                QtWidgets.QMessageBox.StandardButton.No
            )
            
            if reply == QtWidgets.QMessageBox.StandardButton.Yes:
                # ========== ACTUALIZAR CACHÉ PRIMERO (INSTANTÁNEO) ==========
                from ..services.dual_db import get_dual_db
                dual_db = get_dual_db()
                
                # Actualizar caché en memoria (instantáneo)
                dual_db.actualizar_estado_plan_cache_only(plan_id, "TERMINADO", linea)
                
                # Invalidar caché de plan en progreso
                if hasattr(self, '_plan_en_progreso_cache'):
                    self._plan_en_progreso_cache.pop(linea, None)
                
                # NO REFRESCAR PLAN AQUÍ - El worker lo hará cuando termine de actualizar BD
                # Esto evita congelamiento por locks de SQLite
                
                # Notificación de éxito inmediata
                self._show_success_notification(
                    "Plan Terminado",
                    f"Plan {part_no} finalizado",
                    "#dc3545"  # Rojo
                )
                
                # ========== BACKGROUND: Sincronizar BD (NO bloquea) ==========
                worker = EstadoPlanWorker(plan_id, "TERMINADO", part_no, linea)
                worker.finished.connect(lambda success, msg: self._on_db_sync_finished(success, msg))
                
                # Guardar referencia al worker
                if not hasattr(self, '_estado_workers'):
                    self._estado_workers = []
                self._estado_workers.append(worker)
                worker.finished.connect(lambda: self._estado_workers.remove(worker) if worker in self._estado_workers else None)
                
                # Iniciar worker (no bloquea)
                worker.start()
                    
        except Exception as e:
            import logging
            logging.error(f"Error terminando plan {part_no}: {e}")
            QtWidgets.QMessageBox.critical(
                self, 
                "Error", 
                f"Error al terminar el plan: {str(e)}"
            )
    
    def _on_linea_changed(self, nueva_linea: str):
        """Maneja el cambio de línea con validación de plan en progreso"""
        try:
            # Si no hay nueva línea seleccionada, no hacer nada
            if not nueva_linea:
                return
            
            # Si es la misma línea, no hacer nada (evitar llamadas redundantes)
            if hasattr(self, '_linea_anterior') and self._linea_anterior == nueva_linea:
                return
            
            # 🚀 Notificar al caché de métricas sobre el cambio de línea
            try:
                from ..services.metrics_cache import get_metrics_cache
                metrics_cache = get_metrics_cache()
                if metrics_cache:
                    metrics_cache.set_active_line(nueva_linea)
                    logger.info(f"🎯 Caché de métricas actualizado a línea: {nueva_linea}")
            except Exception as cache_err:
                logger.debug(f"Error actualizando línea activa en caché: {cache_err}")
            
            # NOTA: Permitir cambio de línea libremente
            # La validación de plan EN PROGRESO se hace en el escaneo, no en el cambio de línea
            # Solo actualizar la línea anterior y refrescar
            self._linea_anterior = nueva_linea
            self.refresh_plan_only(force=True)  # Forzar actualización inmediata
            
        except Exception as e:
            import logging
            logging.error(f"Error en cambio de línea: {e}")
            # En caso de error, permitir el cambio
            self._linea_anterior = nueva_linea
            self.refresh_plan_only(force=True)  # Forzar actualización inmediata
    
    def _check_inactivity(self):
        """Verifica inactividad y pausa automáticamente el plan si no hay escaneos en 1.5 minutos"""
        try:
            # Obtener la línea actualmente seleccionada
            linea_actual = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else None
            if not linea_actual:
                return
            
            # Verificar si hay un plan en progreso en la línea actual
            planes_en_progreso = self._verificar_plan_en_progreso_por_linea(linea_actual)
            
            if not planes_en_progreso:
                # No hay plan en progreso, invalidar caché y no hacer nada
                self._plan_en_progreso_cache.pop(linea_actual, None)
                return
            
            # Obtener el último tiempo de escaneo para esta línea
            ultimo_escaneo = self._last_scan_time_per_line.get(linea_actual)
            
            if ultimo_escaneo is None:
                # Primera vez que se verifica, guardar tiempo actual
                import time
                self._last_scan_time_per_line[linea_actual] = time.time()
                return
            
            # Calcular tiempo transcurrido desde el último escaneo
            import time
            tiempo_transcurrido = time.time() - ultimo_escaneo
            
            # Si ha pasado más de 90 segundos (1.5 minutos) sin escaneo
            TIEMPO_INACTIVIDAD = 90  # 90 segundos = 1.5 minutos
            
            if tiempo_transcurrido >= TIEMPO_INACTIVIDAD:
                # Obtener el plan_id del plan en progreso (usar caché si está disponible)
                from ..services.dual_db import get_dual_db
                dual_db = get_dual_db()
                
                # Verificar caché primero (válido por 60 segundos)
                cache_entry = self._plan_en_progreso_cache.get(linea_actual)
                if cache_entry and (time.time() - cache_entry[2]) < 60:
                    plan_id, part_no = cache_entry[0], cache_entry[1]
                else:
                    # Consultar SQLite solo si caché expiró o no existe
                    import sqlite3
                    with sqlite3.connect(dual_db.sqlite_path, timeout=5.0) as conn:
                        cursor = conn.execute("""
                            SELECT id, part_no FROM plan_local 
                            WHERE status = 'EN PROGRESO' AND line = ?
                            LIMIT 1
                        """, (linea_actual,))
                        
                        result = cursor.fetchone()
                        if not result:
                            # No hay plan en progreso, invalidar caché
                            self._plan_en_progreso_cache.pop(linea_actual, None)
                            return
                        
                        plan_id, part_no = result
                        # Actualizar caché
                        self._plan_en_progreso_cache[linea_actual] = (plan_id, part_no, time.time())
                
                # Pausar automáticamente el plan
                if plan_id and part_no:
                    # Pausar automáticamente el plan (OPTIMIZADO - Caché primero)
                    logger.info(f"🔴 Auto-pausa por inactividad: {part_no} en línea {linea_actual} ({tiempo_transcurrido:.0f}s sin escaneo)")
                    
                    # ========== ACTUALIZAR CACHÉ PRIMERO (INSTANTÁNEO) ==========
                    dual_db.actualizar_estado_plan_cache_only(plan_id, "PAUSADO", linea_actual)
                    
                    # Invalidar caché de plan en progreso
                    self._plan_en_progreso_cache.pop(linea_actual, None)
                    
                    # NO REFRESCAR PLAN AQUÍ - El worker lo hará cuando termine de actualizar BD
                    # Esto evita congelamiento por locks de SQLite
                    
                    # Notificación inmediata
                    self._show_success_notification(
                        "Plan Pausado Automáticamente",
                        f"Plan {part_no}",
                        "#ffc107"  # Amarillo/naranja
                    )
                    
                    # ========== BACKGROUND: Sincronizar BD (NO bloquea) ==========
                    worker = EstadoPlanWorker(plan_id, "PAUSADO", part_no, linea_actual)
                    worker.finished.connect(lambda success, msg: self._on_db_sync_finished(success, msg))
                    
                    # Guardar referencia
                    if not hasattr(self, '_estado_workers'):
                        self._estado_workers = []
                    self._estado_workers.append(worker)
                    worker.finished.connect(lambda: self._estado_workers.remove(worker) if worker in self._estado_workers else None)
                    
                    # Iniciar worker
                    worker.start()
                    
                    # Resetear el contador para esta línea
                    self._last_scan_time_per_line[linea_actual] = time.time()
                        
        except Exception as e:
            import logging
            logging.error(f"Error verificando inactividad: {e}")
    
    def _show_success_notification(self, titulo: str, mensaje: str, color: str = "#28a745"):
        """Muestra una notificación de éxito para operaciones exitosas"""
        if not self.duplicate_overlay:
            return
        self.duplicate_overlay.setText(f"{titulo}\n{mensaje}\nACTUALIZADO")
        
        # Ajustar estilo y tamaño según modo pantalla completa
        if self._fullscreen_mode:
            self.duplicate_overlay.resize(800, 300)
            self.duplicate_overlay.setStyleSheet(f"""
                QLabel {{
                    background-color: {color};
                    color: white;
                    font-size: 44px;
                    font-weight: bold;
                    border: 6px solid white;
                    border-radius: 20px;
                    padding: 30px;
                    text-align: center;
                }}
            """)
        else:
            self.duplicate_overlay.resize(600, 200)
            self.duplicate_overlay.setStyleSheet(f"""
                QLabel {{
                    background-color: {color};
                    color: white;
                    font-size: 28px;
                    font-weight: bold;
                    border: 4px solid white;
                    border-radius: 12px;
                    padding: 20px;
                    text-align: center;
                }}
            """)
        
        self._center_overlay(self.duplicate_overlay)
        self.duplicate_overlay.show()
        
        # Auto-ocultar después de 2 segundos
        QtCore.QTimer.singleShot(2000, self.duplicate_overlay.hide)

    def _create_exit_fullscreen_button(self):
        """Crea el botón para salir del modo pantalla completa"""
        self.exit_fullscreen_btn = QtWidgets.QPushButton("X", self)
        self.exit_fullscreen_btn.setStyleSheet("""
            QPushButton {
                background-color: #dc3545;
                color: white;
                font-weight: bold;
                font-size: 18px;
                border: 2px solid #c82333;
                border-radius: 20px;
                padding: 0px;
                min-width: 40px;
                max-width: 40px;
                min-height: 40px;
                max-height: 40px;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
            QPushButton:pressed {
                background-color: #bd2130;
            }
        """)
        self.exit_fullscreen_btn.clicked.connect(self.toggle_fullscreen_mode)
        self.exit_fullscreen_btn.hide()  # Inicialmente oculto
        
        # Posicionar el botón en la esquina superior derecha
        self.exit_fullscreen_btn.move(self.width() - 60, 20)

    def resizeEvent(self, event):
        """Reposicionar overlay cuando se redimensiona la ventana"""
        super().resizeEvent(event)
        if self.duplicate_overlay and self.duplicate_overlay.isVisible():
            self._center_overlay(self.duplicate_overlay)
        if self.ok_overlay and self.ok_overlay.isVisible():
            self._center_overlay(self.ok_overlay)
        
        # Reposicionar botón de salir del modo pantalla completa
        if hasattr(self, 'exit_fullscreen_btn'):
            self.exit_fullscreen_btn.move(self.width() - 60, 20)

    def _center_overlay(self, overlay):
        """Centrar un overlay en la ventana actual"""
        if not overlay:
            return
        parent_rect = self.rect()
        overlay_rect = overlay.rect()
        center_x = (parent_rect.width() - overlay_rect.width()) // 2
        center_y = (parent_rect.height() - overlay_rect.height()) // 2
        overlay.move(center_x, center_y)

    def handle_scan(self) -> None:
        """⚡ ULTRA-RÁPIDO: Procesa escaneo en background sin bloquear UI"""
        raw = self.scan_input.text()
        if not raw:
            return
        
        # ⚡ PROTECCIÓN ANTI-DUPLICADOS: Evitar procesar el mismo código dos veces seguidas
        import time
        current_time = time.time() * 1000  # ms
        if raw == self._last_processed_code and (current_time - self._last_processed_time) < 1000:
            # Mismo código en menos de 1 segundo - ignorar duplicado
            self.scan_input.clear()
            logger.debug(f"🔇 Código duplicado ignorado: {raw[:30]}...")
            return
        
        # Registrar este código como procesado
        self._last_processed_code = raw
        self._last_processed_time = current_time
        
        # ⚡ LIMPIAR INMEDIATAMENTE para que el usuario pueda escanear el siguiente
        self.scan_input.clear()
        
        # Normalizar texto del escáner
        raw = normalize_scanner_text(raw)
        selected_linea = self.linea_selector.currentText()
        
        # ⚡⚡⚡ VALIDACIÓN INSTANTÁNEA Y FEEDBACK VISUAL INMEDIATO ⚡⚡⚡
        # Extraer nparte del código SIN tocar BD (< 1ms)
        validation_result = self._fast_validate_scan(raw, selected_linea)
        
        if validation_result['valid']:
            # ✅ CÓDIGO VÁLIDO - MOSTRAR OK INMEDIATAMENTE
            _play_success_sound()
            self._show_ok_overlay(validation_result['kind'])
            
            # Feedback visual verde
            self.scan_input.setStyleSheet("background-color: #c8e6c9;")  # Verde más intenso
            QtCore.QTimer.singleShot(300, lambda: self.scan_input.setStyleSheet(""))
        else:
            # ❌ ERROR - Mostrar error inmediatamente
            if validation_result.get('play_sound', True):
                _play_error_sound()
            if validation_result.get('message'):
                self._show_plan_notification(
                    validation_result['message'], 
                    raw, 
                    color=validation_result.get('color', '#FF3333')
                )
        
        # ⚡ PROCESAR EN BACKGROUND THREAD (no bloquea UI)
        class ScanWorker(QtCore.QThread):
            finished = QtCore.pyqtSignal(int, str, str)  # result, raw, linea
            
            def __init__(self, raw_code, linea):
                super().__init__()
                self.raw_code = raw_code
                self.linea = linea
            
            def run(self):
                try:
                    result = process_scan_direct(self.raw_code, self.linea)
                    self.finished.emit(result, self.raw_code, self.linea)
                except Exception as e:
                    logger.error(f"Error en ScanWorker: {e}")
                    self.finished.emit(-99, self.raw_code, self.linea)
        
        # Crear y lanzar worker
        worker = ScanWorker(raw, selected_linea)
        worker.finished.connect(self._on_scan_processed)
        
        # Guardar referencia para que no se destruya
        if not hasattr(self, '_scan_workers'):
            self._scan_workers = []
        self._scan_workers.append(worker)
        worker.finished.connect(lambda: self._scan_workers.remove(worker) if worker in self._scan_workers else None)
        
        worker.start()
        
        # ⚡ UI YA ESTÁ LISTA PARA EL SIGUIENTE ESCANEO (no espera a que termine el worker)
    
    def _fast_validate_scan(self, raw: str, linea: str) -> dict:
        """⚡ VALIDACIÓN ULTRA-RÁPIDA (< 1ms) - Solo verifica que el modelo coincida con plan EN PROGRESO"""
        try:
            from ..services.parser import parse_qr_scan, parse_barcode_scan, detect_scan_format
            from ..services.dual_db import get_dual_db
            
            # Detectar formato
            try:
                fmt = detect_scan_format(raw)
                kind = 'QR' if fmt == 'QR' or raw.endswith(';') else 'BARCODE'
            except Exception:
                return {'valid': False, 'message': 'FORMATO INVÁLIDO', 'color': '#FF3333'}
            
            # Parsear para extraer nparte (no toca BD, < 1ms)
            try:
                if kind == 'QR':
                    parsed = parse_qr_scan(raw)
                else:
                    parsed = parse_barcode_scan(raw)
                
                nparte = parsed.nparte
            except Exception as e:
                logger.debug(f"Error parseando: {e}")
                return {'valid': False, 'message': 'ERROR DE FORMATO', 'color': '#FF3333'}
            
            # ⚡ VALIDACIÓN DESDE CACHÉ (0ms - no toca BD)
            dual_db = get_dual_db()
            if hasattr(dual_db, '_plan_cache') and dual_db._plan_cache:
                nparte_escaneado = nparte.strip().upper()
                
                # Buscar plan EN PROGRESO en caché
                plan_en_progreso = None
                for plan in dual_db._plan_cache:
                    if plan.get('line') == linea and plan.get('status') == 'EN PROGRESO':
                        plan_en_progreso = plan
                        break
                
                if plan_en_progreso:
                    # ✅ HAY PLAN EN PROGRESO: Solo comparar con ese plan
                    plan_nparte_activo = plan_en_progreso.get('part_no', '').strip().upper()
                    
                    if nparte_escaneado != plan_nparte_activo:
                        # NO coincide → MODELO DIFERENTE (sin buscar en otros planes)
                        return {
                            'valid': False, 
                            'message': f'❌ MODELO DIFERENTE\nPlan: {plan_nparte_activo}\nEscaneado: {nparte_escaneado}',
                            'color': '#FF3333'
                        }
                    
                    # ✅ Coincide - OK inmediato
                    return {'valid': True, 'kind': kind}
                else:
                    # ⚠️ NO HAY PLAN EN PROGRESO: Verificar si existe en algún plan de la línea
                    codigo_existe_en_plan = False
                    for plan in dual_db._plan_cache:
                        if plan.get('line') == linea:
                            plan_nparte = plan.get('part_no', '').strip().upper()
                            if plan_nparte == nparte_escaneado:
                                codigo_existe_en_plan = True
                                break
                    
                    if codigo_existe_en_plan:
                        # Existe en plan pero ninguno está EN PROGRESO
                        return {'valid': False, 'message': 'NO EN PROGRESO', 'color': '#FF8800'}
                    else:
                        # No existe en ningún plan de la línea
                        return {'valid': False, 'message': f'❌ NO EN PLAN\nEscaneado: {nparte_escaneado}', 'color': '#CC3333'}
            
            # Si no hay caché, asumir válido (el worker validará completo)
            return {'valid': True, 'kind': kind}
            
        except Exception as e:
            logger.debug(f"Error en _fast_validate_scan: {e}")
            # En caso de error, asumir válido para no bloquear
            return {'valid': True, 'kind': 'OK'}
    
    def _on_scan_processed(self, result: int, raw: str, linea: str) -> None:
        """⚡ Callback cuando el worker termina de procesar el escaneo"""
        try:
            if result > 0:
                # ✅ ÉXITO - PAR COMPLETO
                # ⚠️ NO reproducir sonido ni overlay aquí - ya se mostró en handle_scan()
                
                # Actualizar timestamp para timer de inactividad
                import time
                self._last_scan_time_per_line[linea] = time.time()

                
                # ⚡ Actualizar UI en background (no bloquea)
                QtCore.QTimer.singleShot(50, lambda: self._update_ui_after_scan(raw, linea))
                
            elif result == -2:
                # Duplicado - mostrar sin sonido
                self._show_duplicate_notification(raw)
            elif result == -3:
                _play_error_sound()
                self._show_plan_notification("NO EN PLAN", raw, color="#CC3333")
            elif result == -4:
                _play_error_sound()
                self._show_plan_notification("NO EN PROGRESO", raw, color="#FF8800")
            elif result == -5:
                # Guardado, esperando complemento - SIN NOTIFICACIÓN (silencioso)
                pass
            elif result == -6:
                _play_error_sound()
                self._show_plan_notification("INICIA PLAN EN MES", raw, color="#991313")
            elif result == -7:
                _play_error_sound()
                self._show_plan_notification("SUB ASSY: NO MATCH", raw, color="#991313")
            elif result == -8:
                self._show_plan_notification("❌ QR DUPLICADO\nEscanea BARCODE", raw, color="#FF3333")
            elif result == -9:
                self._show_plan_notification("❌ BARCODE DUPLICADO\nEscanea QR", raw, color="#FF3333")
            elif result == -10:
                _play_error_sound()
                self._show_plan_notification("❌ MODELO DIFERENTE", raw, color="#FF3333")
            else:
                _play_error_sound()
                QtWidgets.QMessageBox.warning(self, "Error", "Error al procesar escaneo")
        except Exception as e:
            logger.error(f"Error en _on_scan_processed: {e}")
    
    def _update_ui_after_scan(self, raw: str, linea: str) -> None:
        """⚡ Actualiza UI después de escaneo exitoso (ejecuta en background)"""
        try:
            from ..services.parser import parse_qr
            parsed = parse_qr(raw)
            nparte_escaneado = parsed.nparte if hasattr(parsed, 'nparte') else None
            
            if nparte_escaneado:
                # Actualizar tabla del plan
                try:
                    self._update_single_plan_row(nparte_escaneado, linea)
                except Exception as e:
                    logger.debug(f"Error actualizando tabla: {e}")
                
                # Actualizar cache de métricas
                try:
                    from ..services.metrics_cache import get_metrics_cache
                    from datetime import date
                    
                    metrics_cache = get_metrics_cache()
                    if metrics_cache:
                        fecha_hoy = date.today().isoformat()
                        cached = metrics_cache.get_metrics_from_cache(linea, fecha_hoy)
                        if cached:
                            cached['produccion_real'] += 1
                            if cached['plan_acumulado'] > 0:
                                cached['eficiencia'] = (cached['produccion_real'] / cached['plan_acumulado']) * 100
                            metrics_cache.update_metrics_instant(linea, fecha_hoy, cached)
                            
                            # Actualizar cards si es la línea actual
                            if self.linea_selector.currentText() == linea:
                                self._update_cards_with_metrics(
                                    cached['plan_total'],
                                    cached['plan_acumulado'],
                                    cached['produccion_real'],
                                    cached['eficiencia'],
                                    cached['uph'],
                                    cached['upph']
                                )
                except Exception as e:
                    logger.debug(f"Error actualizando cache: {e}")
        except Exception as e:
            logger.debug(f"Error en _update_ui_after_scan: {e}")

    def _create_duplicate_overlay(self):
        """Crea el overlay de notificación para duplicados"""
        self.duplicate_overlay = QtWidgets.QLabel(self)
        self.duplicate_overlay.setText("ESCANEO DUPLICADO")
        self.duplicate_overlay.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        
        # Estilo prominente con fondo rojo y texto grande
        self.duplicate_overlay.setStyleSheet("""
            QLabel {
                background-color: rgba(220, 20, 20, 0.95);
                color: white;
                font-size: 32px;
                font-weight: bold;
                border: 4px solid #FF0000;
                border-radius: 15px;
                padding: 20px;
                margin: 10px;
            }
        """)
        
        # Ocultar inicialmente y asegurar que esté encima de todo
        self.duplicate_overlay.hide()
        self.duplicate_overlay.setWindowFlags(QtCore.Qt.WindowType.FramelessWindowHint | QtCore.Qt.WindowType.WindowStaysOnTopHint)

    def _create_ok_overlay(self):
        self.ok_overlay = QtWidgets.QLabel(self)
        self.ok_overlay.setText("OK")
        self.ok_overlay.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.ok_overlay.setStyleSheet(
            """
            QLabel { background-color: rgba(20,150,20,0.95); color: white; font-size: 30px; font-weight: bold; border: 4px solid #00cc66; border-radius: 15px; padding: 18px; margin: 10px; }
            """
        )
        self.ok_overlay.resize(360, 140)
        self.ok_overlay.hide()
        # Asegurar que esté encima de todo
        self.ok_overlay.setWindowFlags(QtCore.Qt.WindowType.FramelessWindowHint | QtCore.Qt.WindowType.WindowStaysOnTopHint)
        
    def _show_duplicate_notification(self, scan_code: str):
        """Muestra overlay de duplicado distinguiendo QR vs BARCODE con datos clave."""
        if not self.duplicate_overlay:
            return

        title = "ESCANEO DUPLICADO"
        details = "YA EXISTE EN BASE DE DATOS"
        try:
            fmt = detect_scan_format(scan_code)
            scan_fmt = 'BARCODE' if fmt == 'BARCODE' else 'QR'
            if scan_fmt == 'BARCODE':
                title = 'BARCODE DUPLICADO'
                details = "Este código ya fue escaneado"
            else:
                title = 'QR DUPLICADO'
                details = "Este código ya fue escaneado"
        except Exception:
            pass

        self.duplicate_overlay.setText(
            f"{title}\n{scan_code}\n{details}\nYA EXISTE EN EL SISTEMA"
        )

        # Ajustar tamaño según modo pantalla completa
        if self._fullscreen_mode:
            self.duplicate_overlay.resize(900, 350)
            # Aumentar tamaño de fuente para pantalla completa
            self.duplicate_overlay.setStyleSheet("""
                QLabel {
                    background-color: rgba(220, 20, 20, 0.95);
                    color: white;
                    font-size: 48px;
                    font-weight: bold;
                    border: 6px solid #FF0000;
                    border-radius: 20px;
                    padding: 30px;
                    margin: 15px;
                }
            """)
        else:
            self.duplicate_overlay.resize(660, 230)
            self.duplicate_overlay.setStyleSheet("""
                QLabel {
                    background-color: rgba(220, 20, 20, 0.95);
                    color: white;
                    font-size: 32px;
                    font-weight: bold;
                    border: 4px solid #FF0000;
                    border-radius: 15px;
                    padding: 20px;
                    margin: 10px;
                }
            """)

        # Centrar y mostrar
        self._center_overlay(self.duplicate_overlay)
        self.duplicate_overlay.show()
        self.duplicate_overlay.raise_()

        # ⚡ Mostrar ultra-rápido 200ms para escaneo continuo
        self.duplicate_timer.start(200)

    def _show_plan_notification(self, titulo: str, scan_code: str, color: str = "#004c99"):
        if not self.duplicate_overlay:
            return
        self.duplicate_overlay.setText(f"{titulo}\n{scan_code}\nNO SE ACEPTA")
        
        # Ajustar estilo y tamaño según modo pantalla completa
        if self._fullscreen_mode:
            self.duplicate_overlay.resize(800, 300)
            self.duplicate_overlay.setStyleSheet("""
                QLabel {
                    background-color: #B30000;
                    color: white;
                    font-size: 44px;
                    font-weight: bold;
                    border: 6px solid #FF4D4D;
                    border-radius: 20px;
                    padding: 30px;
                    margin: 15px;
                }
            """)
        else:
            self.duplicate_overlay.resize(600, 210)
            # Forzar siempre rojo intenso independientemente del parámetro recibido
            self.duplicate_overlay.setStyleSheet("""
                QLabel {
                    background-color: #B30000;
                    color: white;
                    font-size: 32px;
                    font-weight: bold;
                    border: 4px solid #FF4D4D;
                    border-radius: 15px;
                    padding: 20px;
                    margin: 10px;
                }
            """)
        
        # Centrar y mostrar
        self._center_overlay(self.duplicate_overlay)
        self.duplicate_overlay.show()
        self.duplicate_overlay.raise_()
        # ⏰ Duración 2 segundos para que sea visible
        self.duplicate_timer.start(2000)

    def _show_wait_pair(self, expected_format: str, scan_code: str):
        """Muestra overlay indicando que falta el formato complementario (QR o BARCODE)."""
        titulo = f"ESPERA EL PAR: ESCANEA {expected_format.upper()}"
        self._show_plan_notification(titulo, scan_code, color="#B30000")
    
    def _toggle_duplicate_blink(self):
        """Parpadeo desactivado (mantenido por compatibilidad)."""
        return
    
    def _hide_duplicate_overlay(self):
        """Oculta el overlay de notificación"""
        if self.duplicate_overlay:
            self.duplicate_overlay.hide()

    def _show_ok_overlay(self, kind: str):
        if not self.ok_overlay:
            return
        kind_txt = (kind or '').strip().upper()
        if kind_txt not in ("QR", "BARCODE"):
            kind_txt = "OK"
        else:
            kind_txt = f"✓ OK {kind_txt}"
        self.ok_overlay.setText(f"{kind_txt}")
        
        # Ajustar tamaño y estilo según modo pantalla completa
        # Estilos simplificados en modo OPTIMIZED para PCs lentas
        is_optimized = os.environ.get('APP_PERFORMANCE_MODE') == 'OPTIMIZED'
        
        if self._fullscreen_mode:
            self.ok_overlay.resize(600, 250)
            if is_optimized:
                self.ok_overlay.setStyleSheet(
                    "QLabel { background-color: rgba(20,180,20,0.9); color: white; font-size: 60px; font-weight: bold; border: 4px solid #00FF66; padding: 20px; }"
                )
            else:
                self.ok_overlay.setStyleSheet(
                    """
                    QLabel { 
                        background-color: rgba(20,180,20,0.95); 
                        color: white; 
                        font-size: 60px; 
                        font-weight: bold; 
                        border: 8px solid #00FF66; 
                        border-radius: 25px; 
                        padding: 40px; 
                        margin: 20px; 
                    }
                    """
                )
        else:
            self.ok_overlay.resize(450, 180)
            if is_optimized:
                self.ok_overlay.setStyleSheet(
                    "QLabel { background-color: rgba(20,180,20,0.9); color: white; font-size: 40px; font-weight: bold; border: 3px solid #00FF66; padding: 15px; }"
                )
            else:
                self.ok_overlay.setStyleSheet(
                    """
                    QLabel { 
                        background-color: rgba(20,180,20,0.95); 
                        color: white; 
                        font-size: 40px; 
                        font-weight: bold; 
                        border: 6px solid #00FF66; 
                        border-radius: 18px; 
                        padding: 30px; 
                        margin: 15px; 
                    }
                    """
                )
        
        # Centrar y mostrar
        self._center_overlay(self.ok_overlay)
        self.ok_overlay.show()
        self.ok_overlay.raise_()
        # ⚡ Ultra-rápido para máxima fluidez
        duration = 100 if getattr(settings, 'SOLO_QR_MODE', False) else 150
        self.ok_timer.start(duration)

    def _hide_ok_overlay(self):
        if self.ok_overlay:
            self.ok_overlay.hide()

    def _ensure_scan_focus(self):
        try:
            if QtWidgets.QApplication.activeModalWidget() is None:
                # En modo pantalla completa, mantener foco más agresivamente
                if self._fullscreen_mode:
                    if self.focusWidget() is not self.scan_input:
                        self.scan_input.setFocus()
                        self.scan_input.raise_()  # Asegurar que esté al frente
                else:
                    if self.focusWidget() is not self.scan_input:
                        self.scan_input.setFocus()
        except Exception:
            pass

    def _is_recent_duplicate(self, raw: str) -> bool:
        """Verifica si el escaneo es un duplicado reciente revisando SQLite local"""
        try:
            from ..services.dual_db import get_dual_db
            from ..services.parser import parse_scan
            
            # Normalizar texto antes de verificar
            raw_normalized = normalize_scanner_text(raw) if raw else raw
            
            # Validar formato básico primero (QR nuevo ñ o antiguo ;)
            if not raw_normalized or not raw_normalized.strip() or not is_complete_qr(raw_normalized.strip()):
                return False
                
            parsed = parse_scan(raw_normalized.strip())
            dual_db = get_dual_db()
            is_duplicate = dual_db._check_duplicate_in_sqlite(parsed)
            
            # Debug logging
            if is_duplicate:
                print(f"DEBUG: Duplicado detectado en UI para: {raw_normalized.strip()}")
            
            return is_duplicate
        except Exception as e:
            print(f"DEBUG: Error verificando duplicado en UI: {e}")
            return False

    def handle_scan_live(self, txt: str) -> None:
        if self._processing_scan:
            return
        raw = (txt or "").strip()
        if not raw:
            self._barcode_timer.stop()
            self._pending_barcode_processed = False
            self._last_char_time = 0.0
            self._interchar_times.clear()
            return
            
        # Normalizar texto del escáner para compatibilidad con distribuciones de teclado
        raw = normalize_scanner_text(raw)
        
        # EVITAR DUPLICADOS - verificar si ya procesamos este código
        import time
        current_time = time.time() * 1000  # ms
        if raw == self._last_processed_code and (current_time - self._last_processed_time) < 500:
            # Mismo código en menos de 500ms - ignorar duplicado
            self.scan_input.clear()
            return
        
        # ⚡ NO limpiar aquí - dejar que el escáner termine de escribir
        # La limpieza se hará en _process_complete_qr o _process_pending_barcode
        
        # Considerar QR completo cuando cumple patrón (nuevo ñ o antiguo ;)
        if not is_complete_qr(raw):
            # Posible BARCODE (no lleva ';') -> usar heurística y debounce
            # Condición: longitud mínima 13, últimos 12 dígitos, todo alfanumérico
            if len(raw) >= 8 and raw.replace('_','').replace('-','').isalnum():
                import time, statistics
                now_ms = time.perf_counter() * 1000.0
                if self._last_char_time:
                    dt = now_ms - self._last_char_time
                    # Filtrar intervalos absurdos (>500ms) para no contaminar
                    if 0 < dt < 500:
                        self._interchar_times.append(dt)
                        if len(self._interchar_times) > 10:
                            self._interchar_times.pop(0)
                self._last_char_time = now_ms
                gap = self._barcode_debounce_ms
                if len(self._interchar_times) >= 3:
                    try:
                        median_dt = statistics.median(self._interchar_times)
                        if median_dt < self._barcode_scanner_threshold_ms:
                            gap = self._barcode_end_gap_fast
                        else:
                            gap = self._barcode_end_gap_slow
                    except Exception:
                        pass
                # Heurística adicional: si los últimos 12 son dígitos y longitud >=13 forzar gap rápido
                if len(raw) >= 13 and raw[-12:].isdigit():
                    gap = min(gap, self._barcode_end_gap_fast)
                self._barcode_timer.start(int(gap))
            else:
                self._barcode_timer.stop()
            self._last_scan_len = len(raw)
            return
        
        # Detectado QR con 4 secciones ';' - es QR en progreso
        # ⚡ NO guardar ni limpiar aquí - esperar a que termine completamente
        # El timer procesará cuando el escáner termine de escribir
        self._qr_complete_timer.start(self._qr_complete_delay_ms)
        return

    def _process_complete_qr(self):
        """Procesa QR completo después de esperar posibles líneas adicionales"""
        if self._processing_scan:
            return
        
        # 🔒 Bloquear actualizaciones durante el escaneo
        self._scan_in_progress = True
        
        # ⚡ Leer del input AHORA (el escáner ya terminó de escribir)
        raw = (self.scan_input.text() or '').strip()
        if not raw:
            self._scan_in_progress = False  # Desbloquear
            return
        
        # Normalizar texto
        raw = normalize_scanner_text(raw)
        
        # EVITAR DUPLICADOS
        import time
        current_time = time.time() * 1000
        if raw == self._last_processed_code and (current_time - self._last_processed_time) < 500:
            self.scan_input.clear()  # Limpiar duplicado
            return
        
        # ⚡ LIMPIAR INPUT AHORA (después de leer)
        self.scan_input.clear()
        
        # Marcar como procesado
        self._last_processed_code = raw
        self._last_processed_time = time.time() * 1000
        
        # Procesar con sistema optimizado
        self._processing_scan = True
        try:
            selected_linea = self.linea_selector.currentText()
            result = process_scan_direct(raw, selected_linea)
            
            if result > 0:
                # ✅ Reproducir sonido de éxito
                _play_success_sound()
                
                # ⚡ OPTIMIZACIÓN MÁXIMA: NO actualizar plan ni totales aquí
                # Solo mostrar overlay y actualizar contador en memoria (instantáneo)
                self._show_ok_overlay("QR")
                
                # Incrementar contador en memoria (instantáneo, no toca DB)
                if not hasattr(self, '_scan_counter'):
                    self._scan_counter = 0
                self._scan_counter += 1
                
                # Actualizar timestamp para auto-pausa
                import time
                self._last_scan_time_per_line[selected_linea] = time.time()
                
                # Actualizar status con contador (no toca DB)
                self.update_status_fast()
                
                # ❌ ELIMINADO refresh_plan_only y refresh_totals_only (causaban congelamiento)
                # ✅ El timer de 15 SEGUNDOS se encarga de actualizar automáticamente (optimizado con Dual DB)
            elif result == -2:
                # Duplicado detectado - NO reproducir sonido
                if raw == self._last_processed_code:
                    logger.debug(f"🔇 Código duplicado consecutivo ignorado (async): {raw[:20]}...")
                    return
                self._show_duplicate_notification(raw)
            elif result == -3:
                _play_error_sound()
                self._show_plan_notification("NO EN PLAN", raw, color="#CC3333")
            elif result == -4:
                _play_error_sound()
                self._show_plan_notification("NO EN PROGRESO", raw, color="#FF8800")
            elif result == -5:
                try:
                    fmt = detect_scan_format(raw)
                    expected = 'QR' if fmt == 'BARCODE' else 'BARCODE'
                    self._show_wait_pair(expected, raw)
                except Exception:
                    self._show_plan_notification("ESPERA EL PAR", raw, color="#991313")
            elif result == -6:
                _play_error_sound()
                self._show_plan_notification("INICIA PLAN EN MES", raw, color="#991313")
            elif result == -7:
                _play_error_sound()
                self._show_plan_notification("SUB ASSY: NO MATCH", raw, color="#991313")
            elif result == -8:
                # QR duplicado - verificar si es consecutivo
                if raw == self._last_processed_code:
                    logger.debug(f"🔇 QR duplicado consecutivo ignorado: {raw[:20]}...")
                    return
                # No es consecutivo - mostrar sin sonido
                self._show_plan_notification("❌ QR DUPLICADO\nEscanea BARCODE", raw, color="#FF3333")
            elif result == -9:
                # BARCODE duplicado - verificar si es consecutivo
                if raw == self._last_processed_code:
                    logger.debug(f"🔇 BARCODE duplicado consecutivo ignorado: {raw[:20]}...")
                    return
                # No es consecutivo - mostrar sin sonido
                self._show_plan_notification("❌ BARCODE DUPLICADO\nEscanea QR", raw, color="#FF3333")
            elif result == -10:
                # Modelo diferente al plan EN PROGRESO
                _play_error_sound()
                self._show_plan_notification("❌ MODELO DIFERENTE\nAL PLAN EN PROGRESO", raw, color="#FF3333")
            else:
                # Error desconocido
                if result < 0:
                    _play_error_sound()
                    # Mostrar código de error para debug
                    error_msgs = {
                        -1: "ERROR: Formato inválido",
                        -2: "DUPLICADO",
                        -3: "NO ESTÁ EN EL PLAN",
                        -4: "PLAN NO EN PROGRESO",
                    }
                    msg = error_msgs.get(result, f"ERROR PROCESANDO ({result})")
                    self._show_plan_notification(msg, raw, color="#FF3333")
        except Exception:
            pass
        finally:
            self._processing_scan = False
            self._scan_in_progress = False  # 🔓 Desbloquear actualizaciones
    
    def _process_pending_barcode(self):
        if self._processing_scan:
            return
        if self._pending_barcode_processed:
            return
            
        # 🔒 Bloquear actualizaciones durante el escaneo
        self._scan_in_progress = True
        
        raw = (self.scan_input.text() or '').strip()
        if not raw or is_complete_qr(raw):
            self._scan_in_progress = False  # Desbloquear
            return  # Ya lo manejará el flujo normal (QR completo o vacío)
        
        # EVITAR DUPLICADOS - verificar si ya procesamos este código
        import time
        current_time = time.time() * 1000
        if raw == self._last_processed_code and (current_time - self._last_processed_time) < 500:
            self.scan_input.clear()
            self._scan_in_progress = False  # Desbloquear
            return
        
        # Validar nuevamente heurística
        if len(raw) >= 13 and raw[-12:].isdigit() and raw.replace('_','').replace('-','').isalnum():
            # LIMPIAR PRIMERO
            self.scan_input.clear()
            
            # Marcar como procesado
            self._last_processed_code = raw
            self._last_processed_time = current_time
            
            self._processing_scan = True
            try:
                selected_linea = self.linea_selector.currentText()
                result = process_scan_direct(raw, selected_linea)
                if result > 0:
                    # ✅ Reproducir sonido de éxito
                    _play_success_sound()
                    
                    self._pending_barcode_processed = True
                    
                    # ⚡ OPTIMIZACIÓN MÁXIMA: NO actualizar plan ni totales aquí
                    # Solo mostrar overlay y actualizar contador en memoria (instantáneo)
                    self._show_ok_overlay("BARCODE")
                    
                    # Incrementar contador en memoria (instantáneo, no toca DB)
                    if not hasattr(self, '_scan_counter'):
                        self._scan_counter = 0
                    self._scan_counter += 1
                    
                    # Actualizar timestamp para auto-pausa
                    self._last_scan_time_per_line[selected_linea] = time.time()
                    
                    # Actualizar status con contador (no toca DB)
                    self.update_status_fast()
                    
                    # ❌ ELIMINADO refresh_plan_only y refresh_totals_only (causaban congelamiento)
                    # ✅ El timer de 15 SEGUNDOS se encarga de actualizar automáticamente (optimizado con Dual DB)
                    
                    # Reset métricas de escritura tras procesar
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -2:
                    # Duplicado detectado - NO reproducir sonido
                    if raw == self._last_processed_code:
                        logger.debug(f"🔇 Código duplicado consecutivo ignorado (pending): {raw[:20]}...")
                        self._last_char_time = 0.0
                        self._interchar_times.clear()
                        return
                    self._show_duplicate_notification(raw)
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -3:
                    _play_error_sound()
                    self._show_plan_notification("FUERA DE PLAN", raw, color="#991313")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -4:
                    _play_error_sound()
                    self._show_plan_notification("PLAN COMPLETO", raw, color="#0066aa")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -5:
                    # Avisar formato complementario requerido
                    try:
                        fmt = detect_scan_format(raw)
                        expected = 'QR' if fmt == 'BARCODE' else 'BARCODE'
                        self._show_wait_pair(expected, raw)
                    except Exception:
                        self._show_plan_notification("ESPERA EL PAR", raw, color="#991313")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -6:
                    _play_error_sound()
                    self._show_plan_notification("INICIA PLAN EN MES", raw, color="#991313")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -7:
                    _play_error_sound()
                    self._show_plan_notification("SUB ASSY: NO MATCH", raw, color="#991313")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -8:
                    # QR duplicado - verificar si es consecutivo
                    if raw == self._last_processed_code:
                        logger.debug(f"🔇 QR duplicado consecutivo ignorado: {raw[:20]}...")
                        return
                    # No es consecutivo - mostrar sin sonido
                    self._show_plan_notification("❌ QR DUPLICADO\nEscanea BARCODE", raw, color="#FF3333")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -9:
                    # BARCODE duplicado - verificar si es consecutivo
                    if raw == self._last_processed_code:
                        logger.debug(f"🔇 BARCODE duplicado consecutivo ignorado: {raw[:20]}...")
                        return
                    # No es consecutivo - mostrar sin sonido
                    self._show_plan_notification("❌ BARCODE DUPLICADO\nEscanea QR", raw, color="#FF3333")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -10:
                    # Modelo diferente al plan EN PROGRESO
                    _play_error_sound()
                    self._show_plan_notification("❌ MODELO DIFERENTE", raw, color="#FF3333")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                else:
                    # Error desconocido
                    if result < 0:
                        _play_error_sound()
                        self._show_plan_notification("ERROR PROCESANDO", raw, color="#FF3333")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
            except Exception:
                pass
            finally:
                self._processing_scan = False
                self._scan_in_progress = False  # 🔓 Desbloquear actualizaciones
                if not self.scan_input.text():
                    self._pending_barcode_processed = False

    def refresh_tables(self) -> None:
        # Reinicializar tabla del plan si es necesario (cambio de modo SUB ASSY)
        self._reinit_plan_table()
        
        # Cargar datos del plan después de reinicializar
        self.refresh_plan_only()
        
        # Últimos escaneos desde SQLite local (ultra-rápido)
        scans = get_last_scans(100)
        self.table_scans.setRowCount(0)
        for s in scans:
            row = self.table_scans.rowCount()
            self.table_scans.insertRow(row)
            vals = [s.id, s.ts, s.raw, s.lote, s.secuencia, s.estacion, s.nparte, s.modelo or "", s.cantidad, s.linea]
            for col, v in enumerate(vals):
                self.table_scans.setItem(row, col, QtWidgets.QTableWidgetItem(str(v)))

        # Totales del día desde SQLite local (ultra-rápido)
        totals = get_today_totals()
        self.table_totals.setRowCount(0)
        for t in totals:
            row = self.table_totals.rowCount()
            self.table_totals.insertRow(row)
            vals = [t.fecha, t.linea, t.nparte or "", t.modelo or "", t.cantidad_total, t.uph_target if t.uph_target is not None else "", t.uph_real if t.uph_real is not None else 0]
            for col, v in enumerate(vals):
                self.table_totals.setItem(row, col, QtWidgets.QTableWidgetItem(str(v)))

        # Save total uph_real for status
        self._last_total_uph_real = sum(int(t.uph_real or 0) for t in totals)
        self.update_status()

    def update_status(self) -> None:
        qsize = self.db.queue_size()
        uph = getattr(self, "_last_total_uph_real", 0)
        self.status.showMessage(f"MySQL conectado | UPH real última hora: {uph} | Cola offline: {qsize}")

    def update_status_only(self) -> None:
        """Actualización rápida solo del status bar sin consultas pesadas"""
        try:
            qsize = self.db.queue_size()
            self.status.showMessage(f"MySQL conectado | Cola offline: {qsize}")
        except Exception as e:
            self.status.showMessage(f"MySQL error: {str(e)[:50]}...")
    
    def _update_ui_throttled(self) -> None:
        """Actualización de UI controlada para evitar sobrecarga durante escaneo constante"""
        import time
        current_time = time.time()
        if current_time - self._last_ui_update < self._ui_update_interval:
            return
        
        self._last_ui_update = current_time
        # Refrescar también plan para ver producido y uph real sin esperar al timer
        self.refresh_totals_only()
        self.refresh_plan_only()
        self.update_status_fast()
    
    def _update_tables_and_status(self) -> None:
        """Actualizar TODO en BACKGROUND thread (sin congelar UI)"""
        # ✅ VERIFICAR CAMBIO DE FECHA (medianoche)
        from datetime import date
        today = date.today()
        if hasattr(self, '_current_date') and self._current_date != today:
            logger.warning(f"CAMBIO DE FECHA DETECTADO: {self._current_date} → {today}")
            self._current_date = today
            
            # Forzar recarga completa del plan para el nuevo día
            QtCore.QTimer.singleShot(500, self._force_reload_plan_for_new_day)
            return  # Salir, la recarga se hará en el callback
        
        # ✅ VERIFICAR CAMBIOS EN EL PLAN DESDE MYSQL (cada 15s el sync worker descarga nuevos datos)
        from ..services.dual_db import get_dual_db
        dual_db = get_dual_db()
        if dual_db.check_plan_changed_and_reset():
            logger.warning("📊 PLAN CAMBIÓ EN MYSQL - Recargando tabla automáticamente...")
            QtCore.QTimer.singleShot(100, self._force_reload_plan_table)
            # Mostrar notificación visual temporal
            if hasattr(self, 'fecha_plan_label'):
                original_style = self.fecha_plan_label.styleSheet()
                self.fecha_plan_label.setStyleSheet("""
                    QLabel {
                        background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                            stop:0 #27ae60, stop:0.5 #2ecc71, stop:1 #27ae60);
                        color: white;
                        font-size: 14px;
                        font-weight: bold;
                        border: 2px solid #2ecc71;
                        border-radius: 6px;
                        padding: 8px 16px;
                        margin: 4px 0px;
                    }
                """)
                # Restaurar estilo después de 2 segundos
                QtCore.QTimer.singleShot(2000, lambda: self.fecha_plan_label.setStyleSheet(original_style))
        
        # Si hay un escaneo en progreso, saltar actualización
        if getattr(self, '_scan_in_progress', False):
            return
        
        # Si ya hay un worker corriendo, saltar
        if hasattr(self, '_update_worker') and self._update_worker and self._update_worker.isRunning():
            return
        
        # ✅ Worker thread para leer datos en background
        class UpdateWorker(QtCore.QThread):
            data_ready = QtCore.pyqtSignal(dict)
            
            def __init__(self, linea: str, parent=None):
                super().__init__(parent)
                self._linea = linea
            
            def run(self):
                try:
                    from ..services.dual_db import get_dual_db
                    dual_db = get_dual_db()
                    
                    # Leer TODOS los datos de SQLite en background
                    with dual_db._get_sqlite_connection(timeout=1.0) as conn:
                        cursor = conn.cursor()
                        
                        # 1. Métricas de cards
                        cursor.execute("""
                            SELECT SUM(plan_count), SUM(produced_count)
                            FROM plan_local
                            WHERE line = ?
                        """, (self._linea,))
                        result = cursor.fetchone()
                        plan_total = result[0] or 0
                        produccion_total = result[1] or 0
                        
                        # 2. Plan rows para tabla
                        plan_rows = dual_db.get_plan_for_line_local(self._linea)
                    
                    # Calcular eficiencia
                    eficiencia = (produccion_total / plan_total * 100) if plan_total > 0 else 0.0
                    
                    # Emitir datos para actualizar UI
                    self.data_ready.emit({
                        'plan_total': plan_total,
                        'produccion_total': produccion_total,
                        'eficiencia': eficiencia,
                        'plan_rows': plan_rows,
                        'linea': self._linea
                    })
                    
                except Exception as e:
                    logger.error(f"Error en UpdateWorker: {e}")
                    self.data_ready.emit({})  # Emitir dict vacío en caso de error
        
        # Crear y ejecutar worker
        linea_actual = self.linea_selector.currentText()
        self._update_worker = UpdateWorker(linea_actual, self)
        self._update_worker.data_ready.connect(self._on_update_data_ready)
        self._update_worker.start()
    
    def _on_update_data_ready(self, data: dict):
        """Actualiza UI con datos preparados en background (rápido, sin bloqueos)"""
        try:
            if not data:
                return
            
            # Verificar que no cambió la línea mientras se cargaban datos
            if data.get('linea') != self.linea_selector.currentText():
                return
            
            # ✅ ACTUALIZAR CARDS (instantáneo)
            self._update_cards_with_metrics(
                plan=data['plan_total'],
                plan_acum=data['plan_total'],
                produccion=data['produccion_total'],
                eficiencia=data['eficiencia'],
                uph=0,
                upph=0.0
            )
            
            # ✅ ACTUALIZAR TABLA DE PLAN (si hay datos)
            if data.get('plan_rows'):
                QtCore.QTimer.singleShot(10, lambda: self._render_plan_table_fast(data['plan_rows']))
            
            # ✅ Actualizar status bar
            self.update_status()
            
            # ✅ SINCRONIZAR VENTANA UPH (si está abierta)
            self._sync_metrics_to_widget()
            
            logger.debug(f"✅ Cards actualizadas: Plan={data['plan_total']}, Prod={data['produccion_total']}, Efic={data['eficiencia']:.1f}%")
            
        except Exception as e:
            logger.warning(f"⚠️ Error actualizando UI: {e}")
    
    def _render_plan_table_fast(self, plan_rows: list):
        """Renderiza tabla de plan con datos YA preparados (sin consultas adicionales)"""
        try:
            # ✅ DETECTAR CAMBIOS EN SECUENCIA: Si el orden cambió, re-renderizar completo
            if self.table_plan.rowCount() == len(plan_rows):
                sequence_changed = False
                for row_idx in range(self.table_plan.rowCount()):
                    part_no_item = self.table_plan.item(row_idx, 0)
                    if part_no_item:
                        plan_id_stored = part_no_item.data(QtCore.Qt.ItemDataRole.UserRole)
                        # El plan en esta posición debería ser el mismo si no cambió el orden
                        if row_idx < len(plan_rows):
                            expected_id = plan_rows[row_idx].get('id')
                            if plan_id_stored != expected_id:
                                sequence_changed = True
                                break
                
                if sequence_changed:
                    self._force_reload_plan_table()
                    return
            
            # Actualizar valores de producción en las celdas existentes
            for row_idx in range(self.table_plan.rowCount()):
                part_no_item = self.table_plan.item(row_idx, 0)
                if not part_no_item:
                    continue
                
                # Obtener plan_id almacenado en la celda (identificador único)
                plan_id_stored = part_no_item.data(QtCore.Qt.ItemDataRole.UserRole)
                
                if plan_id_stored is None:
                    continue
                
                # Buscar el plan correspondiente por ID único (no por part_no)
                for plan in plan_rows:
                    if plan.get('id') == plan_id_stored:
                        # Actualizar columna de "Plan" (plan_count)
                        plan_count = plan.get('plan_count', 0)
                        col_plan = 4 if self.table_plan.columnCount() == 10 else 3
                        plan_item = self.table_plan.item(row_idx, col_plan)
                        if plan_item:
                            plan_item.setText(str(plan_count))
                        
                        # Actualizar columna de "Producido"
                        prod_count = plan.get('produced_count', 0)
                        col_producido = 5 if self.table_plan.columnCount() == 10 else 4
                        prod_item = self.table_plan.item(row_idx, col_producido)
                        if prod_item:
                            prod_item.setText(str(prod_count))
                        
                        # Actualizar Lote (columna 1) - Campo correcto: lot_no
                        lote_nuevo = plan.get('lot_no', '')
                        lote_item = self.table_plan.item(row_idx, 1)
                        if lote_item:
                            lote_item.setText(lote_nuevo)
                        
                        # Actualizar Modelo (columna 2) - Campo correcto: model_code
                        modelo_nuevo = plan.get('model_code', '')
                        modelo_item = self.table_plan.item(row_idx, 2)
                        if modelo_item:
                            modelo_item.setText(modelo_nuevo)
                        
                        # Actualizar % Avance (columna 6 o 5)
                        col_avance = 6 if self.table_plan.columnCount() == 10 else 5
                        avance_item = self.table_plan.item(row_idx, col_avance)
                        if avance_item and plan_count > 0:
                            porcentaje = int((prod_count / plan_count) * 100)
                            avance_item.setText(f"{porcentaje}%")
                        
                        # Actualizar UPH Target (columna 7 o 6) - Campo correcto: uph
                        col_uph = 7 if self.table_plan.columnCount() == 10 else 6
                        uph_item = self.table_plan.item(row_idx, col_uph)
                        if uph_item:
                            uph_target = plan.get('uph', 0)
                            uph_item.setText(str(uph_target))
                        
                        # Actualizar también el estado si cambió
                        estado_nuevo = plan.get('status', '')
                        col_estado = 8 if self.table_plan.columnCount() == 10 else 7
                        estado_item = self.table_plan.item(row_idx, col_estado)
                        if estado_item:
                            estado_item.setText(estado_nuevo)
                        
                        # Actualizar botón TERMINAR según el nuevo estado
                        col_acciones = 9 if self.table_plan.columnCount() == 10 else 8
                        estado_upper = estado_nuevo.upper()
                        
                        # Verificar si debe tener botón TERMINAR
                        debe_tener_boton = 'PROGRESO' in estado_upper or 'PAUSADO' in estado_upper
                        tiene_boton = self.table_plan.cellWidget(row_idx, col_acciones) is not None
                        
                        if debe_tener_boton and not tiene_boton:
                            # Agregar botón TERMINAR
                            btn_terminar = QtWidgets.QPushButton("TERMINAR")
                            btn_terminar.setStyleSheet("""
                                QPushButton {
                                    background-color: #dc3545;
                                    color: white;
                                    border: none;
                                    border-radius: 4px;
                                    padding: 4px 8px;
                                    font-size: 10px;
                                    font-weight: bold;
                                }
                                QPushButton:hover {
                                    background-color: #c82333;
                                }
                                QPushButton:pressed {
                                    background-color: #a71d2a;
                                }
                            """)
                            btn_terminar.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
                            
                            # Obtener datos del plan para el callback
                            linea_actual = self.linea_selector.currentText()
                            part_no = part_no_item.text()
                            btn_terminar.clicked.connect(
                                lambda checked, pid=plan_id_stored, pn=part_no, ln=linea_actual: self._terminar_plan(pid, pn, ln)
                            )
                            self.table_plan.setCellWidget(row_idx, col_acciones, btn_terminar)
                        elif not debe_tener_boton and tiene_boton:
                            # Remover botón TERMINAR
                            self.table_plan.setCellWidget(row_idx, col_acciones, None)
                        
                        break
            
            # Forzar refresco visual de la tabla completa
            self.table_plan.viewport().repaint()
            
            logger.debug(f"✅ Tabla de plan actualizada (solo valores producidos y estado)")
            
        except Exception as e:
            logger.debug(f"Error renderizando tabla rápida: {e}")
    
    def _force_reload_plan_table(self) -> None:
        """Fuerza recarga completa de la tabla de plan (usado cuando cambia secuencia/orden)"""
        try:
            linea = self.linea_selector.currentText()
            if not linea:
                return
            
            # Leer plan actualizado desde SQLite local (ya sincronizado)
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            plan_rows = dual_db.get_plan_for_line_local(linea)
            
            # Re-renderizar tabla completa con nuevo orden
            self._on_plan_data_ready(linea, plan_rows, {})
            
        except Exception as e:
            logger.error(f"Error forzando recarga de tabla: {e}")
    
    def _force_reload_plan_for_new_day(self) -> None:
        """Fuerza recarga completa del plan cuando detecta cambio de fecha (medianoche)"""
        try:
            logger.info("📅 Recargando plan para nuevo día...")
            
            # ✅ ACTUALIZAR INDICADOR VISUAL DE FECHA
            from datetime import date
            fecha_hoy = date.today().strftime("%d/%m/%Y")
            if hasattr(self, 'fecha_plan_label'):
                self.fecha_plan_label.setText(f"PLAN DEL DÍA: {fecha_hoy}")
                # Animación visual: cambiar color temporalmente para llamar la atención
                self.fecha_plan_label.setStyleSheet("""
                    QLabel {
                        background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                            stop:0 #27ae60, stop:0.5 #2ecc71, stop:1 #27ae60);
                        color: white;
                        font-size: 14px;
                        font-weight: bold;
                        border: 2px solid #27ae60;
                        border-radius: 6px;
                        padding: 8px 16px;
                        margin: 4px 0px;
                    }
                """)
                # Volver al color normal después de 3 segundos
                QtCore.QTimer.singleShot(3000, lambda: self.fecha_plan_label.setStyleSheet("""
                    QLabel {
                        background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                            stop:0 #2c3e50, stop:0.5 #34495e, stop:1 #2c3e50);
                        color: #3498db;
                        font-size: 14px;
                        font-weight: bold;
                        border: 2px solid #3498db;
                        border-radius: 6px;
                        padding: 8px 16px;
                        margin: 4px 0px;
                    }
                """))
            
            # Forzar sincronización desde MySQL para obtener plan del día actual
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            # Trigger manual de sincronización de plan
            if hasattr(dual_db, '_sync_worker') and dual_db._sync_worker:
                # Forzar sync inmediato
                dual_db._sync_worker._sync_plan_from_mysql()
                logger.info("✅ Sync de plan forzado para nuevo día")
            
            # Esperar un momento para que se complete la sincronización
            QtCore.QTimer.singleShot(1000, lambda: self._force_reload_plan_table())
            
            # Mostrar notificación al usuario
            if hasattr(self, 'status'):
                self.status.showMessage("Plan actualizado para nuevo día", 5000)
            
        except Exception as e:
            logger.error(f"❌ Error recargando plan para nuevo día: {e}")
    
    def _force_refresh(self) -> None:
        """Forzar actualización manual de todas las tablas y métricas
        
        Este método se llama cuando el usuario presiona el botón 'Actualizar'.
        Realiza una actualización completa inmediata sin esperar el timer automático.
        """
        try:
            # Deshabilitar botón temporalmente para evitar clicks múltiples
            if hasattr(self, 'refresh_button'):
                self.refresh_button.setEnabled(False)
                self.refresh_button.setText("Actualizando...")
            
            # Limpiar cache de UPH para forzar recalcular
            if hasattr(self, '_uph_cache'):
                self._uph_cache.clear()
                self._uph_cache_time.clear()
            
            # Actualizar todas las tablas y métricas
            self._update_tables_and_status()
            
            # Mensaje de confirmación
            if hasattr(self, 'status'):
                self.status.showMessage("Actualización completada", 3000)
            
            # Log de la acción
            import logging
            logger = logging.getLogger(__name__)
            logger.info("Actualización manual forzada por el usuario")
            
        except Exception as e:
            # Manejo de errores
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"❌ Error en actualización manual: {e}")
            
            if hasattr(self, 'status'):
                self.status.showMessage(f"❌ Error al actualizar: {str(e)[:50]}", 5000)
        
        finally:
            # Re-habilitar botón después de 2 segundos
            if hasattr(self, 'refresh_button'):
                QtCore.QTimer.singleShot(2000, lambda: self._re_enable_refresh_button())
    
    def _re_enable_refresh_button(self):
        """Re-habilitar el botón de actualizar"""
        if hasattr(self, 'refresh_button'):
            self.refresh_button.setEnabled(True)
            self.refresh_button.setText("Actualizar")
    
    def _toggle_metrics_widget(self):
        """Abrir/cerrar ventana flotante de métricas"""
        try:
            if self.metrics_widget is None or not self.metrics_widget.isVisible():
                # Crear o mostrar la ventana
                if self.metrics_widget is None:
                    self.metrics_widget = MetricsWidget(self)
                    logger.info("🪟 Creada ventana flotante de métricas")
                
                # Mostrar la ventana primero
                self.metrics_widget.show()
                self.metrics_widget.raise_()
                self.metrics_widget.activateWindow()
                
                # Actualizar con los valores actuales DESPUÉS de mostrarla
                self._sync_metrics_to_widget()
                
                # Actualizar texto del botón
                self.float_metrics_button.setText("CERRAR UPHS")
                self.float_metrics_button.setStyleSheet("""
                    QPushButton {
                        background-color: #27AE60;
                        color: white;
                        border: none;
                        border-radius: 4px;
                        padding: 6px 12px;
                        font-size: 11px;
                        font-weight: bold;
                    }
                    QPushButton:hover {
                        background-color: #229954;
                    }
                    QPushButton:pressed {
                        background-color: #1E8449;
                    }
                """)
                
                logger.info("🪟 Ventana flotante de métricas mostrada")
            else:
                # Cerrar la ventana
                self.metrics_widget.close()
                self.metrics_widget = None
                
                # Restaurar texto del botón
                self.float_metrics_button.setText("UPHS")
                self.float_metrics_button.setStyleSheet("""
                    QPushButton {
                        background-color: #9B59B6;
                        color: white;
                        border: none;
                        border-radius: 4px;
                        padding: 6px 12px;
                        font-size: 11px;
                        font-weight: bold;
                    }
                    QPushButton:hover {
                        background-color: #8E44AD;
                    }
                    QPushButton:pressed {
                        background-color: #7D3C98;
                    }
                """)
                
                logger.info("🪟 Ventana flotante de métricas cerrada")
        
        except Exception as e:
            logger.error(f"❌ Error al toggle ventana flotante: {e}")
            import traceback
            traceback.print_exc()
    
    def _sync_metrics_to_widget(self):
        """Sincronizar métricas actuales con la ventana flotante"""
        if self.metrics_widget is None or not self.metrics_widget.isVisible():
            return
        
        try:
            # Obtener línea seleccionada
            linea_seleccionada = self.linea_selector.currentText()
            
            # Crear diccionario de métricas desde los value_label de las cards
            metrics = {
                'plan': self.card_plan.value_label.text() if hasattr(self.card_plan, 'value_label') else '0',
                'plan_acum': self.card_resultado.value_label.text() if hasattr(self.card_resultado, 'value_label') else '0',
                'produccion': self.card_produccion.value_label.text() if hasattr(self.card_produccion, 'value_label') else '0',
                'eficiencia': self.card_eficiencia.value_label.text() if hasattr(self.card_eficiencia, 'value_label') else '0%',
                'uph': self.card_uph.value_label.text() if hasattr(self.card_uph, 'value_label') else '0',
                'upph': self.card_uphu.value_label.text() if hasattr(self.card_uphu, 'value_label') else '0.00'
            }
            
            # Actualizar ventana flotante
            self.metrics_widget.update_metrics(metrics, linea_seleccionada)
            
        except Exception as e:
            logger.error(f"❌ Error al sincronizar métricas: {e}")
    
    def update_status_fast(self) -> None:
        """Actualización ultra-rápida del status SIN tocar DB (solo memoria)"""
        try:
            # Obtener contador en memoria (instantáneo)
            scan_count = getattr(self, '_scan_counter', 0)
            
            # Obtener línea actual
            selected_linea = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else "N/A"
            
            # Actualizar información del sistema directo
            direct_mysql = get_direct_mysql()
            
            # Status rápido sin bloquear (no intenta conectar si está en ventana offline)
            if direct_mysql and getattr(direct_mysql, 'is_quick_online', None) and direct_mysql.is_quick_online():
                sync_status = "Conectado"
                status_color = "#00aa00"
                # Mostrar contador en memoria (instantáneo, no DB)
                self.status.showMessage(f"MySQL Directo | {sync_status} | Línea: {selected_linea} | Scans: {scan_count} (memoria)")
            else:
                sync_status = "Desconectado"
                status_color = "#aa0000"
                self.status.showMessage(f"Sistema | {sync_status} | Línea: {selected_linea} | Scans: {scan_count} (memoria)")
            
            # Actualizar indicador compacto
            if hasattr(self, 'status_dual'):
                self.status_dual.setText(f"MySQL {sync_status}")
                self.status_dual.setStyleSheet(f"color: {status_color}; font-size: 11px; font-weight: bold;")
                
        except Exception as e:
            self.status.showMessage(f"Error sistema: {str(e)[:40]}...")
            if hasattr(self, 'status_dual'):
                self.status_dual.setText("Sistema Error")
                self.status_dual.setStyleSheet("color: #aa0000; font-size: 11px;")

    def force_table_refresh(self):
        """Fuerza el refresh completo de las tablas tras cambio de configuración"""
        try:
            # Usar logging básico si el personalizado falla
            try:
                from ..logging_config import get_logger
                logger = get_logger(__name__)
                logger.info("Forzando refresh de tablas tras cambio de configuración")
            except (ImportError, Exception):
                print("Forzando refresh de tablas tras cambio de configuración")
            
            # Leer valores actualizados directamente del .env y actualizar settings
            from pathlib import Path
            
            env_path = Path(__file__).parent.parent.parent / '.env'
            
            if env_path.exists():
                with open(env_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('SUB_ASSY_MODE='):
                            settings.SUB_ASSY_MODE = line.split('=', 1)[1].strip() == '1'
                        elif line.startswith('APP_MODE='):
                            settings.APP_MODE = line.split('=', 1)[1].strip()
            
            # Reinicializar tabla del plan completamente
            self._reinit_plan_table()
            
            # Recargar datos del plan (forzar para bypass rate limiting)
            self.refresh_plan_only(force=True)
            
        except Exception as e:
            print(f"Error en force_table_refresh: {e}")
            import traceback
            traceback.print_exc()

    def _reinit_plan_table(self):
        """Reinicializa la tabla del plan cuando cambia el modo SUB ASSY"""
        try:
            # Usar settings recargados en lugar de leer .env directamente
            sub_assy_mode = getattr(settings, 'SUB_ASSY_MODE', False)
            app_mode = getattr(settings, 'APP_MODE', 'ASSY')
            
            # ⚡ SIN UPH Real/Proy/CT: 10 columnas SUB ASSY, 9 normal
            num_columns = 10 if (sub_assy_mode and app_mode.upper() == 'ASSY') else 9
            current_columns = self.table_plan.columnCount()
            
            # Log para debug
            print(f"_reinit_plan_table: SUB_ASSY_MODE={sub_assy_mode}, APP_MODE={app_mode}")
            print(f"_reinit_plan_table: Cambiando de {current_columns} a {num_columns} columnas")
            
            # FORZAR recreación completa de la tabla
            self.table_plan.clear()
            self.table_plan.setRowCount(0)
            self.table_plan.setColumnCount(num_columns)
            
            # Headers dinámicos según modo SUB ASSY - ⚡ SIN UPH Real/Proy/CT
            if num_columns == 10:  # SUB ASSY
                headers = ["Part No", "Lote", "Modelo", "SUB ASSY", "Plan", "Producido", "% Avance", "UPH Target", "Estado", "Acciones"]
            else:  # Normal (9 columnas)
                headers = ["Part No", "Lote", "Modelo", "Plan", "Producido", "% Avance", "UPH Target", "Estado", "Acciones"]
            
            self.table_plan.setHorizontalHeaderLabels(headers)
            self.table_plan.horizontalHeader().setStretchLastSection(True)
            
            # Reajustar tamaños de columnas
            plan_header = self.table_plan.horizontalHeader()
            plan_header.resizeSection(0, 120)  # Part No
            plan_header.resizeSection(1, 90)   # Lote
            plan_header.resizeSection(2, 100)  # Modelo
            
            if num_columns == 10:  # Modo SUB ASSY - ⚡ SIN UPH Real/Proy/CT
                plan_header.resizeSection(3, 80)   # SUB ASSY
                plan_header.resizeSection(4, 60)   # Plan
                plan_header.resizeSection(5, 70)   # Producido
                plan_header.resizeSection(6, 70)   # % Avance
                plan_header.resizeSection(7, 70)   # UPH Target
                plan_header.resizeSection(8, 80)   # Estado
                plan_header.resizeSection(9, 100)  # Acciones
            else:  # Modo normal (9 columnas) - ⚡ SIN UPH Real/Proy/CT
                plan_header.resizeSection(3, 60)   # Plan
                plan_header.resizeSection(4, 70)   # Producido
                plan_header.resizeSection(5, 70)   # % Avance
                plan_header.resizeSection(6, 70)   # UPH Target
                plan_header.resizeSection(7, 80)   # Estado
                plan_header.resizeSection(8, 100)  # Acciones
            
            print(f"_reinit_plan_table: Tabla reinicializada con {self.table_plan.columnCount()} columnas")
            
        except Exception as e:
            print(f"Error reinicializando tabla del plan: {e}")
            import traceback
            traceback.print_exc()

    def refresh_totals_only(self) -> None:
        """
        Actualiza cards usando SOLO SQLite local (instantáneo) - MySQL se sincroniza en background
        ✅ ULTRA RÁPIDO: Lee de SQLite local (< 1ms)
        ✅ NO BLOQUEA: Sincronización con MySQL es automática en background
        ✅ MEJOR UX: Actualización instantánea sin esperas
        """
        try:
            # Si hay un escaneo en progreso, saltar actualización para no interferir
            if getattr(self, '_scan_in_progress', False):
                logger.debug("⏭️ Actualización de totales saltada: escaneo en progreso")
                return
            
            # ✅ OPTIMIZACIÓN: Leer directamente de SQLite local (instantáneo, < 1ms)
            # NO necesitamos worker thread porque SQLite local es ultra rápido
            from ..services.dual_db import get_dual_db
            
            dual_db = get_dual_db()
            
            # ✅ Lectura INSTANTÁNEA de SQLite local (no toca MySQL)
            totals_dict = dual_db.get_local_totals()
            
            # Resetear contador en memoria
            self._scan_counter = 0
            
            # Limpiar tabla
            self.table_totals.setRowCount(0)
            
            # Mostrar totales por línea
            for linea in self._get_line_options():
                if linea in totals_dict:
                    linea_data = totals_dict[linea]
                    for nparte, data in linea_data.items():
                        row = self.table_totals.rowCount()
                        self.table_totals.insertRow(row)
                        
                        fecha = data.get('fecha', 'Hoy')
                        modelo = data.get('modelo', 'Sin modelo')
                        cantidad = data.get('cantidad', 0)
                        uph_target = data.get('uph', 0)
                        uph_real = data.get('uph_real', 0)
                        
                        vals = [fecha, linea, nparte, modelo, cantidad, 
                               uph_target if uph_target > 0 else "", 
                               uph_real if uph_real > 0 else 0]
                        
                        for col, v in enumerate(vals):
                            self.table_totals.setItem(row, col, QtWidgets.QTableWidgetItem(str(v)))

            # Calcular total
            total_cantidad = sum(data.get('cantidad', 0) 
                               for linea_data in totals_dict.values() 
                               for data in linea_data.values())
            
            # Actualizar status con totales locales
            self._last_total_uph_real = total_cantidad
            
            # Estado de sincronización (worker en background)
            sync_status = "Sync OK" if dual_db._sync_worker.is_alive() else "Sync Error"
            
            # ✅ Mostrar que estamos usando caché local ultra rápido
            self.status.showMessage(f"⚡ SQLite Local (instantáneo) | {sync_status} | Total: {total_cantidad}")
            
            # ✅ ACTUALIZAR CARDS DE MÉTRICAS (Plan, Eficiencia, UPH, etc.)
            # Esto actualiza las 6 cards principales cada 15 segundos
            try:
                self.update_status_fast()  # Actualiza cards desde caché de métricas
            except Exception as card_error:
                logger.debug(f"⚠️ No se pudo actualizar cards de métricas: {card_error}")
            
            logger.debug(f"✅ Tabla y cards actualizadas desde SQLite local: {total_cantidad} piezas")
            
        except Exception as e:
            logger.error(f"Error actualizando totales desde SQLite local: {e}")
            # Fallback a MySQL directo solo si SQLite falla
            self._fallback_totals_update(e)
    
    def _fallback_totals_update(self, error: Exception):
        """Actualización de emergencia usando MySQL directo"""
        try:
            logger.warning(f"Usando fallback MySQL para totales debido a: {error}")
            totals = get_today_totals()
            self.table_totals.setRowCount(0)
            for t in totals:
                row = self.table_totals.rowCount()
                self.table_totals.insertRow(row)
                vals = [t.fecha, t.linea, t.nparte or "", t.modelo or "", t.cantidad_total, 
                       t.uph_target if t.uph_target is not None else "", 
                       t.uph_real if t.uph_real is not None else 0]
                for col, v in enumerate(vals):
                    self.table_totals.setItem(row, col, QtWidgets.QTableWidgetItem(str(v)))
            self.status.showMessage(f"MySQL Directo | Error local: {str(error)[:30]}...")
        except Exception as e2:
            logger.error(f"Error en fallback MySQL: {e2}")
            self.status.showMessage(f"Error total: {str(error)[:50]}...")

    def _refresh_plan_from_cache_only(self) -> None:
        """⚡ ULTRA RÁPIDO: Actualiza tabla de plan SOLO desde caché (0ms, sin BD)"""
        try:
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            # Obtener línea actual
            linea = self.linea_selector.currentText()
            
            # ⚡ Leer directamente del caché en memoria (sin BD)
            if not hasattr(dual_db, '_plan_cache') or not dual_db._plan_cache:
                return  # Caché vacío, no hacer nada
            
            # Filtrar planes de la línea actual desde caché
            plan_rows = [
                plan for plan in dual_db._plan_cache 
                if plan.get('line') == linea
            ]
            
            # Renderizar directamente (método interno que ya existe)
            self._on_plan_data_ready(linea, plan_rows, {})
            
        except Exception as e:
            logger.debug(f"Error refrescando desde caché: {e}")

    def refresh_plan_only(self, force=False) -> None:
        """Lanza un worker en background para obtener datos y renderizar cuando termine - OPTIMIZADO"""
        try:
            # ✅ Si hay un escaneo en progreso, saltar actualización para no interferir
            if not force and getattr(self, '_scan_in_progress', False):
                logger.debug("⏭️ Actualización de plan saltada: escaneo en progreso")
                return
                
            # ✅ Rate limiting MODERADO para evitar actualizaciones excesivas
            import time
            if not hasattr(self, '_last_refresh_time'):
                self._last_refresh_time = 0
            current_time = time.time()
            
            # ✅ Intervalo mínimo de 5 segundos entre actualizaciones
            # Con Dual DB optimizado, 5s es suficiente para prevenir sobrecarga
            min_interval = 5.0  # 5 segundos mínimo entre actualizaciones (optimizado con Dual DB)
            
            if not force and current_time - self._last_refresh_time < min_interval:
                logger.debug(f"⏭️ Actualización de plan saltada: muy reciente ({current_time - self._last_refresh_time:.1f}s < {min_interval}s)")
                return
            
            self._last_refresh_time = current_time

            # ✅ Si hay un worker ya corriendo, NO iniciar otro (previene acumulación)
            if getattr(self, '_plan_worker', None) and self._plan_worker.isRunning():
                if force:
                    # Esperar un poco a que termine el worker actual
                    self._plan_worker.wait(100)  # Esperar máximo 100ms
                    if self._plan_worker.isRunning():
                        logger.debug("⏭️ Worker de plan aún corriendo, saltando actualización")
                        return
                else:
                    logger.debug("⏭️ Worker de plan ya en ejecución, saltando")
                    return

            linea = self.linea_selector.currentText()
            # Crear worker
            class PlanFetchWorker(QtCore.QThread):
                data_ready = QtCore.pyqtSignal(str, object, dict)
                def __init__(self, linea: str, parent=None):
                    super().__init__(parent)
                    self._linea = linea
                def run(self):
                    try:
                        from ..services.dual_db import get_dual_db
                        dual_db = get_dual_db()
                        plan_rows = dual_db.get_plan_for_line_local(self._linea)
                        try:
                            uph_map = dual_db.get_uph_real_with_projection(self._linea)
                        except Exception:
                            uph_map = {}
                        self.data_ready.emit(self._linea, plan_rows, uph_map)
                    except Exception as e:
                        # En caso de error, emitir lista vacía para no bloquear
                        logger.debug(f"Error en PlanFetchWorker: {e}")
                        self.data_ready.emit(self._linea, [], {})

            self._plan_worker = PlanFetchWorker(linea, self)
            self._plan_worker.data_ready.connect(self._on_plan_data_ready)
            self._plan_worker.start()
            logger.debug(f"✅ Worker de plan iniciado para línea: {linea}")
        except Exception as e:
            import logging
            logging.error(f"Error en refresh_plan_only: {e}")

    def _on_plan_data_ready(self, linea: str, plan_rows, uph_proj_map: dict) -> None:
        """Renderiza la tabla de plan con datos obtenidos en background."""
        try:
            # Si el usuario cambió la línea mientras cargábamos, ignorar
            if linea != self.linea_selector.currentText():
                return
            self.table_plan.setRowCount(0)
            self.uph_proj_map = uph_proj_map or {}
            # Definición local del widget de progreso
            class _PlanProgressBar(QtWidgets.QWidget):
                def __init__(self, percent: float, projected: float|None=None, parent=None):
                    super().__init__(parent)
                    self.percent = max(0.0, min(100.0, percent))
                    self.projected = None if projected is None else max(0.0, min(100.0, projected))
                    self.setMinimumHeight(22)
                def sizeHint(self):
                    return QtCore.QSize(140, 22)
                def paintEvent(self, event):
                    p = QtGui.QPainter(self)
                    p.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing)
                    rect = self.rect().adjusted(0,0,-1,-1)
                    bg_grad = QtGui.QLinearGradient(QtCore.QPointF(rect.topLeft()), QtCore.QPointF(rect.bottomRight()))
                    bg_grad.setColorAt(0, QtGui.QColor('#243645'))
                    bg_grad.setColorAt(1, QtGui.QColor('#314c5f'))
                    p.setBrush(bg_grad)
                    p.setPen(QtGui.QPen(QtGui.QColor('#4d6475'), 1))
                    radius = 8
                    p.drawRoundedRect(rect, radius, radius)
                    if self.percent > 0:
                        w = rect.width() * (self.percent/100.0)
                        if 0 < w < 10:
                            w = 10
                        bar_rect = QtCore.QRectF(rect.x()+2, rect.y()+2, max(0, w-4), rect.height()-4)
                        grad = QtGui.QLinearGradient(bar_rect.topLeft(), bar_rect.topRight())
                        if self.percent >= 100:
                            grad.setColorAt(0, QtGui.QColor('#0f9d58'))
                            grad.setColorAt(0.5, QtGui.QColor('#18c96d'))
                            grad.setColorAt(1, QtGui.QColor('#35ef89'))
                        elif self.percent >= 80:
                            grad.setColorAt(0, QtGui.QColor('#3cba54'))
                            grad.setColorAt(1, QtGui.QColor('#68d468'))
                        elif self.percent >= 50:
                            grad.setColorAt(0, QtGui.QColor('#d8b11e'))
                            grad.setColorAt(1, QtGui.QColor('#f1cf46'))
                        else:
                            grad.setColorAt(0, QtGui.QColor('#a60000'))
                            grad.setColorAt(1, QtGui.QColor('#d63030'))
                        p.setPen(QtCore.Qt.PenStyle.NoPen)
                        p.setBrush(grad)
                        p.drawRoundedRect(bar_rect, radius-2, radius-2)
                    p.setPen(QtGui.QColor('white'))
                    font = p.font(); font.setBold(True); font.setPointSize(10); p.setFont(font)
                    text = f"{self.percent:.1f}%"
                    p.drawText(rect, QtCore.Qt.AlignmentFlag.AlignCenter, text)
                    p.end()
            def build_progress(percent_float: float, projected_percent: float|None=None):
                return _PlanProgressBar(percent_float, projected_percent)

            for r in (plan_rows or []):
                row = self.table_plan.rowCount()
                self.table_plan.insertRow(row)
                plan = r.get('plan_count') or 0
                prod = r.get('produced_count') or 0
                percent_val = (prod/plan*100) if plan > 0 else 0
                nparte = r.get('part_no','')
                plan_id = r.get('id')  # ID único del plan para operaciones
                estado = r.get('status') or ''
                uph_target_raw = r.get('uph')
                try:
                    uph_target = int(uph_target_raw) if uph_target_raw not in (None, '') else 0
                except Exception:
                    try:
                        uph_target = int(float(uph_target_raw)) if uph_target_raw else 0
                    except Exception:
                        uph_target = 0
                ct_raw = r.get('ct')
                try:
                    ct_val = float(ct_raw) if ct_raw not in (None,'') else 0
                except Exception:
                    ct_val = 0
                metrics = self.uph_proj_map.get(nparte, {'actual':0,'projected':0,'elapsed_min':60})
                uph_real = metrics.get('actual',0)
                uph_proj = metrics.get('projected',0)
                sub_assy_mode = getattr(settings, 'SUB_ASSY_MODE', False)
                app_mode = getattr(settings, 'APP_MODE', 'ASSY')
                show_sub_assy = (sub_assy_mode and app_mode.upper() == 'ASSY')
                if show_sub_assy:
                    try:
                        sub_assy_info = self._get_sub_assy_info(nparte)
                    except Exception:
                        sub_assy_info = "Error SUB ASSY"
                    # ⚡ SIN UPH Real/Proy/CT - Solo: Part No, Lote, Modelo, SUB ASSY, Plan, Producido, % Avance, UPH Target, Estado, Acciones
                    vals = [nparte, r.get('lot_no',''), r.get('model_code',''), sub_assy_info,
                            plan, prod, '', uph_target, estado, '']
                    progress_col = 6
                    acciones_col = 9
                else:
                    # ⚡ SIN UPH Real/Proy/CT - Solo: Part No, Lote, Modelo, Plan, Producido, % Avance, UPH Target, Estado, Acciones
                    vals = [nparte, r.get('lot_no',''), r.get('model_code',''),
                            plan, prod, '', uph_target, estado, '']
                    progress_col = 5
                    acciones_col = 8
                for col,v in enumerate(vals):
                    if col == progress_col:
                        self.table_plan.setCellWidget(row, col, build_progress(percent_val))
                        continue
                    elif col == acciones_col:
                        # Crear botón TERMINAR si está EN PROGRESO o PAUSADO
                        estado_upper = estado.upper()
                        if 'PROGRESO' in estado_upper or 'PAUSADO' in estado_upper:
                            btn_terminar = QtWidgets.QPushButton("TERMINAR")
                            btn_terminar.setStyleSheet("""
                                QPushButton {
                                    background-color: #dc3545;
                                    color: white;
                                    border: none;
                                    border-radius: 4px;
                                    padding: 4px 8px;
                                    font-size: 10px;
                                    font-weight: bold;
                                }
                                QPushButton:hover {
                                    background-color: #c82333;
                                }
                                QPushButton:pressed {
                                    background-color: #a71d2a;
                                }
                            """)
                            btn_terminar.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
                            btn_terminar.clicked.connect(
                                lambda checked, pid=plan_id, pn=nparte, ln=linea: self._terminar_plan(pid, pn, ln)
                            )
                            self.table_plan.setCellWidget(row, col, btn_terminar)
                        continue
                    item = QtWidgets.QTableWidgetItem(str(v))
                    # ⚡ Almacenar plan_id en la columna 0 (Part No) para identificación única
                    if col == 0:
                        item.setData(QtCore.Qt.ItemDataRole.UserRole, plan_id)
                    # ⚡ SIN coloreo UPH Real (columna eliminada)
                    self.table_plan.setItem(row, col, item)
            # Totales / métricas
            self._update_plan_totals(plan_rows or [])
            self._sync_metrics_to_widget()
        except Exception as e:
            import logging
            logging.error(f"Error renderizando plan (worker): {e}")

    def _get_sub_assy_info(self, nparte: str) -> str:
        """Obtiene información SUB ASSY desde el cache del sistema dual (muy rápido)"""
        try:
            from ..services.dual_db import get_dual_db
            
            dual_db = get_dual_db()
            return dual_db.get_sub_assy_info(nparte)
            
        except Exception as e:
            # Log error for debugging without using custom logger
            print(f"Error obteniendo SUB ASSY para {nparte}: {e}")
            return "Error SUB ASSY"

    def _update_single_plan_row(self, nparte: str, linea: str) -> None:
        """Actualiza solo la fila del plan correspondiente al nparte escaneado (ultra-rápido)"""
        try:
            # Solo actualizar si la línea actual coincide
            if linea != self.linea_selector.currentText():
                return
            
            # Buscar la fila que corresponde a este nparte
            row_to_update = -1
            for row in range(self.table_plan.rowCount()):
                item = self.table_plan.item(row, 0)  # Columna 0 = Part No
                if item and item.text() == nparte:
                    row_to_update = row
                    break
            
            if row_to_update < 0:
                # El nparte no está en la tabla visible, no hacer nada
                return
            
            # Obtener datos actualizados de SQLite local (ultra-rápido, < 5ms)
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            import sqlite3
            from ..config import settings
            from datetime import date
            today = date.today().isoformat()
            
            # ⚡ USAR LOCK GLOBAL para evitar "database is locked"
            with dual_db._sqlite_lock:
                with sqlite3.connect(dual_db.sqlite_path, timeout=5.0) as conn:
                    conn.execute("PRAGMA busy_timeout=5000")
                    conn.row_factory = sqlite3.Row
                    cur = conn.execute(
                        """
                        SELECT produced_count, plan_count FROM plan_local
                        WHERE line=? AND working_date=? AND part_no=?
                        LIMIT 1
                        """,
                        (linea, today, nparte)
                    )
                    row_data = cur.fetchone()
            
            if not row_data:
                return
            
            produced = row_data['produced_count'] or 0
            plan = row_data['plan_count'] or 0
            percent_val = (produced / plan * 100) if plan > 0 else 0
            
            # Determinar columna de Producido según modo SUB ASSY
            sub_assy_mode = getattr(settings, 'SUB_ASSY_MODE', False)
            app_mode = getattr(settings, 'APP_MODE', 'ASSY')
            show_sub_assy = (sub_assy_mode and app_mode.upper() == 'ASSY')
            
            if show_sub_assy:
                producido_col = 5  # Con SUB ASSY: Part No, Lote, Modelo, SUB ASSY, Plan, [Producido]
                avance_col = 6
            else:
                producido_col = 4  # Sin SUB ASSY: Part No, Lote, Modelo, Plan, [Producido]
                avance_col = 5
            
            # Actualizar columna "Producido"
            item_prod = self.table_plan.item(row_to_update, producido_col)
            if item_prod:
                item_prod.setText(str(produced))
            else:
                item_prod = QtWidgets.QTableWidgetItem(str(produced))
                self.table_plan.setItem(row_to_update, producido_col, item_prod)
            
            # Actualizar barra de progreso "% Avance"
            class _PlanProgressBar(QtWidgets.QWidget):
                def __init__(self, percent: float, parent=None):
                    super().__init__(parent)
                    self.percent = max(0.0, min(100.0, percent))
                    self.setMinimumHeight(22)
                
                def sizeHint(self):
                    return QtCore.QSize(140, 22)
                
                def paintEvent(self, event):
                    p = QtGui.QPainter(self)
                    p.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing)
                    rect = self.rect().adjusted(0,0,-1,-1)
                    bg_grad = QtGui.QLinearGradient(QtCore.QPointF(rect.topLeft()), QtCore.QPointF(rect.bottomRight()))
                    bg_grad.setColorAt(0, QtGui.QColor('#243645'))
                    bg_grad.setColorAt(1, QtGui.QColor('#314c5f'))
                    p.setBrush(bg_grad)
                    p.setPen(QtGui.QPen(QtGui.QColor('#4d6475'), 1))
                    radius = 8
                    p.drawRoundedRect(rect, radius, radius)
                    if self.percent > 0:
                        w = rect.width() * (self.percent/100.0)
                        if 0 < w < 10:
                            w = 10
                        bar_rect = QtCore.QRectF(rect.x()+2, rect.y()+2, max(0, w-4), rect.height()-4)
                        grad = QtGui.QLinearGradient(bar_rect.topLeft(), bar_rect.topRight())
                        if self.percent >= 100:
                            grad.setColorAt(0, QtGui.QColor('#0f9d58'))
                            grad.setColorAt(0.5, QtGui.QColor('#18c96d'))
                            grad.setColorAt(1, QtGui.QColor('#35ef89'))
                        elif self.percent >= 80:
                            grad.setColorAt(0, QtGui.QColor('#3cba54'))
                            grad.setColorAt(1, QtGui.QColor('#68d468'))
                        elif self.percent >= 50:
                            grad.setColorAt(0, QtGui.QColor('#d8b11e'))
                            grad.setColorAt(1, QtGui.QColor('#f1cf46'))
                        else:
                            grad.setColorAt(0, QtGui.QColor('#a60000'))
                            grad.setColorAt(1, QtGui.QColor('#d63030'))
                        p.setPen(QtCore.Qt.PenStyle.NoPen)
                        p.setBrush(grad)
                        p.drawRoundedRect(bar_rect, radius-2, radius-2)
                    p.setPen(QtGui.QColor('white'))
                    font = p.font(); font.setBold(True); font.setPointSize(10); p.setFont(font)
                    text = f"{self.percent:.1f}%"
                    p.drawText(rect, QtCore.Qt.AlignmentFlag.AlignCenter, text)
                    p.end()
            
            progress_widget = _PlanProgressBar(percent_val)
            self.table_plan.setCellWidget(row_to_update, avance_col, progress_widget)
            
            logger.debug(f"✅ Fila del plan actualizada: {nparte} → Producido: {produced}/{plan} ({percent_val:.1f}%)")
            
        except Exception as e:
            logger.debug(f"Error actualizando fila del plan: {e}")

    def export_csv(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Exportar CSV", "escaneos.csv", "CSV Files (*.csv)")
        if not path:
            return
        scans = get_last_scans(1000)
        try:
            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["id", "ts", "raw", "lote", "secuencia", "estacion", "nparte", "modelo", "cantidad", "linea"])
                for s in scans:
                    writer.writerow([s.id, s.ts, s.raw, s.lote, s.secuencia, s.estacion, s.nparte, s.modelo or "", s.cantidad, s.linea])
            QtWidgets.QMessageBox.information(self, "Exportar", "Archivo CSV generado")
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Exportar", f"No se pudo exportar: {e}")

    





    def configure_db_location(self) -> None:
        """Configurar la ubicación de la base de datos local SQLite"""
        try:
            import os
            
            # Método deshabilitado - sistema optimizado usa solo MySQL directo
            QtWidgets.QMessageBox.information(
                self,
                "Configuración no disponible",
                "El sistema optimizado usa MySQL directo.\nNo requiere configuración de base de datos local."
            )
            return
            
            # Código legacy comentado:
            # dual_db = get_dual_db()
            # current_path = dual_db.sqlite_path
            current_dir = os.path.dirname(current_path)
            
            # Diálogo de información actual
            current_info = f"Ubicación actual: {current_path}\nTamaño: {self._get_db_size(current_path)}"
            
            reply = QtWidgets.QMessageBox.question(
                self,
                "Configurar Base de Datos Local",
                f"{current_info}\n\n¿Desea cambiar la ubicación de la base de datos SQLite local?",
                QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
                QtWidgets.QMessageBox.StandardButton.No
            )
            
            if reply == QtWidgets.QMessageBox.StandardButton.Yes:
                # Selector de nueva ubicación
                new_dir = QtWidgets.QFileDialog.getExistingDirectory(
                    self,
                    "Seleccionar nueva ubicación para base de datos local",
                    current_dir
                )
                
                if new_dir:
                    new_path = os.path.join(new_dir, "local_scans.db")
                    
                    # Confirmar cambio
                    confirm = QtWidgets.QMessageBox.question(
                        self,
                        "Confirmar cambio",
                        f"Nueva ubicación: {new_path}\n\n¿Desea mover la base de datos a esta ubicación?\n\nNota: La aplicación se reiniciará.",
                        QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No
                    )
                    
                    if confirm == QtWidgets.QMessageBox.StandardButton.Yes:
                        self._move_database(current_path, new_path)
        
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Error configurando DB: {e}")
    
    def _get_db_size(self, path: str) -> str:
        """Obtener tamaño de la base de datos"""
        try:
            import os
            if os.path.exists(path):
                size_bytes = os.path.getsize(path)
                if size_bytes < 1024:
                    return f"{size_bytes} bytes"
                elif size_bytes < 1024 * 1024:
                    return f"{size_bytes / 1024:.1f} KB"
                else:
                    return f"{size_bytes / (1024 * 1024):.1f} MB"
            else:
                return "No existe"
        except:
            return "Desconocido"
    
    def _move_database(self, old_path: str, new_path: str) -> None:
        """Método deshabilitado - sistema optimizado usa solo MySQL directo"""
        QtWidgets.QMessageBox.information(
            self,
            "Operación no disponible",
            "El sistema optimizado usa MySQL directo.\nNo requiere mover bases de datos locales."
        )
        return

    def open_configuracion(self) -> None:
        """Abrir diálogo de configuración general del sistema"""
        try:
            # Crear diálogo de configuración
            dialog = ConfiguracionDialog(self)
            dialog.exec()
            
            # Actualizar totales después de cualquier cambio
            self.refresh_totals_only()
            # Aplicar línea por defecto actualizada si cambió
            try:
                from ..config import settings as _settings
                # Rellenar opciones por modo actual
                current_lines = self._get_line_options()
                self.linea_selector.clear()
                self.linea_selector.addItems(current_lines)
                new_default = getattr(_settings, 'DEFAULT_LINE', None)
                if new_default and new_default in current_lines:
                    self.linea_selector.setCurrentText(new_default)
                else:
                    self.linea_selector.setCurrentIndex(0)
                self.refresh_plan_only()
            except Exception:
                pass
            
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Error abriendo configuración: {e}")

    def toggle_fullscreen_mode(self) -> None:
        """Alternar entre modo pantalla completa y modo normal"""
        try:
            if self._fullscreen_mode:
                self._exit_fullscreen_mode()
            else:
                self._enter_fullscreen_mode()
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Error cambiando modo pantalla completa: {e}")

    def _enter_fullscreen_mode(self) -> None:
        """Entrar en modo pantalla completa"""
        # Guardar estado actual de la ventana
        self._normal_window_state = self.saveGeometry()
        
        # Hacer el campo de escaneo invisible pero funcional
        # En lugar de hide(), hacerlo transparente y de altura 0
        self.scan_input.setStyleSheet("""
            QLineEdit {
                background-color: transparent;
                border: none;
                color: transparent;
                height: 0px;
                max-height: 0px;
                min-height: 0px;
                padding: 0px;
                margin: 0px;
            }
        """)
        
        # Ocultar todo el contenedor del selector de línea
        if hasattr(self, 'linea_container_widget'):
            self.linea_container_widget.hide()
        
        # Ocultar la barra de menú y status
        self.menuBar().hide()
        self.statusBar().hide()
        
        # MANTENER EL LOGO VISIBLE - asegurarse de que esté visible
        if hasattr(self, 'logo_label'):
            self.logo_label.show()
            self.logo_label.raise_()  # Traer al frente
        
        # Modificar el título del plan - incluir información de la línea actual
        linea_actual = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else "N/A"
        self.title_plan.setText(f"PLAN DE PRODUCCIÓN - LÍNEA {linea_actual} - ESCANEO ACTIVO")
        self.title_plan.setStyleSheet("""
            font-weight: bold; 
            font-size: 24px; 
            color: #ffffff; 
            background-color: #1f2d3a; 
            padding: 15px; 
            margin: 5px;
            text-align: center;
        """)
        
        # Aumentar tamaño de fuente de la tabla
        self.table_plan.setStyleSheet("""
            QTableWidget {
                font-size: 18px; 
                gridline-color: #2d3e50;
                selection-background-color: #3498db;
            }
            QHeaderView::section {
                background: #1f2d3a; 
                color: #e0e0e0; 
                font-weight: bold; 
                font-size: 16px; 
                padding: 8px;
            }
        """)
        
        # Aumentar altura de las filas
        self.table_plan.verticalHeader().setDefaultSectionSize(45)
        
        # Mejorar el logo para pantalla completa
        if hasattr(self, 'logo_label'):
            # Hacer el logo más grande en pantalla completa
            logo_path = ROOT_DIR / "logoLogIn.png"
            if logo_path.exists():
                pix = QtGui.QPixmap(str(logo_path))
                if not pix.isNull():
                    # Logo más grande para pantalla completa
                    self.logo_label.setPixmap(pix.scaledToHeight(50, QtCore.Qt.TransformationMode.SmoothTransformation))
        
        # Cambiar a pantalla completa
        self.showFullScreen()
        
        # Mostrar botón para salir del modo pantalla completa
        if hasattr(self, 'exit_fullscreen_btn'):
            self.exit_fullscreen_btn.show()
            self.exit_fullscreen_btn.raise_()  # Traer al frente
        
        # Actualizar estado
        self._fullscreen_mode = True
        
        # El campo de escaneo mantiene el foco aunque esté invisible
        self.scan_input.setFocus()
        # Forzar que mantenga el foco de manera más agresiva
        QtCore.QTimer.singleShot(100, lambda: self.scan_input.setFocus())
        QtCore.QTimer.singleShot(500, lambda: self.scan_input.setFocus())

    def _exit_fullscreen_mode(self) -> None:
        """Salir del modo pantalla completa"""
        # Restaurar el estilo normal del campo de escaneo
        self.scan_input.setStyleSheet("")  # Eliminar estilos personalizados
        self.scan_input.show()
        
        # Mostrar el contenedor del selector de línea
        if hasattr(self, 'linea_container_widget'):
            self.linea_container_widget.show()
        
        # Mostrar la barra de menú y status
        self.menuBar().show()
        self.statusBar().show()
        
        # Restaurar título del plan
        self.title_plan.setText("Plan de Producción (Línea Seleccionada)")
        self.title_plan.setStyleSheet("font-weight: bold; margin-top:8px;")
        
        # Restaurar tamaño de fuente de la tabla
        self.table_plan.setStyleSheet("""
            QTableWidget {font-size:14px; gridline-color:#2d3e50;}
            QHeaderView::section {background:#1f2d3a; color:#e0e0e0; font-weight:bold; font-size:13px; padding:4px;}
        """)
        
        # Restaurar altura de las filas
        self.table_plan.verticalHeader().setDefaultSectionSize(32)
        
        # Restaurar tamaño normal del logo
        if hasattr(self, 'logo_label'):
            logo_path = ROOT_DIR / "logoLogIn.png"
            if logo_path.exists():
                pix = QtGui.QPixmap(str(logo_path))
                if not pix.isNull():
                    # Tamaño normal del logo
                    self.logo_label.setPixmap(pix.scaledToHeight(28, QtCore.Qt.TransformationMode.SmoothTransformation))
        
        # Restaurar ventana normal
        self.showNormal()
        
        # Ocultar botón de salir del modo pantalla completa
        if hasattr(self, 'exit_fullscreen_btn'):
            self.exit_fullscreen_btn.hide()
        
        # Restaurar geometría si se guardó
        if self._normal_window_state:
            self.restoreGeometry(self._normal_window_state)
        
        # Actualizar estado
        self._fullscreen_mode = False
        
        # Restaurar foco al campo de escaneo
        self.scan_input.setFocus()

    def keyPressEvent(self, event):
        """Manejar eventos de teclado"""
        # ESC para salir del modo pantalla completa
        if event.key() == QtCore.Qt.Key.Key_Escape and self._fullscreen_mode:
            self._exit_fullscreen_mode()
            event.accept()
            return
        
        # F11 para alternar pantalla completa
        if event.key() == QtCore.Qt.Key.Key_F11:
            self.toggle_fullscreen_mode()
            event.accept()
            return
            
        # Pasar el evento al padre
        super().keyPressEvent(event)

    # Fin de clase

    def _emit_scan_processed(self, linea: str, nparte: str, event: str) -> None:
        """Callback (worker thread) para recibir notificaciones del backend."""
        try:
            self.scan_processed_signal.emit(linea or "", nparte or "", event or "")
        except RuntimeError:
            # La ventana ya se cerró; ignorar
            pass

    @QtCore.pyqtSlot(str, str, str)
    def _handle_scan_processed(self, linea: str, nparte: str, event: str) -> None:
        """Actualiza UI inmediatamente cuando se completa un par."""
        if event != "PAIR_COMPLETED":
            return
        try:
            # Actualizar el timestamp del último escaneo para esta línea
            import time
            if linea:
                self._last_scan_time_per_line[linea] = time.time()
            
            # 🚀 OPTIMIZACIÓN: Actualizar caché de métricas inmediatamente
            # (sin consultar MySQL, solo incrementa contador en SQLite)
            try:
                from ..services.metrics_cache import get_metrics_cache
                from datetime import date
                
                metrics_cache = get_metrics_cache()
                if metrics_cache and linea:
                    fecha_hoy = date.today().isoformat()
                    
                    # Obtener métricas actuales del caché
                    cached = metrics_cache.get_metrics_from_cache(linea, fecha_hoy)
                    if cached:
                        # Incrementar producción real
                        cached['produccion_real'] += 1
                        
                        # Recalcular eficiencia
                        if cached['plan_acumulado'] > 0:
                            cached['eficiencia'] = (cached['produccion_real'] / cached['plan_acumulado']) * 100
                        
                        # Actualizar caché
                        metrics_cache.update_metrics_instant(linea, fecha_hoy, cached)
                        
                        # 🚀 Actualizar cards instantáneamente desde caché
                        if self.linea_selector.currentText() == linea:
                            self._update_cards_with_metrics(
                                cached['plan_total'],
                                cached['plan_acumulado'],
                                cached['produccion_real'],
                                cached['eficiencia'],
                                cached['uph'],
                                cached['upph']
                            )
                            logger.debug(f"⚡ Cards actualizadas instantáneamente desde caché tras escaneo")
            except Exception as cache_err:
                logger.debug(f"Error actualizando caché de métricas: {cache_err}")
            
            # ⚡ ACTUALIZACIÓN INCREMENTAL: Actualizar solo la fila del nparte escaneado
            # Esto es rápido porque solo actualiza UNA fila, no toda la tabla
            try:
                self._update_single_plan_row(nparte, linea)
            except Exception as update_err:
                logger.debug(f"Error actualizando fila del plan: {update_err}")
            
            # Solo incrementar contador visual (instantáneo, no toca DB)
            if not hasattr(self, '_scan_counter'):
                self._scan_counter = 0
            self._scan_counter += 1
            
            # Actualizar status con contador en memoria (ultra-rápido, no toca DB)
            self.update_status_fast()
            
        except Exception as refresh_err:
            logger.debug(f"Error en handle_scan_processed: {refresh_err}")
        try:
            # Sincronizar métricas si la ventana flotante está abierta
            # Esto es rápido porque solo copia valores ya calculados
            self._sync_metrics_to_widget()
        except Exception:
            pass

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """Desregistrar listeners antes de cerrar la ventana."""
        try:
            if getattr(self, "_direct_mysql_listener_registered", False) and getattr(self, "_direct_mysql", None):
                self._direct_mysql.unregister_scan_listener(self._emit_scan_processed)
        except Exception:
            pass
        finally:
            super().closeEvent(event)

    def _get_line_options(self) -> list:
        """Opciones de línea según modo actual (ASSY/IMD)."""
        try:
            from ..config import settings as _settings
            mode = getattr(_settings, 'APP_MODE', 'ASSY').upper()
            if mode == 'IMD':
                return ["PANA A", "PANA B", "PANA C", "PANA D"]
        except Exception:
            pass
        return ["M1", "M2", "M3", "M4", "D1", "D2", "D3", "H1"]
