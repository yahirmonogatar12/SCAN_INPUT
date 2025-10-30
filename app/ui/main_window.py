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
    
    # Mapeo de caracteres comunes que cambian entre distribucií³n inglí©s/espaí±ol
    # NOTA: NO convertir í‘ porque es separador válido en QR formato nuevo
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
                    logger.info(f"… Lí­nea predeterminada establecida: '{line}' (í­ndice {i})")
                    self._linea_anterior = line
                    line_found = True
                    break
            
            if not line_found:
                logger.warning(f"⚠️ Línea '{default_line}' no encontrada. Usando primera opción.")
                default_line = allowed[0] if allowed else "M1"
                self.linea_selector.setCurrentText(default_line)
                self._linea_anterior = default_line
            
            # ðŸ”„ Notificar al cache de mí©tricas sobre la lí­nea inicial
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
        # Hacer el dropdown mucho más pequeí±o
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
        
        # Botí³n de actualizacií³n manual
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
        
        # Botí³n para abrir ventana flotante de mí©tricas
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
        
        # Guardar referencias especí­ficas para el modo pantalla completa
        self.linea_label = linea_label
        
        # Variable para la ventana flotante
        self.metrics_widget = None
        
        linea_layout.addWidget(linea_label)
        linea_layout.addWidget(self.linea_selector)
        linea_layout.addWidget(self.status_dual)
        linea_layout.addWidget(self.refresh_button)
        linea_layout.addWidget(self.float_metrics_button)
        linea_layout.addStretch()

        # Crear un widget contenedor para el layout de lí­nea para poder ocultarlo fácilmente
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
        
        # Tabla de plan  SIN UPH Real/Proy/CT (ya tenemos cards)
        # Determinar níºmero de columnas segíºn modo SUB ASSY (ahora +1 por columna de Acciones)
        num_columns = 10 if (getattr(settings, 'SUB_ASSY_MODE', False) and 
                            getattr(settings, 'APP_MODE', 'ASSY').upper() == 'ASSY') else 9
        
        self.table_plan = QtWidgets.QTableWidget(0, num_columns)
        self.table_plan.setStyleSheet("""
            QTableWidget {font-size:14px; gridline-color:#2d3e50;}
            QHeaderView::section {background:#1f2d3a; color:#e0e0e0; font-weight:bold; font-size:13px; padding:4px;}
        """)
        self.table_plan.verticalHeader().setDefaultSectionSize(32)
        
        # Headers dinámicos segíºn modo SUB ASSY (con nueva columna Acciones) -  Sin UPH Real/Proy/CT
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
        
        if num_columns == 10:  # Modo SUB ASSY con Acciones -  Sin UPH Real/Proy/CT
            plan_header.resizeSection(3, 80)   # SUB ASSY
            plan_header.resizeSection(4, 60)   # Plan
            plan_header.resizeSection(5, 70)   # Producido
            plan_header.resizeSection(6, 70)   # % Avance
            plan_header.resizeSection(7, 70)   # UPH Target
            plan_header.resizeSection(8, 80)   # Estado
            plan_header.resizeSection(9, 100)  # Acciones
        else:  # Modo normal con Acciones (9 columnas) -  Sin UPH Real/Proy/CT
            plan_header.resizeSection(3, 60)   # Plan
            plan_header.resizeSection(4, 70)   # Producido
            plan_header.resizeSection(5, 70)   # % Avance
            plan_header.resizeSection(6, 70)   # UPH Target
            plan_header.resizeSection(7, 80)   # Estado
            plan_header.resizeSection(8, 100)  # Acciones

        # Contenedor para totales y plan
        # Contenedor principal con tí­tulo
        main_container = QtWidgets.QWidget()
        main_layout = QtWidgets.QVBoxLayout(main_container)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        #  TARJETAS DE TOTALES ARRIBA (antes del tí­tulo)
        self.plan_totals_widget = self._create_plan_totals_widget()
        main_layout.addWidget(self.plan_totals_widget)
        
        self.title_plan = QtWidgets.QLabel("Plan de Produccion (Lí­nea Seleccionada)")
        self.title_plan.setStyleSheet("font-weight: bold; margin-top:8px;")
        main_layout.addWidget(self.title_plan)
        
        # Tabla de plan (sin splitter, tarjetas ya están arriba)
        main_layout.addWidget(self.table_plan)
        
        # ðŸ“œ HISTORIAL DE SCANS (QR y BARCODE)
        self.scan_history_widget = self._create_scan_history_widget()
        main_layout.addWidget(self.scan_history_widget)
        
        layout.addWidget(main_container)

        # Status bar (oculto - info en tarjetas)
        self.status = self.statusBar()
        self.status.hide()  #  Ocultamos la barra de abajo

        # Meníº
        menubar = self.menuBar()
        admin_menu = menubar.addMenu("Opciones")
        
        # Opcií³n para modo pantalla completa
        self.action_fullscreen = admin_menu.addAction("Modo Pantalla Completa (F11)")
        self.action_fullscreen.triggered.connect(self.toggle_fullscreen_mode)
        
        admin_menu.addSeparator()
        
        # Opcií³n para configurar ubicacií³n de DB local
        self.action_db_config = admin_menu.addAction("Configurar DB Local")
        self.action_db_config.triggered.connect(self.configure_db_location)
        
        admin_menu.addSeparator()

        # Opción de configuración general
        self.action_configuracion = admin_menu.addAction("Configuracion")
        self.action_configuracion.triggered.connect(self.open_configuracion)

        # Conexiones simplificadas
        #  ESCANEO SIMPLIFICADO: Solo procesa con ENTER (escáner enví­a ENTER automáticamente)
        # Eliminado reconocimiento automático de QR/BARCODE para evitar procesamiento innecesario
        self._processing_scan = False
        self._scan_in_progress = False  # Flag para pausar actualizaciones durante escaneo
        self.scan_input.returnPressed.connect(self.handle_scan)  #  Solo ENTER
        self._last_processed_code = ""  # Para evitar duplicados
        self._last_processed_time = 0
        
        # Configuracií³n por defecto - siempre modo rápido y auto-refresh
        self._fast_scan_mode = True
        self._auto_refresh_active = True

        # Atajo F2 para foco
        shortcut = QtGui.QShortcut(QtGui.QKeySequence("F2"), self)
        shortcut.activated.connect(lambda: self.scan_input.setFocus())
        
        # Atajo F11 para modo pantalla completa
        fullscreen_shortcut = QtGui.QShortcut(QtGui.QKeySequence("F11"), self)
        fullscreen_shortcut.activated.connect(self.toggle_fullscreen_mode)

        # … Timer automático para actualizaciones de CARDS (15 SEGUNDOS - SQLite instantáneo!)
        # Ahora es SEGURO actualizar cada 5s porque SQLite local es < 1ms
        #  Reducido a 5s para detectar cambios de plan más rápido (antes 15s)
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(5_000)  # … 5 SEGUNDOS (5,000 ms) - Lectura instantánea de SQLite
        self.timer.timeout.connect(self._update_tables_and_status)
        self.timer.start()  # Siempre activo - sin bloqueos gracias a Dual DB
        
        # âŒ DESHABILITADO - Timer de plan redundante (ahora _update_tables_and_status hace todo)
        # self.plan_timer = QtCore.QTimer(self)
        # self.plan_timer.setInterval(15_000)
        # self.plan_timer.timeout.connect(lambda: self.refresh_plan_only(force=False))
        # self.plan_timer.start()

        # Cambio de lí­nea actualiza plan (con validacií³n de plan en progreso)
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
        
        # Variable para rastrear el íºltimo escaneo por lí­nea
        self._last_scan_time_per_line = {}  # {linea: timestamp}
        
        # Cachí© para plan en progreso (evitar consultas constantes)
        self._plan_en_progreso_cache = {}  # {linea: (plan_id, part_no, timestamp)}

        # Meníº Admin siempre visible (sin control de usuarios)

        # Crear overlay de notificacií³n para duplicados
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
        
        # Crear botí³n para salir del modo pantalla completa (inicialmente oculto)
        self._create_exit_fullscreen_button()

    def showEvent(self, event):
        """Evento cuando se muestra la ventana - forzar icono en barra de tareas Windows"""
        super().showEvent(event)
        
        # Windows: Forzar icono en la barra de tareas usando Win32 API
        if hasattr(self, '_icon_set'):
            return  # Ya se establecií³
            
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
                        0, 0,  # tamaí±o (0 = usar tamaí±o predeterminado del icono)
                        LR_LOADFROMFILE | LR_DEFAULTSIZE
                    )
                    
                    # Cargar icono pequeí±o (para tí­tulo de ventana)
                    hicon_small = ctypes.windll.user32.LoadImageW(
                        None,
                        icon_path,
                        IMAGE_ICON,
                        16, 16,  # tamaí±o pequeí±o
                        LR_LOADFROMFILE
                    )
                    
                    if hicon_big:
                        # Establecer icono grande (barra de tareas)
                        ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, ICON_BIG, hicon_big)
                    
                    if hicon_small:
                        # Establecer icono pequeí±o (tí­tulo de ventana)
                        ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, ICON_SMALL, hicon_small)
                    
                    logger.info("… Icono de barra de tareas establecido correctamente")
                    self._icon_set = True
                    
        except Exception as e:
            logger.warning(f"No se pudo establecer el icono de la barra de tareas: {e}")

    def _create_plan_totals_widget(self):
        """Crea tarjetas grandes de totales estilo control_bom.css"""
        widget = QtWidgets.QFrame()
        widget.setMinimumHeight(140)  #  Altura mí­nima para que se vea
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
        
        # Tarjeta 1: Plan Total de la Lí­nea
        self.card_plan = self._create_metric_card("Plan", "0", "#3498DB")
        layout.addWidget(self.card_plan)
        
        # Tarjeta 2: Plan Acumulado (expectativa segíºn tiempo transcurrido)
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
        
        #  BARRA DE PROGRESO DE PRODUCCIí“N (estilo control_bom)
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
        self.general_progress_bar.setMaximum(1000)  # x10 para mayor precisií³n
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
        
        # â±ï¸ INDICADOR DE TIEMPO TRANSCURRIDO
        self.tiempo_transcurrido_label = QtWidgets.QLabel("Tiempo transcurrido: -- min")
        self.tiempo_transcurrido_label.setStyleSheet("""
            color: #95a5a6;
            font-size: 10px;
            font-weight: 500;
            background: transparent;
            padding: 2px 0px;
        """)
        self.tiempo_transcurrido_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        progress_layout.addWidget(self.tiempo_transcurrido_label)
        
        # ðŸ“… INDICADOR VISUAL DE FECHA DEL PLAN (PROMINENTE)
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
        card.setMinimumHeight(100)  #  Altura mí­nima visible
        card.setMinimumWidth(120)   #  Ancho mí­nimo visible
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
        
        # Tí­tulo (arriba, pequeí±o, estilo control_bom)
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
        
        # Guardar referencia al label de valor para actualizar despuí©s
        card.value_label = value_label
        card.color = color
        
        return card
    
    def _create_scan_history_widget(self):
        """Crea widget de historial de scans en formato LISTA VERTICAL con altura ajustable"""
        # Contenedor principal
        container = QtWidgets.QFrame()
        container.setStyleSheet("""
            QFrame {
                background-color: #34334E;
                border: 1px solid #20688C;
                border-radius: 4px;
                padding: 8px;
            }
        """)
        # Altura inicial ajustable (se puede cambiar con botones)
        self._scan_history_height = 200  # Altura inicial en píxeles
        container.setMinimumHeight(self._scan_history_height)
        container.setMaximumHeight(self._scan_history_height)
        
        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(4)
        
        # Header: Título + Botones de control de altura
        header_layout = QtWidgets.QHBoxLayout()
        header_layout.setSpacing(8)
        
        # Título
        title = QtWidgets.QLabel("Historial de Escaneos (Ultimos 20)")
        title.setStyleSheet("""
            color: #3498db;
            font-size: 12px;
            font-weight: bold;
            background: transparent;
        """)
        header_layout.addWidget(title)

        header_layout.addStretch(1)

        # Botón: Ver historial completo
        btn_full_history = QtWidgets.QPushButton("Ver Todo")
        btn_full_history.setFixedSize(80, 24)
        btn_full_history.setToolTip("Ver historial completo en ventana separada")
        btn_full_history.setStyleSheet("""
            QPushButton {
                background-color: #27ae60;
                border: 1px solid #229954;
                border-radius: 4px;
                color: white;
                font-weight: bold;
                font-size: 10px;
            }
            QPushButton:hover {
                background-color: #2ecc71;
                border-color: #27ae60;
            }
            QPushButton:pressed {
                background-color: #1e8449;
            }
        """)
        btn_full_history.clicked.connect(self._show_full_history_window)
        header_layout.addWidget(btn_full_history)        # Botón: Reducir altura
        btn_decrease = QtWidgets.QPushButton("")
        btn_decrease.setFixedSize(30, 24)
        btn_decrease.setToolTip("Reducir altura del historial")
        btn_decrease.setStyleSheet("""
            QPushButton {
                background-color: #3C3940;
                border: 1px solid #20688C;
                border-radius: 4px;
                color: #3498db;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #45424A;
                border-color: #3498db;
            }
            QPushButton:pressed {
                background-color: #2c3e50;
            }
        """)
        btn_decrease.clicked.connect(lambda: self._adjust_scan_history_height(-50))
        header_layout.addWidget(btn_decrease)
        
        # Etiqueta de altura actual
        self.scan_history_height_label = QtWidgets.QLabel(f"{self._scan_history_height}px")
        self.scan_history_height_label.setStyleSheet("""
            color: #95a5a6;
            font-size: 10px;
            background: transparent;
            padding: 0px 4px;
        """)
        self.scan_history_height_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.scan_history_height_label.setFixedWidth(50)
        header_layout.addWidget(self.scan_history_height_label)
        
        # Botón: Aumentar altura
        btn_increase = QtWidgets.QPushButton("")
        btn_increase.setFixedSize(30, 24)
        btn_increase.setToolTip("Aumentar altura del historial")
        btn_increase.setStyleSheet("""
            QPushButton {
                background-color: #3C3940;
                border: 1px solid #20688C;
                border-radius: 4px;
                color: #3498db;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #45424A;
                border-color: #3498db;
            }
            QPushButton:pressed {
                background-color: #2c3e50;
            }
        """)
        btn_increase.clicked.connect(lambda: self._adjust_scan_history_height(50))
        header_layout.addWidget(btn_increase)
        
        layout.addLayout(header_layout)
        
        # Área de scroll VERTICAL para mostrar scans en lista
        scroll_area = QtWidgets.QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll_area.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll_area.setStyleSheet("""
            QScrollArea {
                background: transparent;
                border: none;
            }
            QScrollBar:vertical {
                background: #2c3e50;
                width: 10px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical {
                background: #3498db;
                border-radius: 5px;
                min-height: 20px;
            }
            QScrollBar::handle:vertical:hover {
                background: #5dade2;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
        """)
        
        # Widget interno con layout VERTICAL para lista de scans
        self.scan_history_container = QtWidgets.QWidget()
        self.scan_history_layout = QtWidgets.QVBoxLayout(self.scan_history_container)
        self.scan_history_layout.setContentsMargins(0, 0, 0, 0)
        self.scan_history_layout.setSpacing(4)
        self.scan_history_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop)
        
        # Mensaje inicial (sin scans)
        self.scan_history_empty_label = QtWidgets.QLabel("Esperando escaneos...")
        self.scan_history_empty_label.setStyleSheet("""
            color: #95a5a6;
            font-size: 11px;
            font-style: italic;
            background: transparent;
            padding: 8px;
        """)
        self.scan_history_layout.addWidget(self.scan_history_empty_label)
        
        scroll_area.setWidget(self.scan_history_container)
        layout.addWidget(scroll_area)
        
        # Lista para mantener historial de scans (máximo 20)
        self.scan_history = []
        
        # Guardar referencia al contenedor para ajustar altura
        self.scan_history_main_container = container
        
        return container
    
    def _adjust_scan_history_height(self, delta: int):
        """Ajusta la altura del historial de scans"""
        # Límites: mínimo 100px, máximo 500px
        new_height = max(100, min(500, self._scan_history_height + delta))
        
        if new_height != self._scan_history_height:
            self._scan_history_height = new_height
            self.scan_history_main_container.setMinimumHeight(new_height)
            self.scan_history_main_container.setMaximumHeight(new_height)
            self.scan_history_height_label.setText(f"{new_height}px")

    def _show_full_history_window(self):
        """Muestra una ventana con el historial completo de scans con filtros de fecha"""
        try:
            from datetime import datetime
            from zoneinfo import ZoneInfo

            # Crear ventana modal
            dialog = QtWidgets.QDialog(self)
            dialog.setWindowTitle("Historial Completo de Escaneos")
            dialog.setMinimumSize(1200, 700)
            dialog.setStyleSheet("""
                QDialog {
                    background-color: #2C2C2C;
                }
            """)

            layout = QtWidgets.QVBoxLayout(dialog)
            layout.setContentsMargins(15, 15, 15, 15)

            # Título
            title = QtWidgets.QLabel("Historial Completo de Escaneos")
            title.setStyleSheet("""
                color: #3498db;
                font-size: 16px;
                font-weight: bold;
            """)
            layout.addWidget(title)
            
            # Panel de filtros
            filter_frame = QtWidgets.QFrame()
            filter_frame.setStyleSheet("""
                QFrame {
                    background-color: #1E1E1E;
                    border: 1px solid #3C3940;
                    border-radius: 4px;
                    padding: 10px;
                }
            """)
            filter_layout = QtWidgets.QHBoxLayout(filter_frame)
            
            # Buscador de código
            search_label = QtWidgets.QLabel("Buscar:")
            search_label.setStyleSheet("color: #ecf0f1; font-weight: bold;")
            filter_layout.addWidget(search_label)
            
            search_input = QtWidgets.QLineEdit()
            search_input.setPlaceholderText("Código completo o parcial...")
            search_input.setFixedWidth(250)
            search_input.setStyleSheet("""
                QLineEdit {
                    background-color: #2C2C2C;
                    color: #ecf0f1;
                    border: 1px solid #3C3940;
                    padding: 5px;
                    border-radius: 3px;
                }
                QLineEdit:focus {
                    border: 1px solid #3498db;
                }
            """)
            filter_layout.addWidget(search_input)
            
            filter_layout.addSpacing(20)
            
            # Fecha desde
            fecha_desde_label = QtWidgets.QLabel("Desde:")
            fecha_desde_label.setStyleSheet("color: #ecf0f1; font-weight: bold;")
            filter_layout.addWidget(fecha_desde_label)
            
            fecha_desde = QtWidgets.QDateEdit()
            today = datetime.now(ZoneInfo("America/Monterrey"))
            fecha_desde.setDate(today.date())
            fecha_desde.setCalendarPopup(True)
            fecha_desde.setStyleSheet("""
                QDateEdit {
                    background-color: #2C2C2C;
                    color: #ecf0f1;
                    border: 1px solid #3C3940;
                    padding: 5px;
                    border-radius: 3px;
                }
                QDateEdit::drop-down {
                    border: none;
                }
            """)
            filter_layout.addWidget(fecha_desde)
            
            # Fecha hasta
            fecha_hasta_label = QtWidgets.QLabel("Hasta:")
            fecha_hasta_label.setStyleSheet("color: #ecf0f1; font-weight: bold;")
            filter_layout.addWidget(fecha_hasta_label)
            
            fecha_hasta = QtWidgets.QDateEdit()
            fecha_hasta.setDate(today.date())
            fecha_hasta.setCalendarPopup(True)
            fecha_hasta.setStyleSheet("""
                QDateEdit {
                    background-color: #2C2C2C;
                    color: #ecf0f1;
                    border: 1px solid #3C3940;
                    padding: 5px;
                    border-radius: 3px;
                }
                QDateEdit::drop-down {
                    border: none;
                }
            """)
            filter_layout.addWidget(fecha_hasta)
            
            # Filtro de tipo
            tipo_combo = QtWidgets.QComboBox()
            tipo_combo.addItems(["Todos", "Solo OK", "Solo NG"])
            tipo_combo.setStyleSheet("""
                QComboBox {
                    background-color: #2C2C2C;
                    color: #ecf0f1;
                    border: 1px solid #3C3940;
                    padding: 5px;
                    border-radius: 3px;
                }
                QComboBox::drop-down {
                    border: none;
                }
                QComboBox QAbstractItemView {
                    background-color: #2C2C2C;
                    color: #ecf0f1;
                    selection-background-color: #3498db;
                }
            """)
            filter_layout.addWidget(tipo_combo)
            
            filter_layout.addStretch()
            
            # Botón filtrar
            btn_filtrar = QtWidgets.QPushButton("Filtrar")
            btn_filtrar.setFixedSize(100, 30)
            btn_filtrar.setStyleSheet("""
                QPushButton {
                    background-color: #3498db;
                    color: white;
                    border: none;
                    border-radius: 4px;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background-color: #2980b9;
                }
            """)
            filter_layout.addWidget(btn_filtrar)
            
            # Botón refrescar
            btn_refrescar = QtWidgets.QPushButton("Refrescar")
            btn_refrescar.setFixedSize(100, 30)
            btn_refrescar.setStyleSheet("""
                QPushButton {
                    background-color: #27ae60;
                    color: white;
                    border: none;
                    border-radius: 4px;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background-color: #229954;
                }
            """)
            filter_layout.addWidget(btn_refrescar)
            
            layout.addWidget(filter_frame)            # Tabla de scans
            table = QtWidgets.QTableWidget()
            table.setStyleSheet("""
                QTableWidget {
                    background-color: #1E1E1E;
                    color: #ecf0f1;
                    border: 1px solid #3C3940;
                    gridline-color: #3C3940;
                }
                QTableWidget::item {
                    padding: 8px;
                }
                QTableWidget::item:selected {
                    background-color: #3498db;
                }
                QHeaderView::section {
                    background-color: #2C2C2C;
                    color: #3498db;
                    font-weight: bold;
                    padding: 8px;
                    border: 1px solid #3C3940;
                }
            """)

            # Columnas
            table.setColumnCount(8)
            table.setHorizontalHeaderLabels(["Status", "Tipo", "Código Completo", "NParte", "Línea", "Mensaje", "Hora", "Fecha"])
            
            # Label para contador
            count_label = QtWidgets.QLabel("Cargando...")
            count_label.setStyleSheet("color: #95a5a6; font-size: 11px;")
            layout.addWidget(count_label)
            
            # Función para cargar datos
            def load_data():
                """Carga los datos de scans desde SQLite según filtros"""
                try:
                    from ..services.dual_db import get_dual_db
                    dual_db = get_dual_db()
                    
                    # Obtener fechas del filtro
                    fecha_desde_str = fecha_desde.date().toString("yyyy-MM-dd")
                    fecha_hasta_str = fecha_hasta.date().toString("yyyy-MM-dd")
                    tipo_filtro = tipo_combo.currentText()
                    search_text = search_input.text().strip()
                    
                    with dual_db._get_sqlite_connection(timeout=3.0) as conn:
                        # Construir query combinando scans exitosos y errores
                        if tipo_filtro == "Solo OK":
                            if search_text:
                                query = """
                                    SELECT raw, scan_format, nparte, linea, '' as error_msg, ts, 'OK' as status
                                    FROM scans_local
                                    WHERE DATE(ts) BETWEEN ? AND ?
                                    AND raw LIKE ?
                                    ORDER BY ts DESC
                                    LIMIT 1000
                                """
                                params = (fecha_desde_str, fecha_hasta_str, f"%{search_text}%")
                            else:
                                query = """
                                    SELECT raw, scan_format, nparte, linea, '' as error_msg, ts, 'OK' as status
                                    FROM scans_local
                                    WHERE DATE(ts) BETWEEN ? AND ?
                                    ORDER BY ts DESC
                                    LIMIT 1000
                                """
                                params = (fecha_desde_str, fecha_hasta_str)
                        elif tipo_filtro == "Solo NG":
                            if search_text:
                                query = """
                                    SELECT raw, scan_format, nparte, linea, error_message, ts, 'NG' as status
                                    FROM scan_errors
                                    WHERE DATE(fecha) BETWEEN ? AND ?
                                    AND raw LIKE ?
                                    ORDER BY ts DESC
                                    LIMIT 1000
                                """
                                params = (fecha_desde_str, fecha_hasta_str, f"%{search_text}%")
                            else:
                                query = """
                                    SELECT raw, scan_format, nparte, linea, error_message, ts, 'NG' as status
                                    FROM scan_errors
                                    WHERE DATE(fecha) BETWEEN ? AND ?
                                    ORDER BY ts DESC
                                    LIMIT 1000
                                """
                                params = (fecha_desde_str, fecha_hasta_str)
                        else:  # Todos
                            if search_text:
                                query = """
                                    SELECT raw, scan_format, nparte, linea, '' as error_msg, ts, 'OK' as status
                                    FROM scans_local
                                    WHERE DATE(ts) BETWEEN ? AND ?
                                    AND raw LIKE ?
                                    UNION ALL
                                    SELECT raw, scan_format, nparte, linea, error_message, ts, 'NG' as status
                                    FROM scan_errors
                                    WHERE DATE(fecha) BETWEEN ? AND ?
                                    AND raw LIKE ?
                                    ORDER BY ts DESC
                                    LIMIT 1000
                                """
                                params = (fecha_desde_str, fecha_hasta_str, f"%{search_text}%", 
                                         fecha_desde_str, fecha_hasta_str, f"%{search_text}%")
                            else:
                                query = """
                                    SELECT raw, scan_format, nparte, linea, '' as error_msg, ts, 'OK' as status
                                    FROM scans_local
                                    WHERE DATE(ts) BETWEEN ? AND ?
                                    UNION ALL
                                    SELECT raw, scan_format, nparte, linea, error_message, ts, 'NG' as status
                                    FROM scan_errors
                                    WHERE DATE(fecha) BETWEEN ? AND ?
                                    ORDER BY ts DESC
                                    LIMIT 1000
                                """
                                params = (fecha_desde_str, fecha_hasta_str, fecha_desde_str, fecha_hasta_str)
                        
                        cursor = conn.execute(query, params)
                        scans = cursor.fetchall()
                        
                        # Actualizar contador
                        count_label.setText(f"Total: {len(scans)} escaneos encontrados")
                        
                        # Llenar tabla
                        table.setRowCount(len(scans))
                        
                        for i, row in enumerate(scans):
                            raw, scan_format, nparte, linea, error_msg, ts, status = row
                            
                            # Status
                            status_item = QtWidgets.QTableWidgetItem(status)
                            if status == "OK":
                                status_item.setForeground(QtGui.QColor("#27ae60"))
                            else:
                                status_item.setForeground(QtGui.QColor("#e74c3c"))
                            status_item.setFont(QtGui.QFont("Arial", 10, QtGui.QFont.Weight.Bold))
                            table.setItem(i, 0, status_item)
                            
                            # Tipo
                            type_item = QtWidgets.QTableWidgetItem(scan_format or "N/A")
                            type_item.setForeground(QtGui.QColor("#3498db" if scan_format == "QR" else "#95a5a6"))
                            table.setItem(i, 1, type_item)
                            
                            # Código completo
                            code_item = QtWidgets.QTableWidgetItem(raw or "")
                            code_item.setFont(QtGui.QFont("Consolas", 9))
                            table.setItem(i, 2, code_item)
                            
                            # NParte
                            nparte_item = QtWidgets.QTableWidgetItem(nparte or "N/A")
                            table.setItem(i, 3, nparte_item)
                            
                            # Línea
                            linea_item = QtWidgets.QTableWidgetItem(linea or "N/A")
                            table.setItem(i, 4, linea_item)
                            
                            # Mensaje (solo para errores)
                            msg_item = QtWidgets.QTableWidgetItem(error_msg or "")
                            if error_msg:
                                msg_item.setForeground(QtGui.QColor("#e74c3c"))
                            table.setItem(i, 5, msg_item)
                            
                            # Hora y Fecha
                            try:
                                dt = datetime.fromisoformat(ts)
                                time_item = QtWidgets.QTableWidgetItem(dt.strftime("%H:%M:%S"))
                                date_item = QtWidgets.QTableWidgetItem(dt.strftime("%Y-%m-%d"))
                            except Exception:
                                time_item = QtWidgets.QTableWidgetItem(ts or "")
                                date_item = QtWidgets.QTableWidgetItem("")
                            
                            table.setItem(i, 6, time_item)
                            table.setItem(i, 7, date_item)
                        
                        # Ajustar columnas
                        table.setColumnWidth(0, 60)   # Status
                        table.setColumnWidth(1, 80)   # Tipo
                        table.setColumnWidth(2, 350)  # Código
                        table.setColumnWidth(3, 100)  # NParte
                        table.setColumnWidth(4, 80)   # Línea
                        table.setColumnWidth(5, 200)  # Mensaje
                        table.setColumnWidth(6, 80)   # Hora
                        table.setColumnWidth(7, 100)  # Fecha
                        
                except Exception as e:
                    logger.error(f"Error cargando historial: {e}")
                    count_label.setText(f"Error: {e}")
                    table.setRowCount(1)
                    error_item = QtWidgets.QTableWidgetItem(f"Error cargando datos: {e}")
                    error_item.setForeground(QtGui.QColor("#e74c3c"))
                    table.setItem(0, 0, error_item)
            
            # Conectar botones
            btn_filtrar.clicked.connect(load_data)
            btn_refrescar.clicked.connect(load_data)
            search_input.returnPressed.connect(load_data)  # Buscar al presionar Enter
            
            # Cargar datos iniciales
            load_data()
            
            layout.addWidget(table)
            
            # Botón cerrar
            btn_close = QtWidgets.QPushButton("Cerrar")
            btn_close.setFixedSize(100, 35)
            btn_close.setStyleSheet("""
                QPushButton {
                    background-color: #3498db;
                    color: white;
                    border: none;
                    border-radius: 4px;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background-color: #2980b9;
                }
            """)
            btn_close.clicked.connect(dialog.close)
            
            btn_layout = QtWidgets.QHBoxLayout()
            btn_layout.addStretch()
            btn_layout.addWidget(btn_close)
            layout.addLayout(btn_layout)
            
            dialog.exec()
            
        except Exception as e:
            logger.error(f"Error mostrando historial completo: {e}")
            QtWidgets.QMessageBox.warning(self, "Error", f"No se pudo abrir el historial: {e}")

    def _add_scan_to_history(self, raw: str, scan_type: str, success: bool, message: str = ""):
        """
        Agrega un scan al historial visual en formato LISTA (fila horizontal completa)
        
        Args:
            raw: Código escaneado
            scan_type: 'QR' o 'BARCODE'
            success: True si fue exitoso, False si hubo error
            message: Mensaje opcional de error
        """
        from datetime import datetime
        
        # Detectar nparte del scan
        nparte = "N/A"
        try:
            from ..services.parser import parse_scan
            parsed = parse_scan(raw)
            if hasattr(parsed, 'nparte'):
                nparte = parsed.nparte
        except Exception:
            pass

        # Guardar errores en la base de datos
        if not success:
            try:
                from ..services.dual_db import get_dual_db
                from datetime import datetime
                from zoneinfo import ZoneInfo

                dual_db = get_dual_db()
                now = datetime.now(ZoneInfo("America/Monterrey"))
                ts = now.isoformat()
                fecha = now.strftime("%Y-%m-%d")
                
                # Detectar código de error del mensaje
                error_code = -1
                if "QR+QR" in message or "DUPLICADO QR" in message:
                    error_code = -8
                elif "BC+BC" in message or "DUPLICADO BARCODE" in message:
                    error_code = -9
                elif "MODELO DIFERENTE" in message:
                    error_code = -10
                elif "SIN PLAN ACTIVO" in message:
                    error_code = -11
                
                # Obtener línea actual desde el selector (SIEMPRE tiene un valor)
                linea = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else "N/A"
                logger.debug(f"💾 Guardando error NG en línea: {linea}")
                
                # Guardar en SQLite local
                with dual_db._get_sqlite_connection(timeout=2.0) as conn:
                    conn.execute("""
                        INSERT INTO scan_errors (raw, nparte, linea, scan_format, error_code, error_message, ts, fecha)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (raw, nparte if nparte != "N/A" else None, linea, scan_type, error_code, message, ts, fecha))
                    conn.commit()
                
                # Enviar también a MySQL (asíncrono, no bloquea UI)
                import threading
                def send_to_mysql():
                    try:
                        dual_db.insert_error_to_mysql(
                            raw=raw,
                            nparte=nparte if nparte != "N/A" else None,
                            linea=linea,
                            scan_format=scan_type,
                            error_code=error_code,
                            error_message=message,
                            ts=ts
                        )
                    except Exception as e:
                        logger.error(f"Error enviando NG a MySQL: {e}")
                
                threading.Thread(target=send_to_mysql, daemon=True).start()
                
                logger.debug(f"Error guardado en BD local y enviado a MySQL: {scan_type} - {message}")
            except Exception as e:
                logger.error(f"Error guardando error en BD: {e}")

        # Crear widget de scan en formato FILA (lista horizontal)
        scan_widget = QtWidgets.QFrame()
        scan_widget.setMinimumHeight(40)
        scan_widget.setMaximumHeight(40)
        
        # Color según resultado
        if success:
            border_color = "#27ae60" if scan_type == "QR" else "#3498db"
            bg_color = "#2d5016" if scan_type == "QR" else "#1a4d6d"
            status_icon = ""
        else:
            border_color = "#e74c3c"
            bg_color = "#4d1a1a"
            status_icon = ""
        
        scan_widget.setStyleSheet(f"""
            QFrame {{
                background-color: {bg_color};
                border-left: 4px solid {border_color};
                border-radius: 2px;
                padding: 4px 8px;
            }}
            QFrame:hover {{
                background-color: {bg_color}DD;
                border-left: 4px solid {border_color}FF;
            }}
        """)
        
        # Layout HORIZONTAL para mostrar info en fila
        layout = QtWidgets.QHBoxLayout(scan_widget)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(12)

        # 1. Status OK/NG
        status_text = "OK" if success else "NG"
        status_color = "#27ae60" if success else "#e74c3c"
        status_label = QtWidgets.QLabel(status_text)
        status_label.setStyleSheet(f"""
            color: {status_color};
            font-size: 13px;
            font-weight: bold;
            background: transparent;
        """)
        status_label.setFixedWidth(35)
        status_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(status_label)

        # 2. Tipo de scan
        type_label = QtWidgets.QLabel(scan_type)
        type_label.setStyleSheet(f"""
            color: {border_color};
            font-size: 10px;
            font-weight: bold;
            background: transparent;
        """)
        type_label.setFixedWidth(70)
        layout.addWidget(type_label)

        # 3. Código completo (sin truncar, scrolleable)
        code_label = QtWidgets.QLabel(raw)
        code_label.setStyleSheet("""
            color: #ecf0f1;
            font-size: 9px;
            font-family: 'Consolas', 'Courier New', monospace;
            background: transparent;
        """)
        code_label.setMinimumWidth(350)
        code_label.setMaximumWidth(600)
        code_label.setWordWrap(False)
        code_label.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextSelectableByMouse)
        code_label.setToolTip(f"Código completo (seleccionable): {raw}")
        layout.addWidget(code_label)

        # 4. NParte
        nparte_label = QtWidgets.QLabel(f"Parte: {nparte}")
        nparte_label.setStyleSheet("""
            color: #95a5a6;
            font-size: 9px;
            background: transparent;
        """)
        nparte_label.setFixedWidth(120)
        nparte_label.setToolTip(nparte)
        layout.addWidget(nparte_label)

        # 5. Mensaje (si hay)
        if message:
            msg_label = QtWidgets.QLabel(message)
            msg_label.setStyleSheet(f"""
                color: {'#f39c12' if success else '#e74c3c'};
                font-size: 9px;
                font-style: italic;
                background: transparent;
            """)
            msg_label.setWordWrap(False)
            layout.addWidget(msg_label)
            layout.addStretch(1)
        else:
            layout.addStretch(1)

        # 6. Timestamp (al final)
        from zoneinfo import ZoneInfo
        now_local = datetime.now(ZoneInfo("America/Monterrey"))
        timestamp = now_local.strftime("%H:%M:%S")
        time_label = QtWidgets.QLabel(timestamp)
        time_label.setStyleSheet("""
            color: #7f8c8d;
            font-size: 9px;
            background: transparent;
        """)
        time_label.setFixedWidth(60)
        time_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight | QtCore.Qt.AlignmentFlag.AlignVCenter)
        layout.addWidget(time_label)        # Agregar al historial (máximo 20, más reciente PRIMERO - arriba)
        self.scan_history.insert(0, scan_widget)  # Insertar al inicio
        if len(self.scan_history) > 20:
            # Eliminar el más antiguo (último de la lista)
            old_widget = self.scan_history.pop()
            self.scan_history_layout.removeWidget(old_widget)
            old_widget.deleteLater()
        
        # Ocultar mensaje "Esperando escaneos..." si existe
        if hasattr(self, 'scan_history_empty_label') and self.scan_history_empty_label:
            self.scan_history_empty_label.setVisible(False)
        
        # Insertar al INICIO del layout (arriba) para que más reciente aparezca primero
        self.scan_history_layout.insertWidget(0, scan_widget)
    def _create_metric_label(self, title, value, value_color="#ecf0f1"):
        """Crea un label de mí©trica con tí­tulo y valor (compacto)"""
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
        value_label.setObjectName(f"{title}_value")  # Para poder actualizar despuí©s
        layout.addWidget(value_label)
        
        return container
    
    def _update_cards_with_metrics(self, plan: int, plan_acum: int, produccion: int, 
                                    eficiencia: float, uph: float, upph: float,
                                    M_trans_total: float = 0, M_tot_total: float = 0):
        """
        Actualiza las cards con mí©tricas calculadas
        Mí©todo helper para evitar duplicacií³n de cí³digo
        
        Args:
            M_trans_total: Minutos transcurridos totales (opcional)
            M_tot_total: Minutos totales efectivos (opcional)
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
            
            # Actualizar tarjeta Produccií³n
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
                progress_value = int(eficiencia * 10)  # x10 para precisií³n
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
            
            # â±ï¸ Actualizar label de tiempo transcurrido
            if hasattr(self, 'tiempo_transcurrido_label') and self.tiempo_transcurrido_label:
                import datetime
                from zoneinfo import ZoneInfo
                monterrey_tz = ZoneInfo("America/Monterrey")
                ahora = datetime.datetime.now(monterrey_tz)
                
                if M_tot_total > 0:
                    porcentaje = (M_trans_total / M_tot_total) * 100
                    tiempo_texto = f"📊 Tiempo transcurrido: {M_trans_total:.0f} / {M_tot_total:.0f} min ({porcentaje:.1f}%)"
                    color = "#27ae60"  # Verde
                else:
                    tiempo_texto = f"📉 Hora actual: {ahora.strftime('%H:%M')}"
                    color = "#95a5a6"  # Gris
                
                self.tiempo_transcurrido_label.setText(tiempo_texto)
                self.tiempo_transcurrido_label.setStyleSheet(f"""
                    color: {color};
                    font-size: 10px;
                    font-weight: 600;
                    background: transparent;
                    padding: 2px 0px;
                """)
                self.tiempo_transcurrido_label.repaint()
            
            logger.debug(f"… Cards actualizadas: Plan={plan}, Prod={produccion}, Efic={eficiencia:.1f}%")
            
        except Exception as e:
            logger.error(f"âŒ Error actualizando cards: {e}")
    
    def _update_plan_totals(self, plan_rows):
        """Actualiza las tarjetas con mí©tricas de la lí­nea
        
        ï¿½ OPTIMIZACIí“N: Lee mí©tricas desde cachí© SQLite (ultra-rápido, sin bloqueos)
        El worker en background actualiza el cachí© cada 3 segundos desde MySQL
        
        ï¿½ðŸ“ Fí“RMULAS CANí“NICAS (Single Source of Truth):
        
        1. m_eff_total = Minutos efectivos del turno (duracií³n - breaks del turno completo)
        2. m_eff_trans = Minutos efectivos transcurridos (tiempo corrido - breaks ya ocurridos)
        3. plan_acum = plan_total í— (m_eff_trans / m_eff_total)
        4. eficiencia = (prod_real / plan_acum) í— 100  [si plan_acum > 0]
        
        âš ï¸ IMPORTANTE: UPH (íºltima hora) NO interviene en eficiencia acumulada.
                      La eficiencia SOLO depende del tiempo efectivo transcurrido.
        
        Ejemplo (turno de 8h, 1 break de 30min):
        - m_eff_total = 480 - 30 = 450 min
        - A las 4h transcurridas con 15min de break: m_eff_trans = 240 - 15 = 225 min
        - plan_total = 1000 piezas
        - plan_acum = 1000 í— (225/450) = 500 piezas
        - prod_real = 520 piezas
        - eficiencia = (520 / 500) í— 100 = 104%
        """
        try:
            # Verificar que las tarjetas existan
            if not hasattr(self, 'card_plan') or not self.card_plan:
                return
            
            # … LEER DIRECTAMENTE DE SQLITE (SIN CACHí‰ - DATOS SIEMPRE FRESCOS)
            from ..services.dual_db import get_dual_db
            import datetime
            from zoneinfo import ZoneInfo
            
            # Calcular fecha/hora actual al inicio
            monterrey_tz = ZoneInfo("America/Monterrey")
            ahora = datetime.datetime.now(monterrey_tz)
            today = ahora.date().isoformat()
            
            dual_db = get_dual_db()
            linea_seleccionada = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else ''
            
            # Leer datos directos de SQLite
            with dual_db._get_sqlite_connection(timeout=1.0) as conn:
                cursor = conn.cursor()
                
                # Plan total (TODOS los dí­as - para referencia)
                cursor.execute("""
                    SELECT SUM(plan_count), SUM(produced_count)
                    FROM plan_local
                    WHERE line = ?
                """, (linea_seleccionada,))
                
                result = cursor.fetchone()
                plan_total_todos = result[0] or 0
                produccion_total_todos = result[1] or 0
                
                # Plan del dí­a actual (para Plan Acumulado y tiempo transcurrido)
                # Filtrar por working_date en lugar de planned_start
                cursor.execute("""
                    SELECT SUM(plan_count), SUM(produced_count)
                    FROM plan_local
                    WHERE line = ?
                    AND working_date = ?
                """, (linea_seleccionada, today))
                
                result_hoy = cursor.fetchone()
                plan_total = result_hoy[0] or 0
                produccion_total = result_hoy[1] or 0
                
                # â±ï¸ Obtener datos de planes para calcular tiempo transcurrido
                # Solo considerar planes del dí­a actual para el tiempo transcurrido
                cursor.execute("""
                    SELECT planned_start, planned_end, effective_minutes
                    FROM plan_local
                    WHERE line = ?
                    AND working_date = ?
                """, (linea_seleccionada, today))
                
                plan_rows_time = cursor.fetchall()
            
            # â±ï¸ CALCULAR TIEMPO TRANSCURRIDO
            
            M_trans_total = 0
            M_tot_total = 0
            
            BREAKS = [
                (9, 30, 15),   # 09:30-09:45
                (12, 0, 30),   # 12:00-12:30 (comida)
                (15, 0, 15),   # 15:00-15:15
            ]
            
            for row in plan_rows_time:
                planned_start = row[0]
                planned_end = row[1]
                effective_minutes = row[2] or 0
                
                # Calcular M_trans y M_tot para este plan
                if not planned_start or not planned_end or planned_start == 'N/A' or planned_end == 'N/A':
                    # Sin fechas, asumir 50% del tiempo
                    M_tot = effective_minutes
                    M_trans = M_tot // 2
                else:
                    try:
                        if isinstance(planned_start, str):
                            planned_start = datetime.datetime.fromisoformat(planned_start).replace(tzinfo=monterrey_tz)
                        if isinstance(planned_end, str):
                            planned_end = datetime.datetime.fromisoformat(planned_end).replace(tzinfo=monterrey_tz)
                        
                        ahora_dt = ahora
                        M_tot = effective_minutes
                        
                        # Si no ha empezado
                        if ahora_dt < planned_start:
                            M_trans = 0
                        # Si ya terminí³
                        elif ahora_dt >= planned_end:
                            M_trans = M_tot
                        # Durante el plan
                        else:
                            # Calcular minutos transcurridos
                            t_ini = planned_start
                            t_fin = min(ahora_dt, planned_end)
                            m_raw = max(0, int((t_fin - t_ini).total_seconds() / 60))
                            
                            # Deducir breaks
                            m_brk = 0
                            for hora_break, minuto_break, duracion_break in BREAKS:
                                inicio_break = t_ini.replace(hour=hora_break, minute=minuto_break, second=0, microsecond=0)
                                fin_break = inicio_break + datetime.timedelta(minutes=duracion_break)
                                
                                overlap_start = max(inicio_break, t_ini)
                                overlap_end = min(fin_break, t_fin)
                                
                                if overlap_start < overlap_end:
                                    overlap_minutos = int((overlap_end - overlap_start).total_seconds() / 60)
                                    m_brk += overlap_minutos
                            
                            M_trans = min(max(0, m_raw - m_brk), M_tot)
                    except Exception as e:
                        logger.error(f"Error calculando tiempo: {e}")
                        M_tot = effective_minutes
                        M_trans = M_tot // 2
                
                M_trans_total += M_trans
                M_tot_total += M_tot
            
            # ðŸ“Š CALCULAR PLAN ACUMULADO basado en tiempo transcurrido
            if M_tot_total > 0:
                fraccion_transcurrida = M_trans_total / M_tot_total
                plan_acumulado = round(plan_total * fraccion_transcurrida)
            else:
                # Si no hay tiempo configurado, usar el plan total
                plan_acumulado = plan_total
            
            # ðŸŽ¯ CALCULAR EFICIENCIA BASADA EN PLAN ACUMULADO (tiempo transcurrido)
            # Eficiencia = (Produccií³n Real / Plan Acumulado) í— 100
            # - 100% = Van al ritmo esperado
            # - >100% = Van adelantados
            # - <100% = Van atrasados
            if plan_acumulado > 0:
                eficiencia = (produccion_total / plan_acumulado) * 100
            else:
                # Si el plan no ha empezado (plan_acumulado = 0), no hay eficiencia aíºn
                eficiencia = 0.0
            
            # Actualizar cards con datos reales de SQLite (incluyendo tiempo)
            self._update_cards_with_metrics(
                plan=plan_total,
                plan_acum=plan_acumulado,
                produccion=produccion_total,
                eficiencia=eficiencia,
                uph=0,
                upph=0.0,
                M_trans_total=M_trans_total,
                M_tot_total=M_tot_total
            )
            
            logger.debug(f"… _update_plan_totals desde SQLite: Plan={plan_total}, Prod={produccion_total}, Efic={eficiencia:.1f}%")
            return
            
            # ===== Cí“DIGO VIEJO DEL CACHí‰ (DESHABILITADO) =====
            from ..services.metrics_cache import get_metrics_cache
            from datetime import date
            
            fecha_hoy = date.today().isoformat()
            
            metrics_cache = get_metrics_cache()
            cached_metrics = None
            
            if metrics_cache:
                cached_metrics = metrics_cache.get_metrics_from_cache(linea_seleccionada, fecha_hoy)
            
            # Si hay mí©tricas en cachí©, usarlas (ultra-rápido)
            if cached_metrics:
                print(f"\n{'='*80}")
                print(f"📊 USANDO CACHÉ - Calculando tiempo transcurrido...")
                print(f"{'='*80}\n")

                logger.debug(f" Usando métricas desde cache para {linea_seleccionada}")

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
                
                # â±ï¸ CALCULAR TIEMPO TRANSCURRIDO (incluso con cachí©)
                import datetime
                from zoneinfo import ZoneInfo
                monterrey_tz = ZoneInfo("America/Monterrey")
                ahora = datetime.datetime.now(monterrey_tz)
                
                # Obtener M_trans_total y M_tot_total de los planes
                M_trans_total = 0
                M_tot_total = 0
                
                if plan_rows:
                    BREAKS = [
                        (9, 30, 15),   # 09:30-09:45
                        (12, 0, 30),   # 12:00-12:30 (comida)
                        (15, 0, 15),   # 15:00-15:15
                    ]
                    
                    for plan_dict in plan_rows:
                        planned_start = plan_dict.get('planned_start')
                        planned_end = plan_dict.get('planned_end')
                        effective_minutes = plan_dict.get('effective_minutes', 0)
                        
                        # Calcular M_trans y M_tot para este plan (versií³n simplificada)
                        if not planned_start or not planned_end or planned_start == 'N/A' or planned_end == 'N/A':
                            # Sin fechas, asumir 50% del tiempo
                            M_tot = effective_minutes or 0
                            M_trans = M_tot // 2
                        else:
                            try:
                                if isinstance(planned_start, str):
                                    planned_start = datetime.datetime.fromisoformat(planned_start).replace(tzinfo=monterrey_tz)
                                if isinstance(planned_end, str):
                                    planned_end = datetime.datetime.fromisoformat(planned_end).replace(tzinfo=monterrey_tz)
                                
                                ahora_dt = ahora
                                
                                # Calcular M_tot (minutos efectivos totales con breaks deducidos)
                                M_tot = effective_minutes or 0
                                
                                # Si no ha empezado
                                if ahora_dt < planned_start:
                                    M_trans = 0
                                # Si ya terminí³
                                elif ahora_dt >= planned_end:
                                    M_trans = M_tot
                                # Durante el plan
                                else:
                                    # Calcular minutos transcurridos desde planned_start
                                    t_ini = planned_start
                                    t_fin = min(ahora_dt, planned_end)
                                    m_raw = max(0, int((t_fin - t_ini).total_seconds() / 60))
                                    
                                    # Deducir breaks
                                    m_brk = 0
                                    for hora_break, minuto_break, duracion_break in BREAKS:
                                        inicio_break = t_ini.replace(hour=hora_break, minute=minuto_break, second=0, microsecond=0)
                                        fin_break = inicio_break + datetime.timedelta(minutes=duracion_break)
                                        
                                        overlap_start = max(inicio_break, t_ini)
                                        overlap_end = min(fin_break, t_fin)
                                        
                                        if overlap_start < overlap_end:
                                            overlap_minutos = int((overlap_end - overlap_start).total_seconds() / 60)
                                            m_brk += overlap_minutos
                                    
                                    M_trans = min(max(0, m_raw - m_brk), M_tot)
                            except Exception as e:
                                logger.error(f"Error calculando tiempo: {e}")
                                M_tot = effective_minutes or 0
                                M_trans = M_tot // 2
                        
                        M_trans_total += M_trans
                        M_tot_total += M_tot
                
                print(f"\n{'â±ï¸'*40}")
                print(f"RESULTADO CíLCULO TIEMPO (CACHí‰):")
                print(f"  M_trans_total = {M_trans_total}")
                print(f"  M_tot_total = {M_tot_total}")
                print(f"  plan_rows count = {len(plan_rows) if plan_rows else 0}")
                print(f"{'â±ï¸'*40}\n")
                
                # Actualizar label de tiempo transcurrido
                if hasattr(self, 'tiempo_transcurrido_label') and self.tiempo_transcurrido_label:
                    print(f"  … Label existe, actualizando...")
                    if M_tot_total > 0:
                        porcentaje = (M_trans_total / M_tot_total) * 100
                        tiempo_texto = f"✓ Tiempo transcurrido: {M_trans_total:.0f} / {M_tot_total:.0f} min ({porcentaje:.1f}%)"
                        color = "#27ae60"  # Verde
                        print(f"  📊 Texto: {tiempo_texto}")
                    else:
                        tiempo_texto = f"✓ Hora actual: {ahora.strftime('%H:%M')}"
                        color = "#95a5a6"  # Gris
                        print(f"  📉 M_tot_total=0, mostrando hora: {tiempo_texto}")

                    self.tiempo_transcurrido_label.setText(tiempo_texto)
                    self.tiempo_transcurrido_label.setStyleSheet(f"""
                        color: {color};
                        font-size: 10px;
                        font-weight: 600;
                        background: transparent;
                        padding: 2px 0px;
                    """)
                else:
                    print(f"  📉 Label NO existe o no está disponible")

                logger.debug(f"… Cards actualizadas desde cache: Eficiencia={eficiencia:.1f}%")
                return

            # Si no hay cache, calcular métricas tradicional (fallback)
            logger.debug(f"📉 Sin cache, calculando métricas tradicional")

            # ========== IMPORTS Y CONSTANTES (SIEMPRE) ==========
            import datetime
            from zoneinfo import ZoneInfo
            
            monterrey_tz = ZoneInfo("America/Monterrey")
            ahora = datetime.datetime.now(monterrey_tz)
            
            # Variables para tiempo transcurrido (inicializar aquí­ para que estí©n en scope)
            M_trans_total = 0
            M_tot_total = 0
            
            # Si no hay planes, mostrar ceros y mensaje
            if not plan_rows:
                plan = resultado = produccion_acumulada = eficiencia = uph = upph = 0
                logger.info("ðŸ“‹ Sin plan cargado - mostrando valores en cero")
            else:
                # ========== BREAKS ESTíNDAR ==========
                # Breaks estándar (hora, minuto, duracií³n_minutos)
                BREAKS = [
                    (9, 30, 15),   # 09:30-09:45
                    (12, 0, 30),   # 12:00-12:30 (comida)
                    (15, 0, 15),   # 15:00-15:15
                ]
                
                # ========== CALCULAR PLAN TOTAL DE LA LíNEA ==========
                plan_total_linea = sum(r.get('plan_count', 0) or 0 for r in plan_rows)
                logger.info(f"ðŸ” DEBUG: Níºmero de planes en lí­nea: {len(plan_rows)}")
                for idx, r in enumerate(plan_rows, 1):
                    logger.info(f"   Plan {idx}: part_no={r.get('part_no')}, plan_count={r.get('plan_count')}, produced={r.get('produced_count')}, status={r.get('status')}")
                logger.info(f"ðŸ” DEBUG: Plan total lí­nea (suma de plan_count): {plan_total_linea}")
                
                # ========== FUNCIí“N: CALCULAR MINUTOS EFECTIVOS ==========
                def calcular_minutos_efectivos_plan(plan_dict, ahora_dt):
                    """
                    Retorna (m_eff_trans, m_eff_total) para un plan.
                    
                    m_eff_trans = minutos efectivos transcurridos (con breaks descontados)
                    m_eff_total = minutos efectivos totales del plan (con breaks descontados)
                    
                    Lí³gica:
                    1. Obtener M_tot de effective_minutes (o calcular desde fechas, o default 450min)
                    2. Si no hay fechas â†’ asumir 50% transcurrido
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
                            logger.debug(f"ðŸ“Š Plan {plan_dict.get('part_no')}: calculado M_tot={M_tot} min desde fechas")
                        except Exception as e:
                            logger.warning(f"âš ï¸ Error calculando M_tot desde fechas para {plan_dict.get('part_no')}: {e}")
                            M_tot = 0
                    
                    # FALLBACK 2: Si AíšN no hay M_tot, usar valor por defecto (450 min = 7.5h)
                    if M_tot == 0:
                        M_tot = 450  # Valor por defecto: 7.5 horas efectivas
                        logger.debug(f"ðŸ“Š Plan {plan_dict.get('part_no')}: usando M_tot por defecto={M_tot} min")
                    
                    # Si no hay fechas de plan, asumir que estamos a mitad del tiempo
                    if not planned_start_str or not planned_end_str:
                        # Sin fechas, asumir que estamos al 50% del tiempo
                        M_trans = M_tot // 2
                        logger.debug(f"ðŸ“Š Plan {plan_dict.get('part_no')}: sin fechas, asumiendo 50% transcurrido (M_trans={M_trans})")
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
                        
                        # Si aíºn no ha empezado el plan, M_trans = 0
                        if ahora_dt < planned_start:
                            return (0, M_tot)
                        
                        # Si ya terminí³ el plan, M_trans = M_tot
                        if ahora_dt >= planned_end:
                            return (M_tot, M_tot)
                        
                        # ========== CALCULAR m_raw (minutos brutos transcurridos) ==========
                        m_raw = max(0, int((t_fin - t_ini).total_seconds() / 60))
                        
                        # ========== CALCULAR m_brk (minutos de breaks en la ventana [t_ini, t_fin]) ==========
                        m_brk = 0
                        
                        for hora_break, minuto_break, duracion_break in BREAKS:
                            # Crear datetime del break usando el mismo dí­a que t_ini
                            inicio_break = t_ini.replace(hour=hora_break, minute=minuto_break, second=0, microsecond=0)
                            fin_break = inicio_break + datetime.timedelta(minutes=duracion_break)
                            
                            # Calcular overlap entre [inicio_break, fin_break] y [t_ini, t_fin]
                            overlap_start = max(inicio_break, t_ini)
                            overlap_end = min(fin_break, t_fin)
                            
                            if overlap_start < overlap_end:
                                # Hay overlap
                                overlap_minutos = int((overlap_end - overlap_start).total_seconds() / 60)
                                m_brk += overlap_minutos
                                logger.debug(f"ðŸš« Break {hora_break:02d}:{minuto_break:02d} overlap: {overlap_minutos} min")
                        
                        # ========== CALCULAR m_eff_trans (minutos efectivos transcurridos) ==========
                        m_eff_trans = min(max(0, m_raw - m_brk), M_tot)
                        
                        logger.debug(f"ðŸ“Š Plan {plan_dict.get('part_no')}: m_raw={m_raw}, m_brk={m_brk}, m_eff_trans={m_eff_trans}, M_tot={M_tot}")
                        
                        return (m_eff_trans, M_tot)
                        
                    except Exception as e:
                        logger.error(f"âŒ Error calculando M_trans para plan {plan_dict.get('part_no')}: {e}")
                        # En caso de error, asumir 50% transcurrido
                        return (M_tot // 2, M_tot)
                
                # ========== FUNCIí“N AUXILIAR: CALCULAR PLAN ACUMULADO POR PLAN ==========
                def calcular_plan_acumulado_plan(plan_dict, M_trans, M_tot):
                    """
                    Calcula plan_acum_plan = round(plan_count í— (M_trans / M_tot))
                    
                    Args:
                        plan_dict: Diccionario con datos del plan (plan_count)
                        M_trans: Minutos efectivos transcurridos
                        M_tot: Minutos efectivos totales
                    
                    Returns:
                        int: Plan acumulado para este plan especí­fico
                    """
                    plan_count = plan_dict.get('plan_count', 0) or 0
                    
                    if M_tot == 0 or plan_count == 0:
                        return 0
                    
                    # f = M_trans / M_tot (fraccií³n de avance del tiempo)
                    f = M_trans / M_tot
                    
                    # plan_acum_plan = round(plan_count í— f)
                    plan_acum_plan = round(plan_count * f)
                    
                    return plan_acum_plan
                
                # ========== FUNCIí“N AUXILIAR: CALCULAR EFICIENCIA POR PLAN ==========
                def calcular_eficiencia_plan(produced_count, plan_acum_plan):
                    """
                    Calcula Efic% = (produced_count / plan_acum_plan) í— 100
                    
                    Args:
                        produced_count: Produccií³n real del plan
                        plan_acum_plan: Plan acumulado del plan
                    
                    Returns:
                        float: Eficiencia en porcentaje
                    """
                    if plan_acum_plan == 0:
                        return 0.0
                    
                    eficiencia = (produced_count / plan_acum_plan) * 100
                    return eficiencia
                
                # ========== NUEVAS Fí“RMULAS BASADAS EN PLAN_MAIN ==========
                # Ahora trabajamos con cada plan individual y luego sumamos a nivel lí­nea
                
                logger.info(f"â±ï¸ Hora actual: {ahora.strftime('%H:%M:%S')}")
                
                # Variables acumuladas a nivel lí­nea
                Plan_acum_linea = 0
                Prod_acum_linea = 0
                # M_trans_total y M_tot_total ya están inicializadas arriba
                
                # Procesar TODOS los planes de la lí­nea (no solo el "en progreso")
                for plan_dict in plan_rows:
                    part_no = plan_dict.get('part_no', '')
                    plan_count = plan_dict.get('plan_count', 0) or 0
                    produced_count = plan_dict.get('produced_count', 0) or 0
                    planned_start = plan_dict.get('planned_start', 'N/A')
                    planned_end = plan_dict.get('planned_end', 'N/A')
                    effective_minutes = plan_dict.get('effective_minutes', 0)
                    
                    # Calcular M_trans y M_tot para este plan
                    M_trans, M_tot = calcular_minutos_efectivos_plan(plan_dict, ahora)
                    
                    # Acumular tiempos para mostrar en el label
                    M_trans_total += M_trans
                    M_tot_total += M_tot
                    
                    # Calcular plan_acum_plan para este plan
                    plan_acum_plan = calcular_plan_acumulado_plan(plan_dict, M_trans, M_tot)
                    
                    # Acumular a nivel lí­nea
                    Plan_acum_linea += plan_acum_plan
                    Prod_acum_linea += produced_count
                    
                    logger.warning(f"Plan '{part_no}': plan_count={plan_count}, M_trans={M_trans}min, M_tot={M_tot}min, plan_acum={plan_acum_plan}, produced={produced_count}, start={planned_start}, end={planned_end}, eff_min={effective_minutes}, status={plan_dict.get('status')}")
                    
                    # DEBUG: Print visible output to console
                    print(f"\n{'='*80}")
                    print(f"DEBUG PLAN ACUMULADO - Plan '{part_no}'")
                    print(f"{'='*80}")
                    print(f"  plan_count (total):      {plan_count}")
                    print(f"  produced_count (actual): {produced_count}")
                    print(f"  planned_start:           {planned_start}")
                    print(f"  planned_end:             {planned_end}")
                    print(f"  effective_minutes:       {effective_minutes}")
                    print(f"  M_trans (min transcur):  {M_trans} min")
                    print(f"  M_tot (min totales):     {M_tot} min")
                    print(f"  Fraccion transcurrida:   {M_trans/M_tot if M_tot > 0 else 0:.2%}")
                    print(f"  plan_acum (esperado):    {plan_acum_plan}")
                    print(f"  status:                  {plan_dict.get('status')}")
                    print(f"{'='*80}\n")
                
                # Plan total ya se calculí³ arriba (plan_total_linea)
                plan = plan_total_linea
                
                # Resultado = Plan acumulado a nivel lí­nea
                resultado = Plan_acum_linea
                plan_acumulado = Plan_acum_linea
                
                logger.info(f"ðŸ” DEBUG FINAL: Plan_acum_linea (suma) = {Plan_acum_linea}, Prod_acum_linea (suma) = {Prod_acum_linea}")
                
                # Produccií³n acumulada = Suma de produced_count de todos los planes
                produccion_acumulada = Prod_acum_linea
                
                # Eficiencia a nivel lí­nea = (Prod_acum_linea / Plan_acum_linea) í— 100
                if Plan_acum_linea > 0:
                    eficiencia = (Prod_acum_linea / Plan_acum_linea) * 100
                    # Limitar a 999.9% para evitar valores absurdos
                    if eficiencia > 999.9:
                        logger.warning(f"âš ï¸ Eficiencia muy alta ({eficiencia:.1f}%), limitando a 999.9%")
                        eficiencia = 999.9
                else:
                    eficiencia = 0.0
                
                logger.info(f"ðŸ“Š LíNEA {linea_seleccionada}: Plan_total={plan_total_linea}, Plan_acum={Plan_acum_linea}, Prod_acum={Prod_acum_linea}, Efic={eficiencia:.1f}%")
                
                # ========== CíLCULO DE UPH (mantener lí³gica anterior) ==========
                uph = 0
                upph = 0
                
                # UPH se mantiene desde produccií³n real en ventana de 60 min
                # Este cálculo NO cambia con las nuevas fí³rmulas
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
                    logger.debug(f" UPH desde cache: {uph:.1f}")
                else:
                    try:
                        #  OPTIMIZADO: Calcular UPH desde SQLite local (no bloquea)
                        from ..services.dual_db import get_dual_db
                        dual_db = get_dual_db()
                        
                        # Obtener UPH de íºltimos 60 minutos desde SQLite (ultra-rápido)
                        t1 = ahora
                        t0 = t1 - datetime.timedelta(minutes=60)
                        
                        # Usar mí©todo optimizado del dual_db (lee de SQLite local)
                        # Usar fecha actual para filtro
                        fecha_hoy = ahora.strftime("%Y-%m-%d")
                        
                        try:
                            with dual_db._get_sqlite_connection(timeout=0.5) as conn:
                                # ✅ CORREGIDO: Usar datetime() de SQLite para comparación correcta
                                cursor = conn.execute("""
                                    SELECT COUNT(*)/2 as N
                                    FROM scans_local 
                                    WHERE linea = ? 
                                    AND fecha = ?
                                    AND datetime(ts) >= datetime('now', '-1 hour')
                                    AND is_complete = 1
                                """, (linea_seleccionada, fecha_hoy))
                                result_row = cursor.fetchone()
                                N = int(result_row[0]) if result_row and result_row[0] else 0
                                
                                # UPH = piezas completas en 60 min
                                uph = N
                                
                                # Guardar en cache por 5 segundos
                                self._uph_cache[cache_key] = uph
                                self._uph_cache_time[cache_time_key] = current_time
                                
                                logger.debug(f" UPH calculado desde SQLite: {uph} piezas")
                        except Exception as e_sqlite:
                            logger.debug(f"âš ï¸ Error calculando UPH desde SQLite: {e_sqlite}")
                            uph = 0
                    except Exception as e:
                        logger.error(f"âŒ Error calculando UPH: {e}")
                        uph = 0
                
                logger.info(f"ðŸ“Š Resumen final: Plan={plan} | Plan_acum={plan_acumulado} | Prod={produccion_acumulada} | Efic={eficiencia:.1f}% | UPH={uph}")
                
                # ========== OBTENER NíšMERO DE PERSONAS DESDE TABLA RAW DE MYSQL ==========
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
                
                if nparte:  # Si tenemos níºmero de parte
                    try:
                        #  OPTIMIZADO: Usar cachí© para personas (evita consulta MySQL bloqueante)
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
                            logger.debug(f"ðŸ‘¥ Personas desde cache: {num_personas}")
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
                                            logger.debug(f"ðŸ‘¥ Personas obtenidas de MySQL para {nparte}: {num_personas}")
                                        else:
                                            num_personas = 6
                                    else:
                                        num_personas = 6
                            
                    except Exception as e:
                        logger.debug(f"âš ï¸ Error obteniendo personas: {e}")
                        from ..config import settings as _settings
                        num_personas = getattr(_settings, 'NUM_PERSONAS_LINEA', 6)
                else:
                    # Si no hay nparte, usar valor por defecto
                    from ..config import settings as _settings
                    num_personas = getattr(_settings, 'NUM_PERSONAS_LINEA', 6)
                    logger.info(f"ðŸ‘¥ Sin part_no, usando personas por defecto: {num_personas}")
                
                # ========== CíLCULO DE UPPH ==========
                # UPPH = UPH / níºmero_personas
                upph = (uph / num_personas) if num_personas > 0 and uph > 0 else 0
                
                logger.info(f"ðŸ“Š Mí©tricas finales: CT={num_personas} personas | UPH={uph:.1f} | UPPH={upph:.2f}")
            
            #  Actualizar TARJETAS (verificar que existan)
            if hasattr(self.card_plan, 'value_label') and self.card_plan.value_label:
                # PLAN TOTAL = Meta del dí­a completo (suma de todos los modelos de la lí­nea)
                if 'plan_total_linea' in locals() and plan_total_linea > 0:
                    self.card_plan.value_label.setText(f"{int(plan_total_linea)}")
                else:
                    self.card_plan.value_label.setText(str(plan))
            
            if hasattr(self.card_resultado, 'value_label') and self.card_resultado.value_label:
                # PLAN ACUMULADO = Cuántas piezas DEBERíAN llevar desde el inicio del turno
                if 'plan_acumulado' in locals() and plan_acumulado > 0:
                    self.card_resultado.value_label.setText(f"{int(plan_acumulado)}")
                else:
                    self.card_resultado.value_label.setText("0")
            
            if hasattr(self.card_produccion, 'value_label') and self.card_produccion.value_label:
                # PRODUCCIí“N = Cuántas piezas REALMENTE han producido desde el inicio del turno
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
            
            #  Actualizar BARRA DE PROGRESO
            if hasattr(self, 'general_progress_bar') and self.general_progress_bar:
                progress_value = int(eficiencia * 10)  # x10 para precisií³n
                self.general_progress_bar.setValue(progress_value)
                
                # Cambiar color segíºn progreso (estilo control_bom)
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

            # 📊 Actualizar INDICADOR DE TIEMPO TRANSCURRIDO
            if hasattr(self, 'tiempo_transcurrido_label') and self.tiempo_transcurrido_label:
                # DEBUG: Ver valores antes de actualizar
                print(f"\n{'📊'*40}")
                print(f"DEBUG TIEMPO TRANSCURRIDO:")
                print(f"  M_trans_total = {M_trans_total}")
                print(f"  M_tot_total = {M_tot_total}")
                print(f"  hasattr check = {hasattr(self, 'tiempo_transcurrido_label')}")
                print(f"  label exists = {self.tiempo_transcurrido_label is not None}")
                
                # Mostrar M_trans_total si hay datos de plan, sino mostrar hora actual
                if M_tot_total > 0:
                    porcentaje = (M_trans_total / M_tot_total) * 100
                    tiempo_texto = f"📊 Tiempo transcurrido: {M_trans_total:.0f} / {M_tot_total:.0f} min ({porcentaje:.1f}%)"
                    color = "#27ae60"  # Verde
                    print(f"  … Mostrando tiempo transcurrido: {tiempo_texto}")
                else:
                    tiempo_texto = f"📉 Hora actual: {ahora.strftime('%H:%M')}"
                    color = "#95a5a6"  # Gris
                    print(f"  📉 M_tot_total = 0, mostrando hora: {tiempo_texto}")

                print(f"{'📊'*40}\n")

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
        """Verifica si hay algíºn plan actualmente en progreso"""
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
        """ Verifica planes EN PROGRESO leyendo directo de SQLite"""
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
        """Cambia el estado de un plan específico usando su ID único (OPTIMIZADO - Cache primero)"""
        try:
            # Si se quiere iniciar un plan, verificar que no haya otro en progreso EN LA MISMA LÍNEA
            if nuevo_estado == "EN PROGRESO" and linea:
                planes_en_progreso = self._verificar_plan_en_progreso_por_linea(linea)
                
                if planes_en_progreso:
                    # Filtrar si el plan actual ya está en progreso
                    otros_planes = [plan for plan in planes_en_progreso if plan != part_no]
                    
                    if otros_planes:
                        # Usar notificacií³n estilo overlay en lugar de QMessageBox
                        self._show_success_notification(
                            "Plan en Progreso",
                            f"No se puede iniciar {part_no}\nYa hay plan activo en {linea}: {', '.join(otros_planes)}",
                            "#ffc107"  # Color amarillo/naranja para advertencia
                        )
                        return
            
            # ========== OPTIMIZACIí“N: ACTUALIZAR CACHí‰ LOCAL PRIMERO (INSTANTíNEO) ==========
            # Esto hace que la UI se actualice INMEDIATAMENTE sin esperar SQLite/MySQL
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            # Actualizar cachí© en memoria (no bloquea, es instantáneo)
            dual_db.actualizar_estado_plan_cache_only(plan_id, nuevo_estado, linea)
            
            # Invalidar cachí© de plan en progreso para forzar reconsulta
            if hasattr(self, '_plan_en_progreso_cache'):
                self._plan_en_progreso_cache.pop(linea, None)
            
            #  REFRESCAR UI INMEDIATAMENTE desde cachí© (0ms, no toca BD)
            self._refresh_plan_from_cache_only()
            
            # Mostrar notificacií³n de í©xito INMEDIATA (porque el cachí© ya está actualizado)
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
        """Callback cuando la sincronizacií³n de BD termina (actualiza plan despuí©s de BD)"""
        if success:
            logger.debug(f"… {message}")
            # Ahora Sí refrescar plan (BD ya fue actualizada, no hay riesgo de lock)
            try:
                self.refresh_plan_only(force=True)
            except Exception as e:
                logger.debug(f"Error refrescando plan despuí©s de sync: {e}")
        else:
            logger.warning(f"âš ï¸ {message}")
            # Aunque falle la BD, el cachí© ya está actualizado (UI ya muestra el cambio)
            # La sincronizacií³n se reintentará automáticamente
    
    def _terminar_plan(self, plan_id, part_no, linea=None):
        """Termina un plan cambiando su estado a TERMINADO (OPTIMIZADO - Cachí© primero)"""
        try:
            # Confirmar con el usuario
            reply = QtWidgets.QMessageBox.question(
                self,
                "Confirmar Terminacií³n",
                f"Â¿Está seguro de que desea TERMINAR el plan {part_no}?\n\nEsta accií³n finalizará el plan actual.",
                QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
                QtWidgets.QMessageBox.StandardButton.No
            )
            
            if reply == QtWidgets.QMessageBox.StandardButton.Yes:
                # ========== ACTUALIZAR CACHí‰ PRIMERO (INSTANTíNEO) ==========
                from ..services.dual_db import get_dual_db
                dual_db = get_dual_db()
                
                # Actualizar cachí© en memoria (instantáneo)
                dual_db.actualizar_estado_plan_cache_only(plan_id, "TERMINADO", linea)
                
                # Invalidar cachí© de plan en progreso
                if hasattr(self, '_plan_en_progreso_cache'):
                    self._plan_en_progreso_cache.pop(linea, None)
                
                # NO REFRESCAR PLAN AQUí - El worker lo hará cuando termine de actualizar BD
                # Esto evita congelamiento por locks de SQLite
                
                # Notificacií³n de í©xito inmediata
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
        """Maneja el cambio de lí­nea con validacií³n de plan en progreso"""
        try:
            # Si no hay nueva lí­nea seleccionada, no hacer nada
            if not nueva_linea:
                return
            
            # Si es la misma lí­nea, no hacer nada (evitar llamadas redundantes)
            if hasattr(self, '_linea_anterior') and self._linea_anterior == nueva_linea:
                return
            
            # ðŸš€ Notificar al cachí© de mí©tricas sobre el cambio de lí­nea
            try:
                from ..services.metrics_cache import get_metrics_cache
                metrics_cache = get_metrics_cache()
                if metrics_cache:
                    metrics_cache.set_active_line(nueva_linea)
                    logger.info(f"ðŸŽ¯ Cachí© de mí©tricas actualizado a lí­nea: {nueva_linea}")
            except Exception as cache_err:
                logger.debug(f"Error actualizando lí­nea activa en cachí©: {cache_err}")
            
            # NOTA: Permitir cambio de lí­nea libremente
            # La validacií³n de plan EN PROGRESO se hace en el escaneo, no en el cambio de lí­nea
            # Solo actualizar la lí­nea anterior y refrescar
            self._linea_anterior = nueva_linea
            self.refresh_plan_only(force=True)  # Forzar actualizacií³n inmediata
            
        except Exception as e:
            import logging
            logging.error(f"Error en cambio de lí­nea: {e}")
            # En caso de error, permitir el cambio
            self._linea_anterior = nueva_linea
            self.refresh_plan_only(force=True)  # Forzar actualizacií³n inmediata
    
    def _check_inactivity(self):
        """Verifica inactividad y pausa automáticamente el plan si no hay escaneos en 1.5 minutos"""
        try:
            # Obtener la lí­nea actualmente seleccionada
            linea_actual = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else None
            if not linea_actual:
                return
            
            # Verificar si hay un plan en progreso en la lí­nea actual
            planes_en_progreso = self._verificar_plan_en_progreso_por_linea(linea_actual)
            
            if not planes_en_progreso:
                # No hay plan en progreso, invalidar cachí© y no hacer nada
                self._plan_en_progreso_cache.pop(linea_actual, None)
                return
            
            # Obtener el íºltimo tiempo de escaneo para esta lí­nea
            ultimo_escaneo = self._last_scan_time_per_line.get(linea_actual)
            
            if ultimo_escaneo is None:
                # Primera vez que se verifica, guardar tiempo actual
                import time
                self._last_scan_time_per_line[linea_actual] = time.time()
                return
            
            # Calcular tiempo transcurrido desde el íºltimo escaneo
            import time
            tiempo_transcurrido = time.time() - ultimo_escaneo
            
            # Si ha pasado más de 90 segundos (1.5 minutos) sin escaneo
            TIEMPO_INACTIVIDAD = 90  # 90 segundos = 1.5 minutos
            
            if tiempo_transcurrido >= TIEMPO_INACTIVIDAD:
                # Obtener el plan_id del plan en progreso (usar cachí© si está disponible)
                from ..services.dual_db import get_dual_db
                dual_db = get_dual_db()
                
                # Verificar cachí© primero (válido por 60 segundos)
                cache_entry = self._plan_en_progreso_cache.get(linea_actual)
                if cache_entry and (time.time() - cache_entry[2]) < 60:
                    plan_id, part_no = cache_entry[0], cache_entry[1]
                else:
                    # Consultar SQLite solo si cachí© expirí³ o no existe
                    import sqlite3
                    with sqlite3.connect(dual_db.sqlite_path, timeout=5.0) as conn:
                        cursor = conn.execute("""
                            SELECT id, part_no FROM plan_local 
                            WHERE status = 'EN PROGRESO' AND line = ?
                            LIMIT 1
                        """, (linea_actual,))
                        
                        result = cursor.fetchone()
                        if not result:
                            # No hay plan en progreso, invalidar cachí©
                            self._plan_en_progreso_cache.pop(linea_actual, None)
                            return
                        
                        plan_id, part_no = result
                        # Actualizar cachí©
                        self._plan_en_progreso_cache[linea_actual] = (plan_id, part_no, time.time())
                
                # Pausar automáticamente el plan
                if plan_id and part_no:
                    # Pausar automáticamente el plan (OPTIMIZADO - Cachí© primero)
                    logger.info(f"ðŸ”´ Auto-pausa por inactividad: {part_no} en lí­nea {linea_actual} ({tiempo_transcurrido:.0f}s sin escaneo)")
                    
                    # ========== ACTUALIZAR CACHí‰ PRIMERO (INSTANTíNEO) ==========
                    dual_db.actualizar_estado_plan_cache_only(plan_id, "PAUSADO", linea_actual)
                    
                    # Invalidar cachí© de plan en progreso
                    self._plan_en_progreso_cache.pop(linea_actual, None)
                    
                    # NO REFRESCAR PLAN AQUí - El worker lo hará cuando termine de actualizar BD
                    # Esto evita congelamiento por locks de SQLite
                    
                    # Notificacií³n inmediata
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
                    
                    # Resetear el contador para esta lí­nea
                    self._last_scan_time_per_line[linea_actual] = time.time()
                        
        except Exception as e:
            import logging
            logging.error(f"Error verificando inactividad: {e}")
    
    def _show_success_notification(self, titulo: str, mensaje: str, color: str = "#28a745"):
        """Muestra una notificacií³n de í©xito para operaciones exitosas"""
        if not self.duplicate_overlay:
            return
        self.duplicate_overlay.setText(f"{titulo}\n{mensaje}\nACTUALIZADO")
        
        # Ajustar estilo y tamaí±o segíºn modo pantalla completa
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
        
        # Auto-ocultar despuí©s de 2 segundos
        QtCore.QTimer.singleShot(2000, self.duplicate_overlay.hide)

    def _create_exit_fullscreen_button(self):
        """Crea el botí³n para salir del modo pantalla completa"""
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
        
        # Posicionar el botí³n en la esquina superior derecha
        self.exit_fullscreen_btn.move(self.width() - 60, 20)

    def resizeEvent(self, event):
        """Reposicionar overlay cuando se redimensiona la ventana"""
        super().resizeEvent(event)
        if self.duplicate_overlay and self.duplicate_overlay.isVisible():
            self._center_overlay(self.duplicate_overlay)
        if self.ok_overlay and self.ok_overlay.isVisible():
            self._center_overlay(self.ok_overlay)
        
        # Reposicionar botí³n de salir del modo pantalla completa
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
        """ ULTRA-RíPIDO: Procesa escaneo en background sin bloquear UI"""
        raw = self.scan_input.text()
        if not raw:
            return
        
        #  PROTECCIí“N ANTI-DUPLICADOS: Evitar procesar el mismo cí³digo dos veces seguidas
        import time
        current_time = time.time() * 1000  # ms
        if raw == self._last_processed_code and (current_time - self._last_processed_time) < 1000:
            # Mismo cí³digo en menos de 1 segundo - ignorar duplicado
            self.scan_input.clear()
            logger.debug(f"ðŸ”‡ Cí³digo duplicado ignorado: {raw[:30]}...")
            return
        
        # Registrar este cí³digo como procesado
        self._last_processed_code = raw
        self._last_processed_time = current_time
        
        #  LIMPIAR INMEDIATAMENTE para que el usuario pueda escanear el siguiente
        self.scan_input.clear()
        
        # Normalizar texto del escáner
        raw = normalize_scanner_text(raw)
        selected_linea = self.linea_selector.currentText()
        
        #  VALIDACIí“N INSTANTíNEA Y FEEDBACK VISUAL INMEDIATO 
        # Extraer nparte del cí³digo SIN tocar BD (< 1ms)
        validation_result = self._fast_validate_scan(raw, selected_linea)
        
        if validation_result['valid']:
            # … Cí“DIGO VíLIDO - MOSTRAR OK INMEDIATAMENTE
            _play_success_sound()
            self._show_ok_overlay(validation_result['kind'])
            
            # Feedback visual verde
            self.scan_input.setStyleSheet("background-color: #c8e6c9;")  # Verde más intenso
            QtCore.QTimer.singleShot(300, lambda: self.scan_input.setStyleSheet(""))
        else:
            # âŒ ERROR - Mostrar error inmediatamente
            if validation_result.get('play_sound', True):
                _play_error_sound()
            if validation_result.get('message'):
                self._show_plan_notification(
                    validation_result['message'], 
                    raw, 
                    color=validation_result.get('color', '#FF3333')
                )
        
        #  PROCESAR EN BACKGROUND THREAD (no bloquea UI)
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
        
        #  UI YA ESTí LISTA PARA EL SIGUIENTE ESCANEO (no espera a que termine el worker)
    
    def _fast_validate_scan(self, raw: str, linea: str) -> dict:
        """ VALIDACIí“N ULTRA-RíPIDA (< 1ms) - Solo verifica que el modelo coincida con plan EN PROGRESO"""
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
            
            #  VALIDACIí“N DESDE CACHí‰ (0ms - no toca BD)
            dual_db = get_dual_db()
            if hasattr(dual_db, '_plan_cache') and dual_db._plan_cache:
                nparte_escaneado = nparte.strip().upper()
                
                # Buscar plan EN PROGRESO en cachí©
                plan_en_progreso = None
                for plan in dual_db._plan_cache:
                    if plan.get('line') == linea and plan.get('status') == 'EN PROGRESO':
                        plan_en_progreso = plan
                        break
                
                if plan_en_progreso:
                    # … HAY PLAN EN PROGRESO: Solo comparar con ese plan
                    plan_nparte_activo = plan_en_progreso.get('part_no', '').strip().upper()
                    
                    if nparte_escaneado != plan_nparte_activo:
                        # NO coincide â†’ MODELO DIFERENTE (sin buscar en otros planes)
                        return {
                            'valid': False, 
                            'message': f'âŒ MODELO DIFERENTE\nPlan: {plan_nparte_activo}\nEscaneado: {nparte_escaneado}',
                            'color': '#FF3333'
                        }
                    
                    # … Coincide - OK inmediato
                    return {'valid': True, 'kind': kind}
                else:
                    # âš ï¸ NO HAY PLAN EN PROGRESO: Verificar si existe en algíºn plan de la lí­nea
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
                        # No existe en ningíºn plan de la lí­nea
                        return {'valid': False, 'message': f'âŒ NO EN PLAN\nEscaneado: {nparte_escaneado}', 'color': '#CC3333'}
            
            # Si no hay cachí©, asumir válido (el worker validará completo)
            return {'valid': True, 'kind': kind}
            
        except Exception as e:
            logger.debug(f"Error en _fast_validate_scan: {e}")
            # En caso de error, asumir válido para no bloquear
            return {'valid': True, 'kind': 'OK'}
    
    def _on_scan_processed(self, result: int, raw: str, linea: str) -> None:
        """ Callback cuando el worker termina de procesar el escaneo"""
        try:
            if result == 999999:  # PAR COMPLETO`n                # PAR COMPLETO - QR+BARCODE emparejados exitosamente`n                try:`n                    from ..services.parser import detect_scan_format`n                    scan_type = detect_scan_format(raw)`n                    self._add_scan_to_history(raw, scan_type, success=True, message="PAR COMPLETO")`n                except Exception as e:`n                    logger.debug(f"Error agregando a historial: {e}")`n                `n                # Actualizar timestamp para timer de inactividad`n                import time`n                self._last_scan_time_per_line[linea] = time.time()`n                `n                # Actualizar UI en background (no bloquea)`n                QtCore.QTimer.singleShot(50, lambda: self._update_ui_after_scan(raw, linea))`n                `n            elif result == -5:  # ESPERANDO PAR`n                # Primera parte del par - esperando complemento`n                try:`n                    from ..services.parser import detect_scan_format`n                    scan_type = detect_scan_format(raw)`n                    self._add_scan_to_history(raw, scan_type, success=True, message="Esperando par")`n                except Exception as e:`n                    logger.debug(f"Error agregando a historial: {e}")`n                `n                # Actualizar timestamp`n                import time`n                self._last_scan_time_per_line[linea] = time.time()`n                `n                # Actualizar UI`n                QtCore.QTimer.singleShot(50, lambda: self._update_ui_after_scan(raw, linea))`n                `n            elif result > 0:
                # … í‰XITO - PAR COMPLETO
                # âš ï¸ NO reproducir sonido ni overlay aquí­ - ya se mostrí³ en handle_scan()
                
                # ðŸ“œ Agregar al historial de scans (detectar tipo automáticamente)
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=True, message="PAR COMPLETO")
                except Exception as e:
                    logger.debug(f"Error agregando a historial: {e}")
                
                # Actualizar timestamp para timer de inactividad
                import time
                self._last_scan_time_per_line[linea] = time.time()

                
                #  Actualizar UI en background (no bloquea)
                QtCore.QTimer.singleShot(50, lambda: self._update_ui_after_scan(raw, linea))
                
            elif result == 0:
                # Duplicado - IGNORAR SILENCIOSAMENTE (sin sonido, sin notificacií³n)
                # Agregar a historial pero sin error
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=True, message="Duplicado")
                except Exception:
                    pass
                logger.debug(f"ðŸ”‡ Duplicado ignorado en UI: {raw[:30]}...")
                pass  # No hacer nada, es normal en produccií³n
            elif result == -3:
                _play_error_sound()
                self._show_plan_notification("NO EN PLAN", raw, color="#CC3333")
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=False, message="NO EN PLAN")
                except Exception:
                    pass
            elif result == -4:
                _play_error_sound()
                self._show_plan_notification("NO EN PROGRESO", raw, color="#FF8800")
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=False, message="NO EN PROGRESO")
                except Exception:
                    pass
            elif result == -5:
                # Guardado, esperando complemento - SIN NOTIFICACIí“N (silencioso)
                # Agregar a historial como í©xito parcial
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=True, message="Esperando par")
                except Exception:
                    pass
            elif result == -6:
                _play_error_sound()
                self._show_plan_notification("INICIA PLAN EN MES", raw, color="#991313")
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=False, message="INICIA PLAN")
                except Exception:
                    pass
            elif result == -7:
                _play_error_sound()
                self._show_plan_notification("SUB ASSY: NO MATCH", raw, color="#991313")
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=False, message="NO MATCH")
                except Exception:
                    pass
            elif result == -8:
                _play_error_sound()
                self._show_plan_notification("REGRESA TARJETA", raw, color="#FF3333")
                try:
                    self._add_scan_to_history(raw, "QR", success=False, message="REGRESA TARJETA (QR+QR)")
                except Exception:
                    pass
            elif result == -9:
                _play_error_sound()
                self._show_plan_notification("REGRESA TARJETA", raw, color="#FF3333")
                try:
                    self._add_scan_to_history(raw, "BARCODE", success=False, message="REGRESA TARJETA (BC+BC)")
                except Exception:
                    pass
            elif result == -10:
                _play_error_sound()
                self._show_plan_notification("MODELO DIFERENTE", raw, color="#FF3333")
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=False, message="MODELO DIFERENTE")
                except Exception:
                    pass
            else:
                _play_error_sound()
                QtWidgets.QMessageBox.warning(self, "Error", "Error al procesar escaneo")
                try:
                    from ..services.parser import detect_scan_format
                    scan_type = detect_scan_format(raw)
                    self._add_scan_to_history(raw, scan_type, success=False, message="ERROR")
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Error en _on_scan_processed: {e}")
    
    def _update_ui_after_scan(self, raw: str, linea: str) -> None:
        """ Actualiza UI despuí©s de escaneo exitoso (ejecuta en background)"""
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
                
                # Actualizar cache de métricas CON RECALCULO DE UPH Y UPPH
                try:
                    from ..services.metrics_cache import get_metrics_cache
                    from datetime import date
                    import datetime as dt
                    
                    metrics_cache = get_metrics_cache()
                    if metrics_cache:
                        fecha_hoy = date.today().isoformat()
                        cached = metrics_cache.get_metrics_from_cache(linea, fecha_hoy)
                        if cached:
                            # Incrementar producción
                            cached['produccion_real'] += 1
                            
                            # Recalcular eficiencia
                            if cached['plan_acumulado'] > 0:
                                cached['eficiencia'] = (cached['produccion_real'] / cached['plan_acumulado']) * 100
                            
                            # ✅ RECALCULAR UPH desde SQLite (última hora)
                            try:
                                from ..services.dual_db import get_dual_db
                                dual_db = get_dual_db()
                                
                                # Obtener fecha actual
                                ahora = dt.datetime.now()
                                fecha_hoy = ahora.strftime("%Y-%m-%d")
                                
                                with dual_db._get_sqlite_connection(timeout=0.5) as conn:
                                    # ✅ Usar datetime() de SQLite para comparación correcta
                                    cursor = conn.execute("""
                                        SELECT COUNT(*)/2 as N
                                        FROM scans_local 
                                        WHERE linea = ? 
                                        AND fecha = ?
                                        AND datetime(ts) >= datetime('now', '-1 hour')
                                        AND is_complete = 1
                                    """, (linea, fecha_hoy))
                                    result_row = cursor.fetchone()
                                    uph_recalculado = int(result_row[0]) if result_row and result_row[0] else 0
                                    
                                    cached['uph'] = float(uph_recalculado)
                                    
                                    # ✅ RECALCULAR UPPH
                                    num_personas = cached.get('num_personas', 6)
                                    if num_personas > 0:
                                        cached['upph'] = cached['uph'] / num_personas
                                    else:
                                        cached['upph'] = 0.0
                                    
                                    logger.debug(f"🔄 UPH recalculado: {uph_recalculado}, UPPH: {cached['upph']:.2f}")
                                    
                            except Exception as e_uph:
                                logger.debug(f"⚠️ Error recalculando UPH: {e_uph}")
                            
                            # Actualizar cache con valores recalculados
                            metrics_cache.update_metrics_instant(linea, fecha_hoy, cached)
                            
                except Exception as e:
                    logger.debug(f"Error actualizando cache: {e}")
        except Exception as e:
            logger.debug(f"Error en _update_ui_after_scan: {e}")

    def _create_duplicate_overlay(self):
        """Crea el overlay de notificacií³n para duplicados"""
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
        
        # Ocultar inicialmente y asegurar que estí© encima de todo
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
        # Asegurar que estí© encima de todo
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
                details = "Este cí³digo ya fue escaneado"
            else:
                title = 'QR DUPLICADO'
                details = "Este cí³digo ya fue escaneado"
        except Exception:
            pass

        self.duplicate_overlay.setText(
            f"{title}\n{scan_code}\n{details}\nYA EXISTE EN EL SISTEMA"
        )

        # Ajustar tamaí±o segíºn modo pantalla completa
        if self._fullscreen_mode:
            self.duplicate_overlay.resize(900, 350)
            # Aumentar tamaí±o de fuente para pantalla completa
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

        #  Mostrar ultra-rápido 200ms para escaneo continuo
        self.duplicate_timer.start(200)

    def _show_plan_notification(self, titulo: str, scan_code: str, color: str = "#004c99"):
        if not self.duplicate_overlay:
            return
        self.duplicate_overlay.setText(f"{titulo}\n{scan_code}\nNO SE ACEPTA")
        
        # Ajustar estilo y tamaí±o segíºn modo pantalla completa
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
        # â° Duracií³n 2 segundos para que sea visible
        self.duplicate_timer.start(5000)

    def _show_wait_pair(self, expected_format: str, scan_code: str):
        """Muestra overlay indicando que falta el formato complementario (QR o BARCODE)."""
        titulo = f"ESPERA EL PAR: ESCANEA {expected_format.upper()}"
        self._show_plan_notification(titulo, scan_code, color="#B30000")
    
    def _toggle_duplicate_blink(self):
        """Parpadeo desactivado (mantenido por compatibilidad)."""
        return
    
    def _hide_duplicate_overlay(self):
        """Oculta el overlay de notificacií³n"""
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
        
        # Ajustar tamaí±o y estilo segíºn modo pantalla completa
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
        #  Ultra-rápido para máxima fluidez
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
                        self.scan_input.raise_()  # Asegurar que estí© al frente
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
            
            # Validar formato básico primero (QR nuevo í± o antiguo ;)
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
        
        # EVITAR DUPLICADOS - verificar si ya procesamos este cí³digo
        import time
        current_time = time.time() * 1000  # ms
        if raw == self._last_processed_code and (current_time - self._last_processed_time) < 500:
            # Mismo cí³digo en menos de 500ms - ignorar duplicado
            self.scan_input.clear()
            return
        
        #  NO limpiar aquí­ - dejar que el escáner termine de escribir
        # La limpieza se hará en _process_complete_qr o _process_pending_barcode
        
        # Considerar QR completo cuando cumple patrí³n (nuevo í± o antiguo ;)
        if not is_complete_qr(raw):
            # Posible BARCODE (no lleva ';') -> usar heurí­stica y debounce
            # Condicií³n: longitud mí­nima 13, íºltimos 12 dí­gitos, todo alfanumí©rico
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
                # Heurí­stica adicional: si los íºltimos 12 son dí­gitos y longitud >=13 forzar gap rápido
                if len(raw) >= 13 and raw[-12:].isdigit():
                    gap = min(gap, self._barcode_end_gap_fast)
                self._barcode_timer.start(int(gap))
            else:
                self._barcode_timer.stop()
            self._last_scan_len = len(raw)
            return
        
        # Detectado QR con 4 secciones ';' - es QR en progreso
        #  NO guardar ni limpiar aquí­ - esperar a que termine completamente
        # El timer procesará cuando el escáner termine de escribir
        self._qr_complete_timer.start(self._qr_complete_delay_ms)
        return

    def _process_complete_qr(self):
        """Procesa QR completo despuí©s de esperar posibles lí­neas adicionales"""
        if self._processing_scan:
            return
        
        # ðŸ”’ Bloquear actualizaciones durante el escaneo
        self._scan_in_progress = True
        
        #  Leer del input AHORA (el escáner ya terminí³ de escribir)
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
        
        #  LIMPIAR INPUT AHORA (despuí©s de leer)
        self.scan_input.clear()
        
        # Marcar como procesado
        self._last_processed_code = raw
        self._last_processed_time = time.time() * 1000
        
        # Procesar con sistema optimizado
        self._processing_scan = True
        try:
            selected_linea = self.linea_selector.currentText()
            result = process_scan_direct(raw, selected_linea)
            
            if result == 999999:  # PAR COMPLETO`n                # PAR COMPLETO - QR+BARCODE emparejados exitosamente`n                try:`n                    from ..services.parser import detect_scan_format`n                    scan_type = detect_scan_format(raw)`n                    self._add_scan_to_history(raw, scan_type, success=True, message="PAR COMPLETO")`n                except Exception as e:`n                    logger.debug(f"Error agregando a historial: {e}")`n                `n                # Actualizar timestamp para timer de inactividad`n                import time`n                self._last_scan_time_per_line[linea] = time.time()`n                `n                # Actualizar UI en background (no bloquea)`n                QtCore.QTimer.singleShot(50, lambda: self._update_ui_after_scan(raw, linea))`n                `n            elif result == -5:  # ESPERANDO PAR`n                # Primera parte del par - esperando complemento`n                try:`n                    from ..services.parser import detect_scan_format`n                    scan_type = detect_scan_format(raw)`n                    self._add_scan_to_history(raw, scan_type, success=True, message="Esperando par")`n                except Exception as e:`n                    logger.debug(f"Error agregando a historial: {e}")`n                `n                # Actualizar timestamp`n                import time`n                self._last_scan_time_per_line[linea] = time.time()`n                `n                # Actualizar UI`n                QtCore.QTimer.singleShot(50, lambda: self._update_ui_after_scan(raw, linea))`n                `n            elif result > 0:
                # … Reproducir sonido de í©xito
                _play_success_sound()
                
                #  OPTIMIZACIí“N MíXIMA: NO actualizar plan ni totales aquí­
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
                
                # âŒ ELIMINADO refresh_plan_only y refresh_totals_only (causaban congelamiento)
                # … El timer de 15 SEGUNDOS se encarga de actualizar automáticamente (optimizado con Dual DB)
            elif result == 0:
                # Duplicado - IGNORAR SILENCIOSAMENTE (sin sonido, sin notificacií³n)
                logger.debug(f"ðŸ”‡ Duplicado ignorado en UI (_process_complete_qr): {raw[:30]}...")
                pass  # No hacer nada, es normal en produccií³n
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
                    logger.debug(f"QR duplicado consecutivo ignorado: {raw[:20]}...")
                    return
                # No es consecutivo - mostrar sin sonido
                self._show_plan_notification("QR DUPLICADO\nEscanea BARCODE", raw, color="#FF3333")
            elif result == -9:
                # BARCODE duplicado - verificar si es consecutivo
                if raw == self._last_processed_code:
                    logger.debug(f"BARCODE duplicado consecutivo ignorado: {raw[:20]}...")
                    return
                # No es consecutivo - mostrar sin sonido
                self._show_plan_notification("BARCODE DUPLICADO\nEscanea QR", raw, color="#FF3333")
            elif result == -10:
                # Modelo diferente al plan EN PROGRESO
                _play_error_sound()
                self._show_plan_notification("MODELO DIFERENTE\nAL PLAN EN PROGRESO", raw, color="#FF3333")
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
            self._scan_in_progress = False  # ðŸ”“ Desbloquear actualizaciones
    
    def _process_pending_barcode(self):
        if self._processing_scan:
            return
        if self._pending_barcode_processed:
            return
            
        # ðŸ”’ Bloquear actualizaciones durante el escaneo
        self._scan_in_progress = True
        
        raw = (self.scan_input.text() or '').strip()
        if not raw or is_complete_qr(raw):
            self._scan_in_progress = False  # Desbloquear
            return  # Ya lo manejará el flujo normal (QR completo o vací­o)
        
        # EVITAR DUPLICADOS - verificar si ya procesamos este cí³digo
        import time
        current_time = time.time() * 1000
        if raw == self._last_processed_code and (current_time - self._last_processed_time) < 500:
            self.scan_input.clear()
            self._scan_in_progress = False  # Desbloquear
            return
        
        # Validar nuevamente heurí­stica
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
                if result == 999999:  # PAR COMPLETO`n                # PAR COMPLETO - QR+BARCODE emparejados exitosamente`n                try:`n                    from ..services.parser import detect_scan_format`n                    scan_type = detect_scan_format(raw)`n                    self._add_scan_to_history(raw, scan_type, success=True, message="PAR COMPLETO")`n                except Exception as e:`n                    logger.debug(f"Error agregando a historial: {e}")`n                `n                # Actualizar timestamp para timer de inactividad`n                import time`n                self._last_scan_time_per_line[linea] = time.time()`n                `n                # Actualizar UI en background (no bloquea)`n                QtCore.QTimer.singleShot(50, lambda: self._update_ui_after_scan(raw, linea))`n                `n            elif result == -5:  # ESPERANDO PAR`n                # Primera parte del par - esperando complemento`n                try:`n                    from ..services.parser import detect_scan_format`n                    scan_type = detect_scan_format(raw)`n                    self._add_scan_to_history(raw, scan_type, success=True, message="Esperando par")`n                except Exception as e:`n                    logger.debug(f"Error agregando a historial: {e}")`n                `n                # Actualizar timestamp`n                import time`n                self._last_scan_time_per_line[linea] = time.time()`n                `n                # Actualizar UI`n                QtCore.QTimer.singleShot(50, lambda: self._update_ui_after_scan(raw, linea))`n                `n            elif result > 0:
                    # … Reproducir sonido de í©xito
                    _play_success_sound()
                    
                    self._pending_barcode_processed = True
                    
                    #  OPTIMIZACIí“N MíXIMA: NO actualizar plan ni totales aquí­
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
                    
                    # âŒ ELIMINADO refresh_plan_only y refresh_totals_only (causaban congelamiento)
                    # … El timer de 15 SEGUNDOS se encarga de actualizar automáticamente (optimizado con Dual DB)
                    
                    # Reset mí©tricas de escritura tras procesar
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == 0:
                    # Duplicado - IGNORAR SILENCIOSAMENTE (sin sonido, sin notificacií³n)
                    logger.debug(f"ðŸ”‡ Duplicado ignorado en UI (_process_pending_barcode): {raw[:30]}...")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                    pass  # No hacer nada, es normal en produccií³n
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
                        logger.debug(f"ðŸ”‡ QR duplicado consecutivo ignorado: {raw[:20]}...")
                        return
                    # No es consecutivo - mostrar sin sonido
                    self._show_plan_notification("QR DUPLICADO\nEscanea BARCODE", raw, color="#FF3333")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -9:
                    # BARCODE duplicado - verificar si es consecutivo
                    if raw == self._last_processed_code:
                        logger.debug(f"BARCODE duplicado consecutivo ignorado: {raw[:20]}...")
                        return
                    # No es consecutivo - mostrar sin sonido
                    self._show_plan_notification("BARCODE DUPLICADO\nEscanea QR", raw, color="#FF3333")
                    self._last_char_time = 0.0
                    self._interchar_times.clear()
                elif result == -10:
                    # Modelo diferente al plan EN PROGRESO
                    _play_error_sound()
                    self._show_plan_notification("MODELO DIFERENTE", raw, color="#FF3333")
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
                self._scan_in_progress = False  # ðŸ”“ Desbloquear actualizaciones
                if not self.scan_input.text():
                    self._pending_barcode_processed = False

    def refresh_tables(self) -> None:
        # Reinicializar tabla del plan si es necesario (cambio de modo SUB ASSY)
        self._reinit_plan_table()
        
        # Cargar datos del plan despuí©s de reinicializar
        self.refresh_plan_only()
        
        # íšltimos escaneos desde SQLite local (ultra-rápido)
        scans = get_last_scans(100)
        self.table_scans.setRowCount(0)
        for s in scans:
            row = self.table_scans.rowCount()
            self.table_scans.insertRow(row)
            vals = [s.id, s.ts, s.raw, s.lote, s.secuencia, s.estacion, s.nparte, s.modelo or "", s.cantidad, s.linea]
            for col, v in enumerate(vals):
                self.table_scans.setItem(row, col, QtWidgets.QTableWidgetItem(str(v)))

        # Totales del dí­a desde SQLite local (ultra-rápido)
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
        self.status.showMessage(f"MySQL conectado | UPH real íºltima hora: {uph} | Cola offline: {qsize}")

    def update_status_only(self) -> None:
        """Actualizacií³n rápida solo del status bar sin consultas pesadas"""
        try:
            qsize = self.db.queue_size()
            self.status.showMessage(f"MySQL conectado | Cola offline: {qsize}")
        except Exception as e:
            self.status.showMessage(f"MySQL error: {str(e)[:50]}...")
    
    def _update_ui_throttled(self) -> None:
        """Actualizacií³n de UI controlada para evitar sobrecarga durante escaneo constante"""
        import time
        current_time = time.time()
        if current_time - self._last_ui_update < self._ui_update_interval:
            return
        
        self._last_ui_update = current_time
        # Refrescar tambií©n plan para ver producido y uph real sin esperar al timer
        self.refresh_totals_only()
        self.refresh_plan_only()
        self.update_status_fast()
    
    def _update_tables_and_status(self) -> None:
        """Actualizar TODO en BACKGROUND thread (sin congelar UI)"""
        # … VERIFICAR CAMBIO DE FECHA (medianoche)
        from datetime import date
        today = date.today()
        if hasattr(self, '_current_date') and self._current_date != today:
            logger.warning(f"CAMBIO DE FECHA DETECTADO: {self._current_date} â†’ {today}")
            self._current_date = today
            
            # Forzar recarga completa del plan para el nuevo dí­a
            QtCore.QTimer.singleShot(500, self._force_reload_plan_for_new_day)
            return  # Salir, la recarga se hará en el callback
        
        # … VERIFICAR CAMBIOS EN EL PLAN DESDE MYSQL (cada 15s el sync worker descarga nuevos datos)
        from ..services.dual_db import get_dual_db
        dual_db = get_dual_db()
        if dual_db.check_plan_changed_and_reset():
            logger.warning("ðŸ“Š PLAN CAMBIí“ EN MYSQL - Recargando tabla automáticamente...")
            QtCore.QTimer.singleShot(100, self._force_reload_plan_table)
            # Mostrar notificacií³n visual temporal
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
                # Restaurar estilo despuí©s de 2 segundos
                QtCore.QTimer.singleShot(2000, lambda: self.fecha_plan_label.setStyleSheet(original_style))
        
        # Si hay un escaneo en progreso, saltar actualizacií³n
        if getattr(self, '_scan_in_progress', False):
            return
        
        # Si ya hay un worker corriendo, saltar
        if hasattr(self, '_update_worker') and self._update_worker and self._update_worker.isRunning():
            return
        
        # … Worker thread para leer datos en background
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
                        
                        # 1. Mí©tricas de cards
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
                    self.data_ready.emit({})  # Emitir dict vací­o en caso de error
        
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
            
            # Verificar que no cambií³ la lí­nea mientras se cargaban datos
            if data.get('linea') != self.linea_selector.currentText():
                return
            
            # âš ï¸ NO actualizar cards desde worker background - ahora usamos _update_plan_totals()
            # que calcula correctamente el plan_acumulado basado en tiempo transcurrido
            
            # Cí“DIGO VIEJO (DESHABILITADO):
            # self._update_cards_with_metrics(
            #     plan=data['plan_total'],
            #     plan_acum=data['plan_total'],  # âŒ Este valor es INCORRECTO (siempre usa total)
            #     produccion=data['produccion_total'],
            #     eficiencia=data['eficiencia'],
            #     uph=0,
            #     upph=0.0
            # )
            
            # … ACTUALIZAR TABLA DE PLAN (si hay datos)
            if data.get('plan_rows'):
                QtCore.QTimer.singleShot(10, lambda: self._render_plan_table_fast(data['plan_rows']))
            
            # … Actualizar status bar
            self.update_status()
            
            # … SINCRONIZAR VENTANA UPH (si está abierta)
            self._sync_metrics_to_widget()
            
            logger.debug(f"… Cards actualizadas: Plan={data['plan_total']}, Prod={data['produccion_total']}, Efic={data['eficiencia']:.1f}%")
            
        except Exception as e:
            logger.warning(f"âš ï¸ Error actualizando UI: {e}")
    
    def _render_plan_table_fast(self, plan_rows: list):
        """Renderiza tabla de plan con datos YA preparados (sin consultas adicionales)"""
        try:
            # … DETECTAR CAMBIOS EN SECUENCIA: Si el orden cambií³, re-renderizar completo
            if self.table_plan.rowCount() == len(plan_rows):
                sequence_changed = False
                for row_idx in range(self.table_plan.rowCount()):
                    part_no_item = self.table_plan.item(row_idx, 0)
                    if part_no_item:
                        plan_id_stored = part_no_item.data(QtCore.Qt.ItemDataRole.UserRole)
                        # El plan en esta posicií³n deberí­a ser el mismo si no cambií³ el orden
                        if row_idx < len(plan_rows):
                            expected_id = plan_rows[row_idx].get('id')
                            if plan_id_stored != expected_id:
                                sequence_changed = True
                                break
                
                if sequence_changed:
                    self._force_reload_plan_table()
                    return
            
            # Actualizar valores de produccií³n en las celdas existentes
            for row_idx in range(self.table_plan.rowCount()):
                part_no_item = self.table_plan.item(row_idx, 0)
                if not part_no_item:
                    continue
                
                # Obtener plan_id almacenado en la celda (identificador íºnico)
                plan_id_stored = part_no_item.data(QtCore.Qt.ItemDataRole.UserRole)
                
                if plan_id_stored is None:
                    continue
                
                # Buscar el plan correspondiente por ID íºnico (no por part_no)
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
                        
                        # Actualizar tambií©n el estado si cambií³
                        estado_nuevo = plan.get('status', '')
                        col_estado = 8 if self.table_plan.columnCount() == 10 else 7
                        estado_item = self.table_plan.item(row_idx, col_estado)
                        if estado_item:
                            estado_item.setText(estado_nuevo)
                        
                        # Actualizar botí³n TERMINAR segíºn el nuevo estado
                        col_acciones = 9 if self.table_plan.columnCount() == 10 else 8
                        estado_upper = estado_nuevo.upper()
                        
                        # Verificar si debe tener botí³n TERMINAR
                        debe_tener_boton = 'PROGRESO' in estado_upper or 'PAUSADO' in estado_upper
                        tiene_boton = self.table_plan.cellWidget(row_idx, col_acciones) is not None
                        
                        if debe_tener_boton and not tiene_boton:
                            # Agregar botí³n TERMINAR
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
                            # Remover botí³n TERMINAR
                            self.table_plan.setCellWidget(row_idx, col_acciones, None)
                        
                        break
            
            # Forzar refresco visual de la tabla completa
            self.table_plan.viewport().repaint()
            
            logger.debug(f"… Tabla de plan actualizada (solo valores producidos y estado)")
            
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
            logger.info("ðŸ“… Recargando plan para nuevo dí­a...")
            
            # … ACTUALIZAR INDICADOR VISUAL DE FECHA
            from datetime import date
            fecha_hoy = date.today().strftime("%d/%m/%Y")
            if hasattr(self, 'fecha_plan_label'):
                self.fecha_plan_label.setText(f"PLAN DEL DíA: {fecha_hoy}")
                # Animacií³n visual: cambiar color temporalmente para llamar la atencií³n
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
                # Volver al color normal despuí©s de 3 segundos
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
            
            # Forzar sincronizacií³n desde MySQL para obtener plan del dí­a actual
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            # Trigger manual de sincronizacií³n de plan
            if dual_db and hasattr(dual_db, '_sync_plan_from_mysql'):
                # Forzar sync inmediato (llamar directamente al método de dual_db)
                dual_db._sync_plan_from_mysql()
                logger.info("… Sync de plan forzado para nuevo dí­a")
            
            # Esperar un momento para que se complete la sincronizacií³n
            QtCore.QTimer.singleShot(1000, lambda: self._force_reload_plan_table())
            
            # Mostrar notificacií³n al usuario
            if hasattr(self, 'status'):
                self.status.showMessage("Plan actualizado para nuevo dí­a", 5000)
            
        except Exception as e:
            logger.error(f"âŒ Error recargando plan para nuevo dí­a: {e}")
    
    def _force_refresh(self) -> None:
        """Forzar actualizacií³n manual de todas las tablas y mí©tricas
        
        Este mí©todo se llama cuando el usuario presiona el botí³n 'Actualizar'.
        Realiza una actualizacií³n completa inmediata sin esperar el timer automático.
        """
        try:
            # Deshabilitar botí³n temporalmente para evitar clicks míºltiples
            if hasattr(self, 'refresh_button'):
                self.refresh_button.setEnabled(False)
                self.refresh_button.setText("Actualizando...")
            
            # Limpiar cache de UPH para forzar recalcular
            if hasattr(self, '_uph_cache'):
                self._uph_cache.clear()
                self._uph_cache_time.clear()
            
            # Actualizar todas las tablas y mí©tricas
            self._update_tables_and_status()
            
            # Mensaje de confirmacií³n
            if hasattr(self, 'status'):
                self.status.showMessage("Actualizacií³n completada", 3000)
            
            # Log de la accií³n
            import logging
            logger = logging.getLogger(__name__)
            logger.info("Actualizacií³n manual forzada por el usuario")
            
        except Exception as e:
            # Manejo de errores
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"âŒ Error en actualizacií³n manual: {e}")
            
            if hasattr(self, 'status'):
                self.status.showMessage(f"âŒ Error al actualizar: {str(e)[:50]}", 5000)
        
        finally:
            # Re-habilitar botí³n despuí©s de 2 segundos
            if hasattr(self, 'refresh_button'):
                QtCore.QTimer.singleShot(2000, lambda: self._re_enable_refresh_button())
    
    def _re_enable_refresh_button(self):
        """Re-habilitar el botí³n de actualizar"""
        if hasattr(self, 'refresh_button'):
            self.refresh_button.setEnabled(True)
            self.refresh_button.setText("Actualizar")
    
    def _toggle_metrics_widget(self):
        """Abrir/cerrar ventana flotante de mí©tricas"""
        try:
            if self.metrics_widget is None or not self.metrics_widget.isVisible():
                # Crear o mostrar la ventana
                if self.metrics_widget is None:
                    self.metrics_widget = MetricsWidget(self)
                    logger.info("ðŸªŸ Creada ventana flotante de mí©tricas")
                
                # Mostrar la ventana primero
                self.metrics_widget.show()
                self.metrics_widget.raise_()
                self.metrics_widget.activateWindow()
                
                # Actualizar con los valores actuales DESPUí‰S de mostrarla
                self._sync_metrics_to_widget()
                
                # Actualizar texto del botí³n
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
                
                logger.info("ðŸªŸ Ventana flotante de mí©tricas mostrada")
            else:
                # Cerrar la ventana
                self.metrics_widget.close()
                self.metrics_widget = None
                
                # Restaurar texto del botí³n
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
                
                logger.info("ðŸªŸ Ventana flotante de mí©tricas cerrada")
        
        except Exception as e:
            logger.error(f"âŒ Error al toggle ventana flotante: {e}")
            import traceback
            traceback.print_exc()
    
    def _sync_metrics_to_widget(self):
        """Sincronizar mí©tricas actuales con la ventana flotante"""
        if self.metrics_widget is None or not self.metrics_widget.isVisible():
            return
        
        try:
            # Obtener lí­nea seleccionada
            linea_seleccionada = self.linea_selector.currentText()
            
            # Crear diccionario de mí©tricas desde los value_label de las cards
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
            logger.error(f"âŒ Error al sincronizar mí©tricas: {e}")
    
    def update_status_fast(self) -> None:
        """Actualizacií³n ultra-rápida del status SIN tocar DB (solo memoria)"""
        try:
            # Obtener contador en memoria (instantáneo)
            scan_count = getattr(self, '_scan_counter', 0)
            
            # Obtener lí­nea actual
            selected_linea = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else "N/A"
            
            # Actualizar informacií³n del sistema directo
            direct_mysql = get_direct_mysql()
            
            # Status rápido sin bloquear (no intenta conectar si está en ventana offline)
            if direct_mysql and getattr(direct_mysql, 'is_quick_online', None) and direct_mysql.is_quick_online():
                sync_status = "Conectado"
                status_color = "#00aa00"
                # Mostrar contador en memoria (instantáneo, no DB)
                self.status.showMessage(f"MySQL Directo | {sync_status} | Lí­nea: {selected_linea} | Scans: {scan_count} (memoria)")
            else:
                sync_status = "Desconectado"
                status_color = "#aa0000"
                self.status.showMessage(f"Sistema | {sync_status} | Lí­nea: {selected_linea} | Scans: {scan_count} (memoria)")
            
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
            
            #  SIN UPH Real/Proy/CT: 10 columnas SUB ASSY, 9 normal
            num_columns = 10 if (sub_assy_mode and app_mode.upper() == 'ASSY') else 9
            current_columns = self.table_plan.columnCount()
            
            # Log para debug
            print(f"_reinit_plan_table: SUB_ASSY_MODE={sub_assy_mode}, APP_MODE={app_mode}")
            print(f"_reinit_plan_table: Cambiando de {current_columns} a {num_columns} columnas")
            
            # FORZAR recreacií³n completa de la tabla
            self.table_plan.clear()
            self.table_plan.setRowCount(0)
            self.table_plan.setColumnCount(num_columns)
            
            # Headers dinámicos segíºn modo SUB ASSY -  SIN UPH Real/Proy/CT
            if num_columns == 10:  # SUB ASSY
                headers = ["Part No", "Lote", "Modelo", "SUB ASSY", "Plan", "Producido", "% Avance", "UPH Target", "Estado", "Acciones"]
            else:  # Normal (9 columnas)
                headers = ["Part No", "Lote", "Modelo", "Plan", "Producido", "% Avance", "UPH Target", "Estado", "Acciones"]
            
            self.table_plan.setHorizontalHeaderLabels(headers)
            self.table_plan.horizontalHeader().setStretchLastSection(True)
            
            # Reajustar tamaí±os de columnas
            plan_header = self.table_plan.horizontalHeader()
            plan_header.resizeSection(0, 120)  # Part No
            plan_header.resizeSection(1, 90)   # Lote
            plan_header.resizeSection(2, 100)  # Modelo
            
            if num_columns == 10:  # Modo SUB ASSY -  SIN UPH Real/Proy/CT
                plan_header.resizeSection(3, 80)   # SUB ASSY
                plan_header.resizeSection(4, 60)   # Plan
                plan_header.resizeSection(5, 70)   # Producido
                plan_header.resizeSection(6, 70)   # % Avance
                plan_header.resizeSection(7, 70)   # UPH Target
                plan_header.resizeSection(8, 80)   # Estado
                plan_header.resizeSection(9, 100)  # Acciones
            else:  # Modo normal (9 columnas) -  SIN UPH Real/Proy/CT
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
        … ULTRA RíPIDO: Lee de SQLite local (< 1ms)
        … NO BLOQUEA: Sincronizacií³n con MySQL es automática en background
        … MEJOR UX: Actualizacií³n instantánea sin esperas
        """
        try:
            # Si hay un escaneo en progreso, saltar actualizacií³n para no interferir
            if getattr(self, '_scan_in_progress', False):
                logger.debug("â­ï¸ Actualizacií³n de totales saltada: escaneo en progreso")
                return
            
            # … OPTIMIZACIí“N: Leer directamente de SQLite local (instantáneo, < 1ms)
            # NO necesitamos worker thread porque SQLite local es ultra rápido
            from ..services.dual_db import get_dual_db
            
            dual_db = get_dual_db()
            
            # … Lectura INSTANTíNEA de SQLite local (no toca MySQL)
            totals_dict = dual_db.get_local_totals()
            
            # Resetear contador en memoria
            self._scan_counter = 0
            
            # Limpiar tabla
            self.table_totals.setRowCount(0)
            
            # Mostrar totales por lí­nea
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
            
            # Estado de sincronizacií³n (worker en background)
            sync_status = "Sync OK" if dual_db._sync_worker.is_alive() else "Sync Error"
            
            # … Mostrar que estamos usando cachí© local ultra rápido
            self.status.showMessage(f" SQLite Local (instantáneo) | {sync_status} | Total: {total_cantidad}")
            
            # … ACTUALIZAR CARDS DE Mí‰TRICAS (Plan, Eficiencia, UPH, etc.)
            # Esto actualiza las 6 cards principales cada 15 segundos
            try:
                self.update_status_fast()  # Actualiza cards desde cachí© de mí©tricas
            except Exception as card_error:
                logger.debug(f"âš ï¸ No se pudo actualizar cards de mí©tricas: {card_error}")
            
            logger.debug(f"… Tabla y cards actualizadas desde SQLite local: {total_cantidad} piezas")
            
        except Exception as e:
            logger.error(f"Error actualizando totales desde SQLite local: {e}")
            # Fallback a MySQL directo solo si SQLite falla
            self._fallback_totals_update(e)
    
    def _fallback_totals_update(self, error: Exception):
        """Actualizacií³n de emergencia usando MySQL directo"""
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
        """ ULTRA RíPIDO: Actualiza tabla de plan SOLO desde cachí© (0ms, sin BD)"""
        try:
            from ..services.dual_db import get_dual_db
            dual_db = get_dual_db()
            
            # Obtener lí­nea actual
            linea = self.linea_selector.currentText()
            
            #  Leer directamente del cachí© en memoria (sin BD)
            if not hasattr(dual_db, '_plan_cache') or not dual_db._plan_cache:
                return  # Cachí© vací­o, no hacer nada
            
            # Filtrar planes de la lí­nea actual desde cachí©
            plan_rows = [
                plan for plan in dual_db._plan_cache 
                if plan.get('line') == linea
            ]
            
            # Renderizar directamente (mí©todo interno que ya existe)
            self._on_plan_data_ready(linea, plan_rows, {})
            
        except Exception as e:
            logger.debug(f"Error refrescando desde cachí©: {e}")

    def refresh_plan_only(self, force=False) -> None:
        """Lanza un worker en background para obtener datos y renderizar cuando termine - OPTIMIZADO"""
        try:
            # … Si hay un escaneo en progreso, saltar actualizacií³n para no interferir
            if not force and getattr(self, '_scan_in_progress', False):
                logger.debug("â­ï¸ Actualizacií³n de plan saltada: escaneo en progreso")
                return
                
            # … Rate limiting MODERADO para evitar actualizaciones excesivas
            import time
            if not hasattr(self, '_last_refresh_time'):
                self._last_refresh_time = 0
            current_time = time.time()
            
            # … Intervalo mí­nimo de 5 segundos entre actualizaciones
            # Con Dual DB optimizado, 5s es suficiente para prevenir sobrecarga
            min_interval = 5.0  # 5 segundos mí­nimo entre actualizaciones (optimizado con Dual DB)
            
            if not force and current_time - self._last_refresh_time < min_interval:
                logger.debug(f"â­ï¸ Actualizacií³n de plan saltada: muy reciente ({current_time - self._last_refresh_time:.1f}s < {min_interval}s)")
                return
            
            self._last_refresh_time = current_time

            # … Si hay un worker ya corriendo, NO iniciar otro (previene acumulacií³n)
            if getattr(self, '_plan_worker', None) and self._plan_worker.isRunning():
                if force:
                    # Esperar un poco a que termine el worker actual
                    self._plan_worker.wait(100)  # Esperar máximo 100ms
                    if self._plan_worker.isRunning():
                        logger.debug("â­ï¸ Worker de plan aíºn corriendo, saltando actualizacií³n")
                        return
                else:
                    logger.debug("â­ï¸ Worker de plan ya en ejecucií³n, saltando")
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
                        # En caso de error, emitir lista vací­a para no bloquear
                        logger.debug(f"Error en PlanFetchWorker: {e}")
                        self.data_ready.emit(self._linea, [], {})

            self._plan_worker = PlanFetchWorker(linea, self)
            self._plan_worker.data_ready.connect(self._on_plan_data_ready)
            self._plan_worker.start()
            logger.debug(f"… Worker de plan iniciado para lí­nea: {linea}")
        except Exception as e:
            import logging
            logging.error(f"Error en refresh_plan_only: {e}")

    def _on_plan_data_ready(self, linea: str, plan_rows, uph_proj_map: dict) -> None:
        """Renderiza la tabla de plan con datos obtenidos en background."""
        try:
            # Si el usuario cambií³ la lí­nea mientras cargábamos, ignorar
            if linea != self.linea_selector.currentText():
                return
            self.table_plan.setRowCount(0)
            self.uph_proj_map = uph_proj_map or {}
            # Definicií³n local del widget de progreso
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
                plan_id = r.get('id')  # ID íºnico del plan para operaciones
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
                    #  SIN UPH Real/Proy/CT - Solo: Part No, Lote, Modelo, SUB ASSY, Plan, Producido, % Avance, UPH Target, Estado, Acciones
                    vals = [nparte, r.get('lot_no',''), r.get('model_code',''), sub_assy_info,
                            plan, prod, '', uph_target, estado, '']
                    progress_col = 6
                    acciones_col = 9
                else:
                    #  SIN UPH Real/Proy/CT - Solo: Part No, Lote, Modelo, Plan, Producido, % Avance, UPH Target, Estado, Acciones
                    vals = [nparte, r.get('lot_no',''), r.get('model_code',''),
                            plan, prod, '', uph_target, estado, '']
                    progress_col = 5
                    acciones_col = 8
                for col,v in enumerate(vals):
                    if col == progress_col:
                        self.table_plan.setCellWidget(row, col, build_progress(percent_val))
                        continue
                    elif col == acciones_col:
                        # Crear botí³n TERMINAR si está EN PROGRESO o PAUSADO
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
                    #  Almacenar plan_id en la columna 0 (Part No) para identificacií³n íºnica
                    if col == 0:
                        item.setData(QtCore.Qt.ItemDataRole.UserRole, plan_id)
                    #  SIN coloreo UPH Real (columna eliminada)
                    self.table_plan.setItem(row, col, item)
            # Totales / mí©tricas
            self._update_plan_totals(plan_rows or [])
            self._sync_metrics_to_widget()
        except Exception as e:
            import logging
            logging.error(f"Error renderizando plan (worker): {e}")

    def _get_sub_assy_info(self, nparte: str) -> str:
        """Obtiene informacií³n SUB ASSY desde el cache del sistema dual (muy rápido)"""
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
            # Solo actualizar si la lí­nea actual coincide
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
            
            #  USAR LOCK GLOBAL para evitar "database is locked"
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
            
            # Determinar columna de Producido segíºn modo SUB ASSY
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
            
            logger.debug(f"… Fila del plan actualizada: {nparte} â†’ Producido: {produced}/{plan} ({percent_val:.1f}%)")
            
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
        """Configurar la ubicacií³n de la base de datos local SQLite"""
        try:
            import os
            
            # Mí©todo deshabilitado - sistema optimizado usa solo MySQL directo
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
            
            # Diálogo de informacií³n actual
            current_info = f"Ubicacií³n actual: {current_path}\nTamaí±o: {self._get_db_size(current_path)}"
            
            reply = QtWidgets.QMessageBox.question(
                self,
                "Configurar Base de Datos Local",
                f"{current_info}\n\nÂ¿Desea cambiar la ubicacií³n de la base de datos SQLite local?",
                QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
                QtWidgets.QMessageBox.StandardButton.No
            )
            
            if reply == QtWidgets.QMessageBox.StandardButton.Yes:
                # Selector de nueva ubicacií³n
                new_dir = QtWidgets.QFileDialog.getExistingDirectory(
                    self,
                    "Seleccionar nueva ubicacií³n para base de datos local",
                    current_dir
                )
                
                if new_dir:
                    new_path = os.path.join(new_dir, "local_scans.db")
                    
                    # Confirmar cambio
                    confirm = QtWidgets.QMessageBox.question(
                        self,
                        "Confirmar cambio",
                        f"Nueva ubicacií³n: {new_path}\n\nÂ¿Desea mover la base de datos a esta ubicacií³n?\n\nNota: La aplicacií³n se reiniciará.",
                        QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No
                    )
                    
                    if confirm == QtWidgets.QMessageBox.StandardButton.Yes:
                        self._move_database(current_path, new_path)
        
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Error configurando DB: {e}")
    
    def _get_db_size(self, path: str) -> str:
        """Obtener tamaí±o de la base de datos"""
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
        """Mí©todo deshabilitado - sistema optimizado usa solo MySQL directo"""
        QtWidgets.QMessageBox.information(
            self,
            "Operacií³n no disponible",
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
        
        # Ocultar todo el contenedor del selector de lí­nea
        if hasattr(self, 'linea_container_widget'):
            self.linea_container_widget.hide()
        
        # Ocultar la barra de meníº y status
        self.menuBar().hide()
        self.statusBar().hide()
        
        # MANTENER EL LOGO VISIBLE - asegurarse de que estí© visible
        if hasattr(self, 'logo_label'):
            self.logo_label.show()
            self.logo_label.raise_()  # Traer al frente
        
        # Modificar el título del plan - incluir información de la línea actual
        linea_actual = self.linea_selector.currentText() if hasattr(self, 'linea_selector') else "N/A"
        self.title_plan.setText(f"PLAN DE PRODUCCION - LÍNEA {linea_actual} - ESCANEO ACTIVO")
        self.title_plan.setStyleSheet("""
            font-weight: bold; 
            font-size: 24px; 
            color: #ffffff; 
            background-color: #1f2d3a; 
            padding: 15px; 
            margin: 5px;
            text-align: center;
        """)
        
        # Aumentar tamaí±o de fuente de la tabla
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
        
        # Mostrar botí³n para salir del modo pantalla completa
        if hasattr(self, 'exit_fullscreen_btn'):
            self.exit_fullscreen_btn.show()
            self.exit_fullscreen_btn.raise_()  # Traer al frente
        
        # Actualizar estado
        self._fullscreen_mode = True
        
        # El campo de escaneo mantiene el foco aunque estí© invisible
        self.scan_input.setFocus()
        # Forzar que mantenga el foco de manera más agresiva
        QtCore.QTimer.singleShot(100, lambda: self.scan_input.setFocus())
        QtCore.QTimer.singleShot(500, lambda: self.scan_input.setFocus())

    def _exit_fullscreen_mode(self) -> None:
        """Salir del modo pantalla completa"""
        # Restaurar el estilo normal del campo de escaneo
        self.scan_input.setStyleSheet("")  # Eliminar estilos personalizados
        self.scan_input.show()
        
        # Mostrar el contenedor del selector de lí­nea
        if hasattr(self, 'linea_container_widget'):
            self.linea_container_widget.show()
        
        # Mostrar la barra de meníº y status
        self.menuBar().show()
        self.statusBar().show()

        # Restaurar título del plan
        self.title_plan.setText("Plan de Produccion (Línea Seleccionada)")
        self.title_plan.setStyleSheet("font-weight: bold; margin-top:8px;")
        
        # Restaurar tamaño de fuente de la tabla
        self.table_plan.setStyleSheet("""
            QTableWidget {font-size:14px; gridline-color:#2d3e50;}
            QHeaderView::section {background:#1f2d3a; color:#e0e0e0; font-weight:bold; font-size:13px; padding:4px;}
        """)
        
        # Restaurar altura de las filas
        self.table_plan.verticalHeader().setDefaultSectionSize(32)
        
        # Restaurar tamaí±o normal del logo
        if hasattr(self, 'logo_label'):
            logo_path = ROOT_DIR / "logoLogIn.png"
            if logo_path.exists():
                pix = QtGui.QPixmap(str(logo_path))
                if not pix.isNull():
                    # Tamaí±o normal del logo
                    self.logo_label.setPixmap(pix.scaledToHeight(28, QtCore.Qt.TransformationMode.SmoothTransformation))
        
        # Restaurar ventana normal
        self.showNormal()
        
        # Ocultar botí³n de salir del modo pantalla completa
        if hasattr(self, 'exit_fullscreen_btn'):
            self.exit_fullscreen_btn.hide()
        
        # Restaurar geometrí­a si se guardí³
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
            # La ventana ya se cerrí³; ignorar
            pass

    @QtCore.pyqtSlot(str, str, str)
    def _handle_scan_processed(self, linea: str, nparte: str, event: str) -> None:
        """Actualiza UI inmediatamente cuando se completa un par."""
        if event != "PAIR_COMPLETED":
            return
        try:
            # Actualizar el timestamp del íºltimo escaneo para esta lí­nea
            import time
            if linea:
                self._last_scan_time_per_line[linea] = time.time()
            
            # ðŸš€ OPTIMIZACIí“N: Actualizar cachí© de mí©tricas inmediatamente
            # (sin consultar MySQL, solo incrementa contador en SQLite)
            try:
                from ..services.metrics_cache import get_metrics_cache
                from datetime import date
                
                metrics_cache = get_metrics_cache()
                if metrics_cache and linea:
                    fecha_hoy = date.today().isoformat()
                    
                    # Obtener mí©tricas actuales del cachí©
                    cached = metrics_cache.get_metrics_from_cache(linea, fecha_hoy)
                    if cached:
                        # Incrementar produccií³n real
                        cached['produccion_real'] += 1
                        
                        # Recalcular eficiencia
                        if cached['plan_acumulado'] > 0:
                            cached['eficiencia'] = (cached['produccion_real'] / cached['plan_acumulado']) * 100
                        
                        # Actualizar cachí©
                        metrics_cache.update_metrics_instant(linea, fecha_hoy, cached)
                        
                        # âš ï¸ NO actualizar cards desde cachí© - ahora usamos cálculo directo desde SQLite
                        # El timer de _update_plan_totals() se encarga de actualizar las tarjetas
                        
                        # Cí“DIGO VIEJO (DESHABILITADO):
                        # if self.linea_selector.currentText() == linea:
                        #     self._update_cards_with_metrics(
                        #         cached['plan_total'],
                        #         cached['plan_acumulado'],  # âŒ Este valor es INCORRECTO (no usa tiempo transcurrido)
                        #         cached['produccion_real'],
                        #         cached['eficiencia'],
                        #         cached['uph'],
                        #         cached['upph']
                        #     )
                        #     logger.debug(f" Cards actualizadas instantáneamente desde cachí© tras escaneo")
            except Exception as cache_err:
                logger.debug(f"Error actualizando cachí© de mí©tricas: {cache_err}")
            
            #  ACTUALIZACIí“N INCREMENTAL: Actualizar solo la fila del nparte escaneado
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
            # Sincronizar mí©tricas si la ventana flotante está abierta
            # Esto es rápido porque solo copia valores ya calculados
            self._sync_metrics_to_widget()
        except Exception:
            pass

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """Sincronizar datos pendientes y cerrar la ventana correctamente."""
        try:
            # Mostrar mensaje de sincronización
            from PyQt6.QtWidgets import QMessageBox, QProgressDialog
            from PyQt6.QtCore import Qt
            
            progress = QProgressDialog("Sincronizando datos con MySQL...", None, 0, 0, self)
            progress.setWindowTitle("Cerrando")
            progress.setWindowModality(Qt.WindowModality.WindowModal)
            progress.setCancelButton(None)
            progress.setMinimumDuration(0)
            progress.setValue(0)
            progress.show()
            QtWidgets.QApplication.processEvents()
            
            # Sincronizar datos pendientes antes de cerrar
            if hasattr(self, '_dual_db') and self._dual_db:
                logger.info("🔄 Sincronizando datos antes de cerrar...")
                sync_result = self._dual_db.sync_before_shutdown()
                
                total_synced = (sync_result.get('scans_synced', 0) + 
                               sync_result.get('increments_synced', 0) + 
                               sync_result.get('production_synced', 0))
                
                if total_synced > 0:
                    logger.info(f"✅ {total_synced} registros sincronizados antes de cerrar")
                    progress.setLabelText(f"Sincronizados {total_synced} registros")
                    QtWidgets.QApplication.processEvents()
                else:
                    logger.info("✅ Todo estaba sincronizado")
            
            progress.close()
            
            # Desregistrar listeners
            if getattr(self, "_direct_mysql_listener_registered", False) and getattr(self, "_direct_mysql", None):
                self._direct_mysql.unregister_scan_listener(self._emit_scan_processed)
                
        except Exception as e:
            logger.error(f"Error en closeEvent: {e}")
        finally:
            super().closeEvent(event)

    def _get_line_options(self) -> list:
        """Opciones de lí­nea segíºn modo actual (ASSY/IMD)."""
        try:
            from ..config import settings as _settings
            mode = getattr(_settings, 'APP_MODE', 'ASSY').upper()
            if mode == 'IMD':
                return ["PANA A", "PANA B", "PANA C", "PANA D"]
        except Exception:
            pass
        return ["M1", "M2", "M3", "M4", "D1", "D2", "D3", "H1"]














