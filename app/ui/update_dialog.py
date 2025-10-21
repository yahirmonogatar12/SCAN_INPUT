"""
Diálogo de Actualización - Diseño moderno basado en la imagen de referencia
"""

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QTextEdit, QFrame, QSpacerItem, QSizePolicy
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QSize
from PyQt6.QtGui import QFont, QPixmap, QIcon
from pathlib import Path


class UpdateDialog(QDialog):
    """Ventana de actualización con diseño moderno"""
    
    update_accepted = pyqtSignal()
    update_rejected = pyqtSignal()
    
    def __init__(self, current_version: str, new_version: str, release_notes: str = "", parent=None):
        super().__init__(parent)
        self.current_version = current_version
        self.new_version = new_version
        self.release_notes = release_notes
        self.auto_update_seconds = 60
        self.timer = None
        
        self.setWindowTitle("Sistema de Actualización - Input Scan")
        self.setFixedSize(1000, 650)
        self.setModal(True)
        
        # Aplicar estilos
        self.load_custom_styles()
        
        self.init_ui()
        self.start_countdown()
    
    def load_custom_styles(self):
        """Aplica estilos modernos basados en la imagen de referencia"""
        self.setStyleSheet("""
            QDialog {
                background-color: #1e2730;
                color: #e0e0e0;
                font-family: 'Segoe UI', Arial, sans-serif;
            }
            
            QLabel {
                color: #e0e0e0;
                background-color: transparent;
            }
            
            QFrame#main_container {
                background-color: #2a3542;
                border-radius: 12px;
                border: 1px solid #3a4552;
            }
            
            QLabel#header_title {
                color: #ffffff;
                font-size: 18px;
                font-weight: bold;
            }
            
            QLabel#header_subtitle {
                color: #90a4ae;
                font-size: 13px;
            }
            
            QLabel#badge_new {
                background-color: #5c7cfa;
                color: white;
                padding: 6px 16px;
                border-radius: 4px;
                font-weight: 600;
                font-size: 12px;
            }
            
            QFrame#version_box {
                background-color: #232d38;
                border-radius: 8px;
                border: 1px solid #3a4552;
                padding: 20px;
            }
            
            QLabel#version_label {
                color: #90a4ae;
                font-size: 13px;
                font-weight: 500;
            }
            
            QLabel#version_number {
                color: #e0e0e0;
                font-size: 22px;
                font-weight: 600;
            }
            
            QLabel#checkmark {
                color: #51cf66;
                font-size: 20px;
            }
            
            QLabel#badge_nuevo {
                background-color: #51cf66;
                color: white;
                padding: 4px 12px;
                border-radius: 12px;
                font-weight: 600;
                font-size: 11px;
            }
            
            QLabel#badge_nuevo_gray {
                background-color: #4a5568;
                color: #90a4ae;
                padding: 4px 12px;
                border-radius: 12px;
                font-weight: 600;
                font-size: 11px;
            }
            
            QFrame#notes_section {
                background-color: transparent;
                border: none;
            }
            
            QLabel#notes_title {
                color: #ffffff;
                font-size: 15px;
                font-weight: 600;
            }
            
            QTextEdit {
                background-color: transparent;
                color: #90a4ae;
                border: none;
                font-size: 13px;
                padding: 0px;
            }
            
            QLabel#auto_save_label {
                color: #90a4ae;
                font-size: 12px;
            }
            
            QPushButton {
                background-color: #3a4552;
                color: white;
                border: 1px solid #4a5568;
                padding: 10px 24px;
                border-radius: 6px;
                font-weight: 500;
                font-size: 13px;
                min-height: 40px;
            }
            
            QPushButton:hover {
                background-color: #4a5568;
                border: 1px solid #5a6578;
            }
            
            QPushButton#btn_close {
                background-color: #3a4552;
            }
            
            QPushButton#btn_config {
                background-color: #3a4552;
            }
            
            /* Botón grande de actualización */
            QPushButton#btn_update_large {
                background-color: #5c7cfa;
                color: white;
                border: 2px solid #4c6ef5;
                padding: 12px 36px;
                border-radius: 6px;
                font-weight: 600;
                font-size: 15px;
                min-height: 50px;
            }
            
            QPushButton#btn_update_large:hover {
                background-color: #4c6ef5;
                border: 2px solid #4263eb;
            }
            
            QPushButton#btn_update_large:pressed {
                background-color: #3b5bdb;
                border: 2px solid #364fc7;
            }
            
            QLabel#link_update {
                color: #5c7cfa;
                font-size: 13px;
                text-decoration: underline;
            }
            
            QLabel#link_update:hover {
                color: #748ffc;
            }
        """)
    
    def init_ui(self):
        """Inicializa la interfaz de usuario"""
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(40, 40, 40, 40)
        
        # Contenedor principal con borde redondeado
        main_container = QFrame()
        main_container.setObjectName("main_container")
        container_layout = QVBoxLayout()
        container_layout.setContentsMargins(40, 35, 40, 35)
        container_layout.setSpacing(25)
        
        # === HEADER ===
        header_layout = QHBoxLayout()
        header_layout.setSpacing(15)
        
        # Icono verde (sin emoji)
        icon_label = QLabel("")
        icon_label.setStyleSheet("""
            background-color: #51cf66;
            border-radius: 8px;
        """)
        icon_label.setFixedSize(50, 50)
        icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header_layout.addWidget(icon_label)
        
        # Título y subtítulo
        title_layout = QVBoxLayout()
        title_layout.setSpacing(4)
        
        title = QLabel("Sistema de Actualización")
        title.setObjectName("header_title")
        title_layout.addWidget(title)
        
        subtitle = QLabel("Input Scan")
        subtitle.setObjectName("header_subtitle")
        title_layout.addWidget(subtitle)
        
        header_layout.addLayout(title_layout)
        header_layout.addStretch()
        
        # Badge "Nueva Actualización Disponible"
        badge_new = QLabel("Nueva Actualización Disponible")
        badge_new.setObjectName("badge_new")
        header_layout.addWidget(badge_new)
        
        container_layout.addLayout(header_layout)
        
        # === VERSIONES ===
        versions_layout = QHBoxLayout()
        versions_layout.setSpacing(20)
        
        # Versión Actual
        current_box = self.create_version_box(
            "Versión Actual",
            self.current_version,
            is_current=True
        )
        versions_layout.addWidget(current_box)
        
        # Última Versión Disponible
        latest_box = self.create_version_box(
            "Última Versión Disponible",
            self.new_version,
            is_current=False
        )
        versions_layout.addWidget(latest_box)
        
        container_layout.addLayout(versions_layout)
        
        # === NOTAS DE VERSIÓN ===
        notes_frame = QFrame()
        notes_frame.setObjectName("notes_section")
        notes_layout = QVBoxLayout()
        notes_layout.setSpacing(12)
        
        notes_title = QLabel("Notas de la Versión")
        notes_title.setObjectName("notes_title")
        notes_layout.addWidget(notes_title)
        
        # Lista de notas
        notes_text = QTextEdit()
        notes_text.setReadOnly(True)
        notes_text.setPlainText(self.release_notes if self.release_notes else 
            "• Sistema de auto-actualización mejorado\n"
            "• Interfaz de actualización moderna\n"
            "• Actualización automática en 60 segundos\n"
            "• Credenciales integradas"
        )
        notes_text.setMaximumHeight(100)
        notes_layout.addWidget(notes_text)
        
        notes_frame.setLayout(notes_layout)
        container_layout.addWidget(notes_frame)
        
        # === FOOTER ===
        footer_layout = QVBoxLayout()
        footer_layout.setSpacing(15)
        
        # Mensaje de actualización obligatoria
        save_layout = QHBoxLayout()
        
        self.save_label = QLabel(f"La aplicación se cerrará y el instalador se ejecutará automáticamente")
        self.save_label.setObjectName("auto_save_label")
        self.save_label.setStyleSheet("color: #90a4ae; font-weight: normal; font-size: 12px;")
        save_layout.addWidget(self.save_label)
        save_layout.addStretch()
        
        footer_layout.addLayout(save_layout)
        
        # Botón grande de actualización (centrado)
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(12)
        
        buttons_layout.addStretch()
        
        # Botón "Actualizar ahora" - grande y prominente
        self.update_btn = QPushButton("Actualizar ahora")
        self.update_btn.setObjectName("btn_update_large")
        self.update_btn.setMinimumSize(250, 50)
        self.update_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.update_btn.clicked.connect(self.accept_update)
        buttons_layout.addWidget(self.update_btn)
        
        buttons_layout.addStretch()
        
        footer_layout.addLayout(buttons_layout)
        
        container_layout.addLayout(footer_layout)
        
        main_container.setLayout(container_layout)
        main_layout.addWidget(main_container)
        
        self.setLayout(main_layout)
    
    def create_version_box(self, title: str, version: str, is_current: bool) -> QFrame:
        """Crea una caja de versión"""
        box = QFrame()
        box.setObjectName("version_box")
        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(25, 20, 25, 20)
        
        # Título
        title_label = QLabel(title)
        title_label.setObjectName("version_label")
        layout.addWidget(title_label)
        
        # Versión con indicador y badge
        version_layout = QHBoxLayout()
        version_layout.setSpacing(12)
        
        # Indicador visual (círculo verde en lugar de checkmark)
        indicator = QLabel("●")
        indicator.setObjectName("checkmark")
        version_layout.addWidget(indicator)
        
        # Número de versión
        version_label = QLabel(f"V{version}")
        version_label.setObjectName("version_number")
        version_layout.addWidget(version_label)
        
        # Badge "Nuevo" o versión secundaria
        if is_current:
            badge = QLabel(f"V{version}")
            badge.setObjectName("badge_nuevo_gray")
        else:
            badge = QLabel("Nuevo")
            badge.setObjectName("badge_nuevo")
        
        version_layout.addWidget(badge)
        version_layout.addStretch()
        
        layout.addLayout(version_layout)
        
        box.setLayout(layout)
        return box
    
    def start_countdown(self):
        """Inicia el contador regresivo"""
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_countdown)
        self.timer.start(1000)  # Cada segundo
    
    def update_countdown(self):
        """Actualiza el contador cada segundo"""
        self.auto_update_seconds -= 1
        
        # Actualizar mensaje con countdown
        self.save_label.setText(f"Actualizando automáticamente en {self.auto_update_seconds} segundos...")
        
        if self.auto_update_seconds <= 0:
            self.accept_update()
    
    def accept_update(self):
        """Usuario aceptó actualizar (o se acabó el tiempo)"""
        if self.timer:
            self.timer.stop()
        self.update_accepted.emit()
        self.accept()
    
    def closeEvent(self, event):
        """Prevenir el cierre del diálogo - actualización obligatoria"""
        event.ignore()  # No permitir cerrar el diálogo
    
    def show_settings(self):
        """Muestra configuración de actualizaciones"""
        from PyQt6.QtWidgets import QMessageBox
        
        msg = QMessageBox(self)
        msg.setWindowTitle("Configuración de Actualizaciones")
        msg.setIcon(QMessageBox.Icon.Information)
        msg.setText("Configuración de Auto-Actualización")
        msg.setInformativeText(
            "• Verificación automática: Activada\n"
            "• Instalación automática: Activada (60 segundos)\n"
            "• Instalación silenciosa: Activada\n\n"
            "Para cambiar estas opciones, contacte al administrador del sistema."
        )
        msg.exec()
