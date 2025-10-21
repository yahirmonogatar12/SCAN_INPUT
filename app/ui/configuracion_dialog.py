"""
Diálogo de Configuración General del Sistema
"""

from PyQt6 import QtWidgets, QtCore, QtGui
from typing import Optional


class ConfiguracionDialog(QtWidgets.QDialog):
    """Diálogo de configuración general del sistema"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configuración del Sistema")
        self.setModal(True)
        self.resize(500, 450)
        
        # Layout principal
        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # Título
        title = QtWidgets.QLabel("Configuración del Sistema")
        title.setStyleSheet("font-size: 16px; font-weight: bold; color: #20688C; margin-bottom: 10px;")
        layout.addWidget(title)
        
        # Crear pestañas para diferentes configuraciones
        self.tab_widget = QtWidgets.QTabWidget()
        
        # Pestaña Base de Datos
        self._create_database_tab()
        
        # Pestaña Sistema
        self._create_system_tab()
        
        # Pestaña Avanzado (agrega opciones avanzadas)
        self._create_advanced_tab()
        
        layout.addWidget(self.tab_widget)
        
        # Botones
        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addStretch()
        
        self.btn_aplicar = QtWidgets.QPushButton("Aplicar")
        self.btn_aplicar.clicked.connect(self.aplicar_cambios)
        
        self.btn_cancelar = QtWidgets.QPushButton("Cancelar")
        self.btn_cancelar.clicked.connect(self.reject)
        
        self.btn_aceptar = QtWidgets.QPushButton("Aceptar")
        self.btn_aceptar.clicked.connect(self.aceptar_cambios)
        self.btn_aceptar.setDefault(True)
        
        button_layout.addWidget(self.btn_aplicar)
        button_layout.addWidget(self.btn_cancelar)
        button_layout.addWidget(self.btn_aceptar)
        
        layout.addLayout(button_layout)
    
    def _create_database_tab(self):
        """Crear pestaña de configuración de base de datos"""
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        
        # Sección SQLite Local
        sqlite_group = QtWidgets.QGroupBox("Base de Datos Local (SQLite)")
        sqlite_layout = QtWidgets.QFormLayout(sqlite_group)
        
        # Ubicación actual
        self.sqlite_location_label = QtWidgets.QLabel("Cargando...")
        sqlite_layout.addRow("Ubicación actual:", self.sqlite_location_label)
        
        # Botón para cambiar ubicación
        self.btn_cambiar_sqlite = QtWidgets.QPushButton("Cambiar ubicación...")
        self.btn_cambiar_sqlite.clicked.connect(self.cambiar_ubicacion_sqlite)
        sqlite_layout.addRow("", self.btn_cambiar_sqlite)
        
        # Tamaño de base de datos
        self.sqlite_size_label = QtWidgets.QLabel("Calculando...")
        sqlite_layout.addRow("Tamaño:", self.sqlite_size_label)
        
        layout.addWidget(sqlite_group)
        
        # Sección MySQL Backend
        mysql_group = QtWidgets.QGroupBox("Base de Datos Backend (MySQL)")
        mysql_layout = QtWidgets.QFormLayout(mysql_group)
        
        # Estado de conexión
        self.mysql_status_label = QtWidgets.QLabel("Verificando...")
        mysql_layout.addRow("Estado:", self.mysql_status_label)
        
        # Información de sincronización
        self.sync_status_label = QtWidgets.QLabel("Verificando...")
        mysql_layout.addRow("Sincronización:", self.sync_status_label)
        
        layout.addWidget(mysql_group)
        layout.addStretch()
        
        self.tab_widget.addTab(tab, "Base de Datos")
        
        # Cargar información actual
        self._load_database_info()
    
    def _create_system_tab(self):
        """Crear pestaña de configuración del sistema"""
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        
        # Sección Rendimiento
        performance_group = QtWidgets.QGroupBox("Rendimiento")
        performance_layout = QtWidgets.QFormLayout(performance_group)
        
        # Intervalo de actualización
        self.update_interval_spin = QtWidgets.QSpinBox()
        self.update_interval_spin.setRange(1, 10)
        self.update_interval_spin.setValue(2)
        self.update_interval_spin.setSuffix(" segundos")
        performance_layout.addRow("Intervalo de actualización:", self.update_interval_spin)
        
        # Modo de escaneo
        self.scan_mode_combo = QtWidgets.QComboBox()
        self.scan_mode_combo.addItems(["Ultra-rápido (SQLite)", "Balanceado", "Tradicional (MySQL)"])
        self.scan_mode_combo.setCurrentIndex(0)
        performance_layout.addRow("Modo de escaneo:", self.scan_mode_combo)
        
        layout.addWidget(performance_group)
        
        # Sección Interfaz
        ui_group = QtWidgets.QGroupBox("Interfaz de Usuario")
        ui_layout = QtWidgets.QFormLayout(ui_group)
        
        # Línea por defecto
        from ..config import settings
        self.default_line_combo = QtWidgets.QComboBox()
        try:
            from ..config import settings as _settings
            if getattr(_settings, 'APP_MODE', 'ASSY').upper() == 'IMD':
                line_opts = ["PANA A", "PANA B", "PANA C", "PANA D"]
            else:
                line_opts = ["M1", "M2", "M3", "M4", "D1", "D2", "D3", "H1"]
        except Exception:
            line_opts = ["M1", "M2", "M3", "M4", "D1", "D2", "D3", "H1"]
        self.default_line_combo.addItems(line_opts)
        try:
            idx = self.default_line_combo.findText(getattr(settings, 'DEFAULT_LINE', line_opts[0]))
            if idx >= 0:
                self.default_line_combo.setCurrentIndex(idx)
        except Exception:
            pass
        ui_layout.addRow("Línea por defecto:", self.default_line_combo)
        
        layout.addWidget(ui_group)
        
        # ⚡ Sección Producción (NUEVO)
        production_group = QtWidgets.QGroupBox("Configuración de Producción")
        production_layout = QtWidgets.QFormLayout(production_group)
        
        # Número de personas en línea (para calcular UPPH)
        self.num_personas_spin = QtWidgets.QSpinBox()
        self.num_personas_spin.setRange(1, 100)
        self.num_personas_spin.setValue(getattr(settings, 'NUM_PERSONAS_LINEA', 14))
        self.num_personas_spin.setSuffix(" personas")
        self.num_personas_spin.setToolTip("Número de personas en la línea de producción (para calcular UPPH = UPH / Personas)")
        production_layout.addRow("Personas en línea:", self.num_personas_spin)
        
        layout.addWidget(production_group)
        
        layout.addStretch()
        
        self.tab_widget.addTab(tab, "Sistema")
    
    def _create_advanced_tab(self):
        """Crear pestaña de configuración avanzada"""
        from ..config import settings
        tab = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(tab)
        
        solo_qr_group = QtWidgets.QGroupBox("Modo de Escaneo")
        solo_qr_form = QtWidgets.QFormLayout(solo_qr_group)
        
        self.chk_solo_qr = QtWidgets.QCheckBox("SOLO QR (no requiere BARCODE; marca BARCODE como 'SOLO QR')")
        self.chk_solo_qr.setChecked(bool(getattr(settings, 'SOLO_QR_MODE', False)))
        self.chk_solo_qr.setToolTip("Cuando está activo, cada QR se contabiliza como pieza completa y el BARCODE se marca internamente como 'SOLO QR'.")
        solo_qr_form.addRow(self.chk_solo_qr)

        # Modo SUB ASSY (solo disponible en modo ASSY)
        self.chk_sub_assy = QtWidgets.QCheckBox("MODO SUB ASSY (match BARCODE con QR usando tabla raw)")
        self.chk_sub_assy.setChecked(bool(getattr(settings, 'SUB_ASSY_MODE', False)))
        self.chk_sub_assy.setToolTip("Valida que el BARCODE coincida con el QR según la relación sub_assy en tabla raw. Solo funciona en modo ASSY.")
        solo_qr_form.addRow(self.chk_sub_assy)

        # Selector de modo de operación
        self.mode_combo = QtWidgets.QComboBox()
        self.mode_combo.addItems(["ASSY", "IMD"])
        try:
            current_mode = getattr(settings, 'APP_MODE', 'ASSY').upper()
            idx = self.mode_combo.findText(current_mode)
            if idx >= 0:
                self.mode_combo.setCurrentIndex(idx)
        except Exception:
            pass
        solo_qr_form.addRow("Modo de operación:", self.mode_combo)

        # Cambiar dinámicamente las opciones de línea por defecto según el modo
        self.mode_combo.currentTextChanged.connect(self._on_mode_changed)
        # Configurar estado inicial basado en el modo actual
        self._on_mode_changed(self.mode_combo.currentText())
        
        layout.addWidget(solo_qr_group)
        layout.addStretch()
        
        self.tab_widget.addTab(tab, "Avanzado")
    
    def _load_database_info(self):
        """Cargar información actual de las bases de datos"""
        try:
            from ..services.dual_db import get_dual_db
            
            # Información SQLite
            dual_db = get_dual_db()
            sqlite_path = str(dual_db.sqlite_path)
            self.sqlite_location_label.setText(sqlite_path)
            
            # Tamaño SQLite
            import os
            if os.path.exists(sqlite_path):
                size_bytes = os.path.getsize(sqlite_path)
                if size_bytes < 1024:
                    size_str = f"{size_bytes} bytes"
                elif size_bytes < 1024 * 1024:
                    size_str = f"{size_bytes / 1024:.1f} KB"
                else:
                    size_str = f"{size_bytes / (1024 * 1024):.1f} MB"
                self.sqlite_size_label.setText(size_str)
            else:
                self.sqlite_size_label.setText("No existe")
            
            # Estado MySQL
            if dual_db._sync_worker.is_alive():
                self.mysql_status_label.setText("🟢 Conectado")
                self.sync_status_label.setText("🔄 Sincronizando automáticamente")
            else:
                self.mysql_status_label.setText("🔴 Desconectado")
                self.sync_status_label.setText("❌ Sin sincronización")
                
        except Exception as e:
            self.sqlite_location_label.setText(f"Error: {e}")
            self.sqlite_size_label.setText("Error")
            self.mysql_status_label.setText("Error")
            self.sync_status_label.setText("Error")
    
    def cambiar_ubicacion_sqlite(self):
        """Cambiar ubicación de la base de datos SQLite"""
        try:
            from ..services.dual_db import get_dual_db
            import os
            import shutil
            
            dual_db = get_dual_db()
            current_path = dual_db.sqlite_path
            current_dir = os.path.dirname(current_path)
            
            # Selector de nueva ubicación
            new_dir = QtWidgets.QFileDialog.getExistingDirectory(
                self,
                "Seleccionar nueva ubicación para base de datos local",
                current_dir
            )
            
            if new_dir:
                new_path = os.path.join(new_dir, "local_scans.db")
                
                # Confirmar cambio
                reply = QtWidgets.QMessageBox.question(
                    self,
                    "Confirmar cambio",
                    f"Nueva ubicación: {new_path}\n\n¿Mover la base de datos a esta ubicación?",
                    QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No
                )
                
                if reply == QtWidgets.QMessageBox.StandardButton.Yes:
                    # Copiar base de datos si existe
                    try:
                        os.makedirs(os.path.dirname(new_path), exist_ok=True)
                        if os.path.exists(current_path):
                            shutil.copy2(current_path, new_path)
                    except Exception as e:
                        QtWidgets.QMessageBox.warning(self, "Error", f"No se pudo copiar la base de datos: {e}")
                        return

                    # Apuntar el sistema dual a la nueva ruta y reiniciar worker
                    try:
                        dual_db.set_sqlite_path(new_path, persist_env=True)
                    except Exception as e:
                        QtWidgets.QMessageBox.warning(self, "Error", f"No se pudo activar la nueva ubicación: {e}")
                        return

                    # Actualizar UI
                    self._load_database_info()
                    QtWidgets.QMessageBox.information(self, "Base de datos actualizada", "La nueva ubicación fue activada de inmediato.")
        
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Error cambiando ubicación: {e}")
    
    # Método legado eliminado: ahora cambiamos de ubicación en caliente con set_sqlite_path
    
    def aplicar_cambios(self):
        """Aplicar cambios sin cerrar el diálogo"""
        try:
            from ..config import update_env_var, settings
            
            # Recordar estado anterior del SUB ASSY para detectar cambios
            previous_sub_assy = getattr(settings, 'SUB_ASSY_MODE', False)
            
            # Persistir SOLO_QR_MODE a .env
            enabled = self.chk_solo_qr.isChecked()
            update_env_var("SOLO_QR_MODE", "1" if enabled else "0")
            # Actualizar en memoria para efecto inmediato
            settings.SOLO_QR_MODE = bool(enabled)

            # Persistir SUB_ASSY_MODE a .env (solo si está habilitado)
            if hasattr(self, 'chk_sub_assy') and self.chk_sub_assy.isEnabled():
                sub_assy_enabled = self.chk_sub_assy.isChecked()
                update_env_var("SUB_ASSY_MODE", "1" if sub_assy_enabled else "0")
                settings.SUB_ASSY_MODE = bool(sub_assy_enabled)
            else:
                # Forzar desactivado en modo IMD
                update_env_var("SUB_ASSY_MODE", "0")
                settings.SUB_ASSY_MODE = False

            # Persistir DEFAULT_LINE y actualizar en memoria (normalizado)
            default_line = self.default_line_combo.currentText().strip().upper()
            update_env_var("DEFAULT_LINE", default_line)
            settings.DEFAULT_LINE = default_line
            
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"💾 DEFAULT_LINE guardado: '{default_line}'")

            # Persistir APP_MODE y actualizar en memoria
            app_mode = self.mode_combo.currentText().strip().upper()
            update_env_var("APP_MODE", app_mode)
            settings.APP_MODE = app_mode
            
            # ⚡ Persistir NUM_PERSONAS_LINEA (NUEVO)
            num_personas = self.num_personas_spin.value()
            update_env_var("NUM_PERSONAS_LINEA", str(num_personas))
            settings.NUM_PERSONAS_LINEA = num_personas
            
            # Forzar refresh de tablas si cambió el modo SUB ASSY
            current_sub_assy = getattr(settings, 'SUB_ASSY_MODE', False)
            if previous_sub_assy != current_sub_assy:
                print(f"SUB ASSY cambió de {previous_sub_assy} a {current_sub_assy} - Forzando refresh de tablas")
                
                # Buscar la ventana principal y forzar refresh de tablas
                try:
                    parent_window = self.parent()
                    if parent_window and hasattr(parent_window, 'force_table_refresh'):
                        parent_window.force_table_refresh()
                        print("force_table_refresh llamado exitosamente")
                    else:
                        print("No se pudo encontrar el parent o el método force_table_refresh")
                except Exception as e:
                    print(f"Error llamando force_table_refresh: {e}")
                
            QtWidgets.QMessageBox.information(self, "Configuración", "Cambios aplicados correctamente")
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Error aplicando cambios: {e}")
    
    def aceptar_cambios(self):
        """Aplicar cambios y cerrar diálogo"""
        self.aplicar_cambios()
        self.accept()

    def _on_mode_changed(self, mode: str):
        try:
            mode = (mode or "ASSY").upper()
            if mode == 'IMD':
                opts = ["PANA A", "PANA B", "PANA C", "PANA D"]
                # Deshabilitar SUB ASSY en modo IMD
                if hasattr(self, 'chk_sub_assy'):
                    self.chk_sub_assy.setEnabled(False)
                    self.chk_sub_assy.setChecked(False)
                    self.chk_sub_assy.setToolTip("SUB ASSY solo está disponible en modo ASSY")
            else:
                opts = ["M1", "M2", "M3", "M4", "D1", "D2", "D3", "H1"]
                # Habilitar SUB ASSY en modo ASSY
                if hasattr(self, 'chk_sub_assy'):
                    self.chk_sub_assy.setEnabled(True)
                    self.chk_sub_assy.setToolTip("Valida que el BARCODE coincida con el QR según la relación sub_assy en tabla raw. Solo funciona en modo ASSY.")
            current = self.default_line_combo.currentText()
            self.default_line_combo.blockSignals(True)
            self.default_line_combo.clear()
            self.default_line_combo.addItems(opts)
            # try to preserve selection
            if current in opts:
                self.default_line_combo.setCurrentText(current)
            else:
                self.default_line_combo.setCurrentIndex(0)
            self.default_line_combo.blockSignals(False)
        except Exception:
            pass