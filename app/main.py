import sys
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from PyQt6 import QtWidgets, QtGui
from PyQt6.QtCore import QTimer

from .config import settings, ROOT_DIR
from .logging_config import setup_logging
from .db import get_db
from .ui.main_window import MainWindow
from .ui.style import build_dark_palette, stylesheet

# Windows: Configurar AppUserModelID e icono para la barra de tareas
if sys.platform == 'win32':
    import ctypes
    
    # ⚡ IMPORTANTE: Establecer AppUserModelID ANTES de crear ventanas
    # Esto asegura que Windows agrupe correctamente y muestre el icono
    myappid = 'IMD.InputScan.Production.1.0'
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except Exception as e:
        logging.warning(f"No se pudo establecer AppUserModelID: {e}")


def bootstrap() -> None:
    # Logging, TZ
    setup_logging()
    logging.info(f"Arrancando aplicación Input Scan ({settings.DB_ENGINE.upper()})")

    # Inicializar base de datos
    try:
        db = get_db()
        logging.info(f"✅ Conexión {settings.DB_ENGINE.upper()} exitosa")
    except Exception as e:
        logging.error(f"❌ Error conectando a base de datos: {e}")
        raise


def main() -> None:
    bootstrap()

    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setPalette(build_dark_palette())
    app.setStyleSheet(stylesheet())
    
    # ⚡ CONFIGURAR ICONO DE LA APLICACIÓN (barra de tareas y ventana)
    icon_path = ROOT_DIR / "logoLogIn.ico"
    app_icon = None
    if icon_path.exists():
        app_icon = QtGui.QIcon(str(icon_path))
        # Establecer icono en la aplicación (barra de tareas)
        app.setWindowIcon(app_icon)
        logging.info(f"✅ Icono de aplicación cargado: {icon_path}")
    else:
        logging.warning(f"⚠️ No se encontró el icono: {icon_path}")
    
    # Inicio directo sin login (ventana principal primero)
    win = MainWindow()
    
    # Asegurar que el icono se aplique a la ventana principal
    if app_icon:
        win.setWindowIcon(app_icon)
    
    win.show()
    
    # ===================================================================================================
    # ✅ VERIFICAR ACTUALIZACIONES DE FORMA ASÍNCRONA (NO BLOQUEAR EL INICIO)
    # ===================================================================================================
    if settings.AUTO_CHECK_UPDATES:
        def check_updates_async():
            """Verifica actualizaciones en background sin bloquear la UI"""
            try:
                from .services.auto_update import AutoUpdater
                
                logging.info("🔍 Verificando actualizaciones en background...")
                updater = AutoUpdater(
                    settings.UPDATE_NETWORK_PATH, 
                    settings.APP_VERSION,
                    settings.UPDATE_NETWORK_USER,
                    settings.UPDATE_NETWORK_PASSWORD
                )
                has_update, new_version, installer_path = updater.check_for_updates()
                
                if has_update and installer_path:
                    logging.info(f"📦 Nueva versión disponible: {new_version}")
                    
                    if settings.AUTO_INSTALL_UPDATES:
                        # Mostrar diálogo moderno de actualización con countdown
                        from .ui.update_dialog import UpdateDialog
                        
                        # Obtener notas de la versión si existen
                        release_notes = "• Sistema de auto-actualización mejorado\n"
                        release_notes += "• Interfaz de actualización moderna\n"
                        release_notes += "• Actualización automática en 60 segundos\n"
                        release_notes += "• Credenciales integradas\n"
                        
                        update_dialog = UpdateDialog(
                            current_version=settings.APP_VERSION,
                            new_version=new_version,
                            release_notes=release_notes,
                            parent=win  # Usar ventana principal como parent
                        )
                        
                        if app_icon:
                            update_dialog.setWindowIcon(app_icon)
                        
                        # Conectar señales
                        def on_update_accepted():
                            logging.info("✅ Usuario aceptó actualización (o timeout)")
                            if updater.install_update(installer_path, silent=settings.SILENT_UPDATE_INSTALL):
                                sys.exit(0)
                        
                        def on_update_rejected():
                            logging.info("❌ Usuario canceló actualización")
                        
                        update_dialog.update_accepted.connect(on_update_accepted)
                        update_dialog.update_rejected.connect(on_update_rejected)
                        
                        # Mostrar diálogo (se auto-actualiza después de 60 segundos)
                        update_dialog.exec()
                        
                    else:
                        # Modo manual (sin auto-instalación)
                        from PyQt6.QtWidgets import QMessageBox
                        
                        msg = QMessageBox()
                        msg.setIcon(QMessageBox.Icon.Information)
                        msg.setWindowTitle("Actualización Disponible")
                        msg.setText(f"Nueva versión disponible: {new_version}")
                        msg.setInformativeText(f"Versión actual: {settings.APP_VERSION}\n\n¿Desea instalar la actualización ahora?")
                        msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                        msg.setDefaultButton(QMessageBox.StandardButton.Yes)
                        
                        if app_icon:
                            msg.setWindowIcon(app_icon)
                        
                        ret = msg.exec()
                        if ret == QMessageBox.StandardButton.Yes:
                            logging.info("✅ Usuario aceptó instalar actualización")
                            if updater.install_update(installer_path, silent=False):
                                # Cerrar la aplicación
                                sys.exit(0)
                        else:
                            logging.info("ℹ️ Usuario decidió no instalar actualización")
                else:
                    logging.info("✅ El programa está actualizado")
                    
            except Exception as e:
                logging.warning(f"⚠️ Error verificando actualizaciones: {e}")
        
        # ✅ Ejecutar verificación de updates 2 segundos después del inicio
        # Esto permite que la ventana se muestre primero sin bloqueos
        QTimer.singleShot(2000, check_updates_async)
    
    sys.exit(app.exec())
    sys.exit(app.exec())
