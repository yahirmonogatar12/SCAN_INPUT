from pathlib import Path
from PyQt6 import QtWidgets, QtCore, QtGui

from ..config import settings
from ..services.dual_db import get_dual_db


class LogToolsDialog(QtWidgets.QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Logs / DB Local")
        self.resize(800, 520)

        self.dual_db = get_dual_db()

        layout = QtWidgets.QVBoxLayout(self)

        # --- Logs ---
        gb_logs = QtWidgets.QGroupBox("Logs de la aplicación")
        v1 = QtWidgets.QVBoxLayout(gb_logs)
        self.txt_log = QtWidgets.QTextEdit(readOnly=True)
        self.txt_log.setLineWrapMode(QtWidgets.QTextEdit.LineWrapMode.NoWrap)
        v1.addWidget(self.txt_log)

        btns1 = QtWidgets.QHBoxLayout()
        self.btn_log_refresh = QtWidgets.QPushButton("Actualizar")
        self.btn_log_open_file = QtWidgets.QPushButton("Abrir archivo")
        self.btn_log_open_folder = QtWidgets.QPushButton("Abrir carpeta")
        btns1.addWidget(self.btn_log_refresh)
        btns1.addWidget(self.btn_log_open_file)
        btns1.addWidget(self.btn_log_open_folder)
        btns1.addStretch()
        v1.addLayout(btns1)

        layout.addWidget(gb_logs)

        # --- DB Local ---
        gb_db = QtWidgets.QGroupBox("Base de datos local (SQLite)")
        v2 = QtWidgets.QVBoxLayout(gb_db)
        self.lbl_db_path = QtWidgets.QLabel(str(settings.LOCAL_SQLITE_PATH))
        self.lbl_db_path.setTextInteractionFlags(
            QtCore.Qt.TextInteractionFlag.TextSelectableByMouse
        )
        v2.addWidget(self.lbl_db_path)

        btns2 = QtWidgets.QHBoxLayout()
        self.btn_db_change = QtWidgets.QPushButton("Cambiar…")
        self.btn_db_open_folder = QtWidgets.QPushButton("Abrir carpeta")
        btns2.addWidget(self.btn_db_change)
        btns2.addWidget(self.btn_db_open_folder)
        btns2.addStretch()
        v2.addLayout(btns2)

        layout.addWidget(gb_db)

        # Hook buttons
        self.btn_log_refresh.clicked.connect(self.load_log)
        self.btn_log_open_file.clicked.connect(self.open_log_file)
        self.btn_log_open_folder.clicked.connect(self.open_log_folder)
        self.btn_db_change.clicked.connect(self.change_db_path)
        self.btn_db_open_folder.clicked.connect(self.open_db_folder)

        self.load_log()

    # --- Logs helpers ---
    def load_log(self) -> None:
        log_file = settings.LOG_DIR / "app.log"
        if log_file.exists():
            try:
                text = log_file.read_text(encoding="utf-8", errors="ignore")
                # Mostrar solo última parte si es muy grande
                if len(text) > 200_000:
                    text = text[-200_000:]
                self.txt_log.setPlainText(text)
                self.txt_log.moveCursor(QtGui.QTextCursor.MoveOperation.End)
            except Exception as e:
                self.txt_log.setPlainText(f"No se pudo leer el log: {e}")
        else:
            self.txt_log.setPlainText("No existe app.log aún.")

    def open_log_file(self) -> None:
        log_file = settings.LOG_DIR / "app.log"
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(log_file)))

    def open_log_folder(self) -> None:
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(settings.LOG_DIR)))

    # --- DB Local helpers ---
    def change_db_path(self) -> None:
        cur = Path(str(settings.LOCAL_SQLITE_PATH))
        start_dir = str(cur.parent if cur.exists() else settings.DATA_DIR)
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Seleccionar archivo .db",
            str(Path(start_dir) / cur.name),
            "SQLite DB (*.db);;Todos los archivos (*.*)"
        )
        if not path:
            return
        try:
            new_path = Path(path)
            self.dual_db.set_sqlite_path(new_path, persist_env=True)
            self.lbl_db_path.setText(str(new_path))
            QtWidgets.QMessageBox.information(
                self,
                "DB Local",
                f"Ruta actualizada a:\n{new_path}"
            )
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "DB Local", f"No se pudo actualizar: {e}")

    def open_db_folder(self) -> None:
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(Path(self.lbl_db_path.text()).parent)))

