from PyQt6 import QtWidgets, QtGui, QtCore
from pathlib import Path
from ..config import ROOT_DIR
from typing import Optional

from ..services.auth import authenticate


class LoginDialog(QtWidgets.QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Input Scan - Login")
        self.setModal(True)
        self.user: Optional[str] = None
        self.setFixedSize(400, 350)
        
        # Configurar icono de la ventana
        icon_path = ROOT_DIR / "logoLogIn.ico"
        if icon_path.exists():
            self.setWindowIcon(QtGui.QIcon(str(icon_path)))

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # Logo
        logo_path = ROOT_DIR / "logoLogIn.png"
        if logo_path.exists():
            lbl_logo = QtWidgets.QLabel()
            pix = QtGui.QPixmap(str(logo_path))
            if not pix.isNull():
                lbl_logo.setPixmap(pix.scaledToWidth(140, QtCore.Qt.TransformationMode.SmoothTransformation))
                lbl_logo.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
                layout.addWidget(lbl_logo)
        form = QtWidgets.QFormLayout()

        self.username = QtWidgets.QLineEdit()
        self.password = QtWidgets.QLineEdit()
        self.password.setEchoMode(QtWidgets.QLineEdit.EchoMode.Password)

        form.addRow("Usuario:", self.username)
        form.addRow("Contraseña:", self.password)
        layout.addLayout(form)

        btns = QtWidgets.QHBoxLayout()
        self.btn_ok = QtWidgets.QPushButton("Entrar")
        self.btn_ok.setObjectName("btnConsultar")
        self.btn_cancel = QtWidgets.QPushButton("Cancelar")
        self.btn_cancel.setObjectName("btnEliminar")
        
        btns.addWidget(self.btn_ok)
        btns.addWidget(self.btn_cancel)
        layout.addLayout(btns)

        self.btn_ok.clicked.connect(self.try_login)
        self.btn_cancel.clicked.connect(self.reject)

    def try_login(self) -> None:
        u = self.username.text().strip()
        p = self.password.text()
        ok_user = authenticate(u, p)
        if ok_user:
            self.user = ok_user
            self.accept()
            return
        QtWidgets.QMessageBox.warning(self, "Login", "Credenciales inválidas")
