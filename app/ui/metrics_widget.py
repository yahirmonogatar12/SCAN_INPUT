#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ventana flotante de métricas - Widget independiente con todas las cards
"""

from PyQt6 import QtWidgets, QtCore, QtGui
from typing import Dict, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Import ROOT_DIR to get icon path
from ..config import ROOT_DIR


class MetricsWidget(QtWidgets.QWidget):
    """
    Ventana flotante que muestra todas las métricas de producción.
    Puede moverse y redimensionarse libremente, permanece siempre visible.
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Configurar como ventana independiente y siempre visible
        self.setWindowFlags(
            QtCore.Qt.WindowType.Window |
            QtCore.Qt.WindowType.WindowStaysOnTopHint |
            QtCore.Qt.WindowType.CustomizeWindowHint |
            QtCore.Qt.WindowType.WindowTitleHint |
            QtCore.Qt.WindowType.WindowCloseButtonHint |
            QtCore.Qt.WindowType.WindowMinimizeButtonHint |
            QtCore.Qt.WindowType.WindowMaximizeButtonHint
        )
        
        self.setWindowTitle("📊 Métricas de Producción")
        
        # Configurar icono de la ventana
        icon_path = ROOT_DIR / "logoLogIn.ico"
        if icon_path.exists():
            self.setWindowIcon(QtGui.QIcon(str(icon_path)))
        
        self.setMinimumSize(800, 200)
        self.resize(1000, 220)
        
        # Estilo general de la ventana
        self.setStyleSheet("""
            QWidget {
                background-color: #2C2C2C;
                color: white;
                font-family: 'Segoe UI', Arial, sans-serif;
            }
        """)
        
        # Almacenar referencias a las cards
        self.cards: Dict[str, Dict[str, QtWidgets.QLabel]] = {}
        
        # Crear interfaz
        self._setup_ui()
        
        logger.info("🪟 Ventana flotante de métricas creada")
    
    def _setup_ui(self):
        """Configurar la interfaz de usuario"""
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Contenedor de cards (horizontal)
        cards_container = QtWidgets.QWidget()
        self.cards_layout = QtWidgets.QHBoxLayout(cards_container)
        self.cards_layout.setContentsMargins(0, 0, 0, 0)
        self.cards_layout.setSpacing(15)
        
        # Crear las 6 cards
        self._create_card("plan", "Plan", "0", "#3498DB")
        self._create_card("plan_acum", "Plan Acumulado", "0", "#9B59B6")
        self._create_card("produccion", "Producción", "0", "#27AE60")
        self._create_card("eficiencia", "Eficiencia", "0.0%", "#F39C12")
        self._create_card("uph", "UPH (última hora)", "0", "#8E44AD")
        self._create_card("upph", "UPPH", "0.00", "#E74C3C")
        
        layout.addWidget(cards_container)
        
        # Información de línea (opcional, pequeña)
        self.linea_label = QtWidgets.QLabel("Línea: --")
        self.linea_label.setStyleSheet("""
            color: #999;
            font-size: 10px;
            padding: 2px;
        """)
        self.linea_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.linea_label)
    
    def _create_card(self, key: str, title: str, value: str, color: str):
        """Crear una tarjeta de métrica"""
        card = QtWidgets.QFrame()
        card.setMinimumHeight(100)
        card.setMinimumWidth(120)
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
        
        # Título (arriba, pequeño)
        title_label = QtWidgets.QLabel(title)
        title_label.setStyleSheet("""
            color: lightgray;
            font-size: 11px;
            font-weight: 500;
            border: none;
            background: transparent;
        """)
        title_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        title_label.setWordWrap(True)
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
        
        # Guardar referencias
        self.cards[key] = {
            'frame': card,
            'title': title_label,
            'value': value_label,
            'color': color
        }
        
        # Agregar al layout
        self.cards_layout.addWidget(card)
    
    def update_metrics(self, metrics: Dict[str, str], linea: str = ""):
        """
        Actualizar los valores de las métricas
        
        Args:
            metrics: Diccionario con las métricas {key: value}
                    Ejemplo: {'plan': '100', 'produccion': '85', ...}
            linea: Nombre de la línea (opcional)
        """
        for key, value in metrics.items():
            if key in self.cards:
                self.cards[key]['value'].setText(str(value))
        
        if linea:
            self.linea_label.setText(f"Línea: {linea}")
        
        logger.debug(f"📊 Métricas actualizadas en ventana flotante: {metrics}")
    
    def update_single_metric(self, key: str, value: str):
        """Actualizar una métrica individual"""
        if key in self.cards:
            self.cards[key]['value'].setText(str(value))
    
    def closeEvent(self, event: QtGui.QCloseEvent):
        """Manejar el cierre de la ventana"""
        logger.info("🪟 Ventana flotante de métricas cerrada")
        event.accept()
    
    def showEvent(self, event: QtGui.QShowEvent):
        """Manejar cuando se muestra la ventana"""
        logger.info("🪟 Ventana flotante de métricas mostrada")
        event.accept()


class ClickableMetricsArea(QtWidgets.QFrame):
    """
    Área clicable que contiene todas las cards.
    Al hacer clic, abre la ventana flotante.
    """
    
    clicked = QtCore.pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        
        # Estilo hover
        self.setStyleSheet("""
            QFrame:hover {
                background-color: rgba(255, 255, 255, 0.02);
            }
        """)
    
    def mousePressEvent(self, event: QtGui.QMouseEvent):
        """Detectar clic en el área"""
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)
