from PyQt6 import QtGui


def build_dark_palette() -> QtGui.QPalette:
    # BOM-inspired dark palette based on control_bom.css
    palette = QtGui.QPalette()
    base = QtGui.QColor(50, 50, 62)  # #32323E
    alt = QtGui.QColor(52, 51, 78)   # #34334E
    text = QtGui.QColor(211, 211, 211)  # lightgray
    disabled = QtGui.QColor(149, 165, 166)  # #95a5a6
    highlight = QtGui.QColor(32, 104, 140)  # #20688C

    palette.setColor(QtGui.QPalette.ColorRole.Window, base)
    palette.setColor(QtGui.QPalette.ColorRole.WindowText, text)
    palette.setColor(QtGui.QPalette.ColorRole.Base, QtGui.QColor(64, 66, 79))  # #40424F
    palette.setColor(QtGui.QPalette.ColorRole.AlternateBase, QtGui.QColor(68, 71, 90))  # #44475A
    palette.setColor(QtGui.QPalette.ColorRole.ToolTipBase, QtGui.QColor(44, 62, 80))  # #2c3e50
    palette.setColor(QtGui.QPalette.ColorRole.ToolTipText, QtGui.QColor(236, 240, 241))  # #ecf0f1
    palette.setColor(QtGui.QPalette.ColorRole.Text, text)
    palette.setColor(QtGui.QPalette.ColorRole.Button, alt)
    palette.setColor(QtGui.QPalette.ColorRole.ButtonText, text)
    palette.setColor(QtGui.QPalette.ColorRole.BrightText, QtGui.QColor(255, 255, 255))
    palette.setColor(QtGui.QPalette.ColorRole.Highlight, highlight)
    palette.setColor(QtGui.QPalette.ColorRole.HighlightedText, QtGui.QColor(255, 255, 255))

    # Disabled
    palette.setColor(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.Text, disabled)
    palette.setColor(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.ButtonText, disabled)
    return palette


def stylesheet() -> str:
    # BOM-inspired stylesheet based on control_bom.css
    return """
    /* Contenedor principal */
    QWidget { 
        font-family: 'Segoe UI', sans-serif;
        color: #d3d3d3;
        background-color: #32323E;
        font-size: 11px;
    }
    
    QMainWindow { 
        background-color: #32323E;
        color: #d3d3d3;
    }

    /* Barra de estado */
    QStatusBar { 
        background-color: #34334E;
        border-top: 1px solid #20688C;
        color: #d3d3d3;
        font-size: 10px;
    }

    /* Barra de herramientas - estilo BOM toolbar */
    QToolBar { 
        background-color: #34334E;
        border: 1px solid #20688C;
        spacing: 8px;
        padding: 8px;
        margin-bottom: 3px;
    }

    /* Contenedores tipo Card */
    QFrame#Card, QGroupBox#Card {
        background-color: #40424F;
        border: 1px solid #20688C;
        border-radius: 8px;
        padding: 6px;
    }

    /* Inputs y campos de texto */
    QLineEdit { 
        background-color: #34495e;
        color: #ecf0f1;
        border: 2px solid #2c3e50;
        border-radius: 4px;
        padding: 8px 12px;
        font-size: 12px;
        font-weight: 500;
        min-width: 300px;
    }
    
    QLineEdit:focus {
        border-color: #3498db;
        background-color: #2c3e50;
    }

    QLineEdit::placeholder {
        color: #95a5a6;
        font-style: italic;
    }

    /* ComboBox - estilo BOM dropdown */
    QComboBox {
        background-color: #2c3e50;
        color: #d3d3d3;
        border: 1px solid #34495e;
        border-radius: 4px;
        padding: 6px 8px;
        font-size: 11px;
        min-width: 200px;
    }

    QComboBox:focus {
        border-color: #3498db;
    }

    QComboBox::drop-down {
        border: none;
        width: 20px;
    }

    QComboBox::down-arrow {
        image: none;
        border-left: 5px solid transparent;
        border-right: 5px solid transparent;
        border-top: 5px solid #d3d3d3;
        margin-right: 5px;
    }

    QComboBox QAbstractItemView {
        background-color: #2c3e50;
        border: 1px solid #34495e;
        selection-background-color: #3498db;
        color: #d3d3d3;
        font-size: 11px;
    }
    
    /* Selector de línea específico */
    QComboBox[objectName="lineaSelector"] {
        background-color: #20688C;
        border: 2px solid #1a5c7a;
        color: #ffffff;
        font-weight: bold;
        font-size: 12px;
        min-height: 25px;
        padding-left: 8px;
    }
    
    QComboBox[objectName="lineaSelector"]:focus {
        border-color: #3498db;
        background-color: #2980b9;
    }

    /* Tablas - estilo BOM table */
    QTableView, QTableWidget { 
        background-color: #40424F;
        gridline-color: #5F6375;
        border: none;
        font-size: 9px;
        alternate-background-color: #44475A;
    }

    QTableView::item, QTableWidget::item {
        padding: 3px 2px;
        border-bottom: 1px solid #5F6375;
        color: #d3d3d3;
    }

    QTableView::item:selected, QTableWidget::item:selected {
        background-color: #2d5a87;
        color: #ffffff;
    }

    QTableView::item:hover, QTableWidget::item:hover {
        background-color: #485563;
    }

    /* Encabezados de tabla */
    QHeaderView::section { 
        background-color: #172A46;
        color: #ecf0f1;
        padding: 4px 3px;
        border: 1px solid #20688C;
        font-weight: 600;
        font-size: 9px;
        text-align: center;
    }

    QHeaderView::section:hover {
        background-color: #1e3a5f;
    }

    /* Botones - estilo BOM buttons */
    QPushButton { 
        border: 1px solid #20688C;
        padding: 4px 8px;
        border-radius: 3px;
        background-color: #3C3940;
        color: white;
        font-size: 10px;
        font-weight: 500;
        min-width: 60px;
    }

    QPushButton:hover {
        background-color: #2980b9;
    }

    QPushButton:pressed {
        background-color: #21618c;
    }

    /* Botones específicos según el estilo BOM */
    QPushButton#btnConsultar { background-color: #3C3940; }
    QPushButton#btnConsultar:hover { background-color: #2980b9; }

    QPushButton#btnRegistrar { background-color: #502696; }
    QPushButton#btnRegistrar:hover { background-color: #8e44ad; }

    QPushButton#btnEliminar { background-color: #e74c3c; }
    QPushButton#btnEliminar:hover { background-color: #c0392b; }

    QPushButton#btnSustituir { background-color: #e67e22; }
    QPushButton#btnSustituir:hover { background-color: #d35400; }

    QPushButton#btnExportar, QPushButton#btnImportar { background-color: #456636; }
    QPushButton#btnExportar:hover, QPushButton#btnImportar:hover { background-color: #5a7c42; }

    QPushButton#btnLimpiar { background-color: #95a5a6; }
    QPushButton#btnLimpiar:hover { background-color: #7f8c8d; }

    QPushButton#btnActualizar { background-color: #546e7a; }
    QPushButton#btnActualizar:hover { background-color: #607d8b; }

    QPushButton#btnReiniciar { background-color: #6a1b9a; }
    QPushButton#btnReiniciar:hover { background-color: #8e44ad; }

    /* TreeWidget */
    QTreeWidget { 
        background-color: #40424F;
        border: 1px solid #20688C;
        color: #d3d3d3;
        font-size: 10px;
    }
    
    QTreeWidget::item {
        padding: 4px;
        border-bottom: 1px solid #5F6375;
    }
    
    QTreeWidget::item:selected { 
        background-color: #2d5a87;
        color: white;
    }

    QTreeWidget::item:hover {
        background-color: #485563;
    }

    /* Scrollbars */
    QScrollBar:vertical {
        background-color: #34495e;
        width: 12px;
        border-radius: 6px;
    }

    QScrollBar::handle:vertical {
        background-color: #95a5a6;
        border-radius: 6px;
        min-height: 20px;
        margin: 2px;
    }

    QScrollBar::handle:vertical:hover {
        background-color: #bdc3c7;
    }

    QScrollBar:horizontal {
        background-color: #34495e;
        height: 12px;
        border-radius: 6px;
    }

    QScrollBar::handle:horizontal {
        background-color: #95a5a6;
        border-radius: 6px;
        min-width: 20px;
        margin: 2px;
    }

    QScrollBar::handle:horizontal:hover {
        background-color: #bdc3c7;
    }

    /* Menús */
    QMenuBar {
        background-color: #34334E;
        color: #d3d3d3;
        border-bottom: 1px solid #20688C;
    }

    QMenuBar::item {
        padding: 6px 12px;
        background-color: transparent;
    }

    QMenuBar::item:selected {
        background-color: #20688C;
    }

    QMenu {
        background-color: #2c3e50;
        border: 1px solid #34495e;
        color: #d3d3d3;
    }

    QMenu::item {
        padding: 8px 16px;
    }

    QMenu::item:selected {
        background-color: #3498db;
    }

    /* Checkboxes */
    QCheckBox {
        color: #d3d3d3;
        font-size: 10px;
    }

    QCheckBox::indicator {
        width: 16px;
        height: 16px;
    }

    QCheckBox::indicator:unchecked {
        border: 2px solid #95a5a6;
        background-color: #40424F;
        border-radius: 3px;
    }

    QCheckBox::indicator:checked {
        border: 2px solid #27ae60;
        background-color: #27ae60;
        border-radius: 3px;
    }

    QCheckBox::indicator:disabled {
        border-color: #7f8c8d;
        background-color: #2c3e50;
    }

    /* Splitter */
    QSplitter::handle {
        background-color: #20688C;
        margin: 2px;
    }

    QSplitter::handle:horizontal {
        width: 3px;
    }

    QSplitter::handle:vertical {
        height: 3px;
    }

    /* Tooltips */
    QToolTip {
        background-color: #2c3e50;
        color: #ecf0f1;
        border: 1px solid #34495e;
        padding: 8px 12px;
        border-radius: 4px;
        font-size: 11px;
    }
    """

