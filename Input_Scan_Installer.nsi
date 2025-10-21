; ===================================================================================================
; INSTALADOR INPUT SCAN - SISTEMA DE ESCANEO IMD
; Sistema de versionado automático
; Compilado con: NSIS 3.x
; ===================================================================================================

; IMPORTANTE: La versión se lee automáticamente de version.txt
; Para cambiar la versión, edita version.txt y ejecuta build_installer.ps1

; Definiciones generales
!define PRODUCT_NAME "Input Scan - Sistema de Escaneo IMD"
!define /file PRODUCT_VERSION "version.txt"
!define PRODUCT_PUBLISHER "IMD - Industrial Manufacturing Division"
!define PRODUCT_WEB_SITE "https://www.imd.com"
!define PRODUCT_DIR_REGKEY "Software\Microsoft\Windows\CurrentVersion\App Paths\Input_Scan_Main.exe"
!define PRODUCT_UNINST_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
!define PRODUCT_UNINST_ROOT_KEY "HKLM"

; Configuración del instalador
Name "${PRODUCT_NAME} v${PRODUCT_VERSION}"
OutFile "installers\Input_Scan_Setup_v${PRODUCT_VERSION}.exe"
InstallDir "$PROGRAMFILES\IMD\Input Scan"
InstallDirRegKey HKLM "${PRODUCT_DIR_REGKEY}" ""
ShowInstDetails show
ShowUnInstDetails show

; Requerir privilegios de administrador
RequestExecutionLevel admin

; Interfaz moderna
!include "MUI2.nsh"

; Configuración de la interfaz MUI
!define MUI_ABORTWARNING
!define MUI_ICON "logoLogIn.ico"
!define MUI_UNICON "logoLogIn.ico"
!define MUI_WELCOMEFINISHPAGE_BITMAP "logoLogIn.ico"
!define MUI_HEADERIMAGE
!define MUI_HEADERIMAGE_BITMAP "logoLogIn.ico"
!define MUI_HEADERIMAGE_RIGHT

; Páginas del instalador
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "LICENSE.txt"
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!define MUI_FINISHPAGE_RUN "$INSTDIR\Input Scan - Sistema de Escaneo.lnk"
!define MUI_FINISHPAGE_RUN_TEXT "Ejecutar Input Scan ahora"
!insertmacro MUI_PAGE_FINISH

; Páginas del desinstalador
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

; Idiomas
!insertmacro MUI_LANGUAGE "Spanish"

; Incluir plugin para GetSize
!include "FileFunc.nsh"
!insertmacro GetSize

; ===================================================================================================
; SECCIÓN PRINCIPAL DE INSTALACIÓN
; ===================================================================================================
Section "MainSection" SEC01
  SetOutPath "$INSTDIR"
  SetOverwrite on
  
  ; Copiar todos los archivos del directorio dist\Input_Scan_Main (excluyendo carpetas temporales y bases de datos)
  File /r /x "*.db" /x "*.log" /x "*.db-shm" /x "*.db-wal" /x "__pycache__" "dist\Input_Scan_Main\*.*"
  
  ; ⚡ COPIAR ICONO EXPLÍCITAMENTE (asegurar que existe)
  ; Copiar desde la raíz del proyecto al directorio de instalación
  File "logoLogIn.ico"
  
  ; Crear carpeta _internal\data si no existe (para compatibilidad con versiones anteriores)
  CreateDirectory "$INSTDIR\_internal\data"
  
  ; Crear acceso directo en el Escritorio con icono personalizado
  ; Usar el icono embebido en el ejecutable (índice 0)
  CreateShortCut "$DESKTOP\Input Scan - Sistema de Escaneo.lnk" \
    "$INSTDIR\Input_Scan_Main.exe" \
    "" \
    "$INSTDIR\Input_Scan_Main.exe" \
    0 \
    SW_SHOWNORMAL \
    "" \
    "Input Scan - Sistema de Escaneo IMD v${PRODUCT_VERSION}"
  
  ; Crear carpeta en el Menú Inicio
  CreateDirectory "$SMPROGRAMS\Input Scan"
  
  ; Crear acceso directo en el Menú Inicio
  ; Usar el icono embebido en el ejecutable (índice 0)
  CreateShortCut "$SMPROGRAMS\Input Scan\Input Scan - Sistema de Escaneo.lnk" \
    "$INSTDIR\Input_Scan_Main.exe" \
    "" \
    "$INSTDIR\Input_Scan_Main.exe" \
    0 \
    SW_SHOWNORMAL \
    "" \
    "Input Scan - Sistema de Escaneo IMD v${PRODUCT_VERSION}"
  
  ; Crear acceso directo al desinstalador en el Menú Inicio
  CreateShortCut "$SMPROGRAMS\Input Scan\Desinstalar Input Scan.lnk" \
    "$INSTDIR\uninst.exe"
  
  ; Configurar inicio automático con Windows (opcional - comentar si no se desea)
  ; WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Run" "Input Scan IMD" "$INSTDIR\Input_Scan_Main.exe"
  
SectionEnd

; ===================================================================================================
; SECCIÓN POST-INSTALACIÓN
; ===================================================================================================
Section -Post
  WriteUninstaller "$INSTDIR\uninst.exe"
  WriteRegStr HKLM "${PRODUCT_DIR_REGKEY}" "" "$INSTDIR\Input_Scan_Main.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayName" "$(^Name)"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "UninstallString" "$INSTDIR\uninst.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayIcon" "$INSTDIR\Input_Scan_Main.exe,0"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayVersion" "${PRODUCT_VERSION}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "URLInfoAbout" "${PRODUCT_WEB_SITE}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "Publisher" "${PRODUCT_PUBLISHER}"
  
  ; Calcular tamaño estimado de la instalación
  ${GetSize} "$INSTDIR" "/S=0K" $0 $1 $2
  IntFmt $0 "0x%08X" $0
  WriteRegDWORD ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "EstimatedSize" "$0"
SectionEnd

; ===================================================================================================
; FUNCIÓN DE INICIALIZACIÓN DEL INSTALADOR
; ===================================================================================================
Function .onInit
  ; Verificar si Input Scan ya está ejecutándose
  FindWindow $0 "" "Input Scan - Sistema de Escaneo"
  IntCmp $0 0 checkProcess
    MessageBox MB_OKCANCEL|MB_ICONEXCLAMATION \
      "Input Scan está ejecutándose actualmente.$\n$\nPor favor, cierre la aplicación antes de continuar con la instalación.$\n$\n¿Desea intentar cerrarla automáticamente?" \
      IDOK tryClose IDCANCEL abort
    
    tryClose:
      SendMessage $0 ${WM_CLOSE} 0 0
      Sleep 2000
      FindWindow $0 "" "Input Scan - Sistema de Escaneo"
      IntCmp $0 0 checkProcess
        MessageBox MB_OK|MB_ICONEXCLAMATION \
          "No se pudo cerrar Input Scan.$\n$\nPor favor, ciérrelo manualmente y vuelva a ejecutar el instalador."
        Abort
  
  checkProcess:
    ; Verificar si el proceso está corriendo en segundo plano
    nsExec::ExecToStack 'tasklist /FI "IMAGENAME eq Input_Scan_Main.exe" /NH'
    Pop $0
    Pop $1
    StrCpy $2 $1 21
    StrCmp $2 "Input_Scan_Main.exe" 0 notRunning
      MessageBox MB_OK|MB_ICONEXCLAMATION \
        "Input Scan está ejecutándose en segundo plano.$\n$\nPor favor, use el Administrador de Tareas para cerrarlo:$\n1. Presione Ctrl+Shift+Esc$\n2. Busque 'Input_Scan_Main'$\n3. Finalizar tarea$\n$\nLuego vuelva a ejecutar el instalador."
      Abort
  
  notRunning:
  abort:
FunctionEnd

; ===================================================================================================
; SECCIÓN DE DESINSTALACIÓN
; ===================================================================================================
Section Uninstall
  ; Eliminar accesos directos
  Delete "$DESKTOP\Input Scan - Sistema de Escaneo.lnk"
  Delete "$SMPROGRAMS\Input Scan\Input Scan - Sistema de Escaneo.lnk"
  Delete "$SMPROGRAMS\Input Scan\Desinstalar Input Scan.lnk"
  RMDir "$SMPROGRAMS\Input Scan"
  
  ; Eliminar directorio de instalación (incluyendo subdirectorios)
  RMDir /r "$INSTDIR"
  
  ; Limpiar registro
  DeleteRegKey ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}"
  DeleteRegKey HKLM "${PRODUCT_DIR_REGKEY}"
  DeleteRegValue HKLM "Software\Microsoft\Windows\CurrentVersion\Run" "Input Scan IMD"
  
  SetAutoClose true
SectionEnd

; ===================================================================================================
; FUNCIONES DEL DESINSTALADOR
; ===================================================================================================
Function un.onInit
  MessageBox MB_ICONQUESTION|MB_YESNO|MB_DEFBUTTON2 \
    "¿Está seguro de que desea desinstalar $(^Name) y todos sus componentes?" \
    IDYES +2
  Abort
FunctionEnd

Function un.onUninstSuccess
  HideWindow
  MessageBox MB_ICONINFORMATION|MB_OK \
    "$(^Name) se desinstaló correctamente de su computadora."
FunctionEnd
