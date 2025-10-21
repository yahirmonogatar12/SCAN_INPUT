
; Script NSIS para Input Scan - Sistema de Escaneo
; Generado automáticamente el 2025-09-29 13:15

!define PRODUCT_NAME "Input Scan"
!define PRODUCT_VERSION "2025.09.29"
!define PRODUCT_PUBLISHER "MES - Manufacturing Execution System"
!define PRODUCT_WEB_SITE "https://github.com/yahir-sistemas/input-scan"
!define PRODUCT_DIR_REGKEY "Software\Microsoft\Windows\CurrentVersion\App Paths\Input_Scan.exe"
!define PRODUCT_UNINST_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
!define PRODUCT_UNINST_ROOT_KEY "HKLM"

; Configuración MUI
!include "MUI2.nsh"

; Configuración MUI
!define MUI_ABORTWARNING
!define MUI_ICON "..\input_scan_icon.ico"
!define MUI_UNICON "..\input_scan_icon.ico"

; Páginas del instalador
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "..\LICENSE.txt"
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

; Páginas del desinstalador
!insertmacro MUI_UNPAGE_INSTFILES

; Idiomas
!insertmacro MUI_LANGUAGE "Spanish"
!insertmacro MUI_LANGUAGE "English"

; Información del instalador
Name "${PRODUCT_NAME} ${PRODUCT_VERSION}"
OutFile "Input_Scan_Setup_v2025.09.29.exe"
InstallDir "$PROGRAMFILES64\${PRODUCT_NAME}"
InstallDirRegKey HKLM "${PRODUCT_DIR_REGKEY}" ""
ShowInstDetails show
ShowUnInstDetails show

; Sección principal
Section "MainSection" SEC01
  SetOutPath "$INSTDIR"
  SetOverwrite ifnewer
  File "..\dist\Input_Scan.exe"
  File "..\input_scan_icon.ico"
  File "..\logoLogIn.png"
  File "..\control_bom.css"
  File "..\.env"
  
  SetOutPath "$INSTDIR\data"
  File /r "..\data\*.*"
  
  SetOutPath "$INSTDIR\migrations"
  File /r "..\migrations\*.*"
  
  CreateDirectory "$SMPROGRAMS\${PRODUCT_NAME}"
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\${PRODUCT_NAME}.lnk" "$INSTDIR\Input_Scan.exe"
  CreateShortCut "$DESKTOP\${PRODUCT_NAME}.lnk" "$INSTDIR\Input_Scan.exe"
SectionEnd

; Información del desinstalador
Section -AdditionalIcons
  WriteIniStr "$INSTDIR\${PRODUCT_NAME}.url" "InternetShortcut" "URL" "${PRODUCT_WEB_SITE}"
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\Website.lnk" "$INSTDIR\${PRODUCT_NAME}.url"
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\Uninstall.lnk" "$INSTDIR\uninst.exe"
SectionEnd

Section -Post
  WriteUninstaller "$INSTDIR\uninst.exe"
  WriteRegStr HKLM "${PRODUCT_DIR_REGKEY}" "" "$INSTDIR\Input_Scan.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayName" "$(^Name)"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "UninstallString" "$INSTDIR\uninst.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayIcon" "$INSTDIR\Input_Scan.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayVersion" "${PRODUCT_VERSION}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "URLInfoAbout" "${PRODUCT_WEB_SITE}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "Publisher" "${PRODUCT_PUBLISHER}"
SectionEnd

; Función desinstalador
Function un.onUninstSuccess
  HideWindow
  MessageBox MB_ICONINFORMATION|MB_OK "$(^Name) fue removido exitosamente de su computadora."
FunctionEnd

Function un.onInit
  MessageBox MB_ICONQUESTION|MB_YESNO|MB_DEFBUTTON2 "¿Está completamente seguro que desea remover $(^Name) y todos sus componentes?" IDYES +2
  Abort
FunctionEnd

Section Uninstall
  Delete "$INSTDIR\${PRODUCT_NAME}.url"
  Delete "$INSTDIR\uninst.exe"
  Delete "$INSTDIR\Input_Scan.exe"
  Delete "$INSTDIR\input_scan_icon.ico"
  Delete "$INSTDIR\logoLogIn.png"
  Delete "$INSTDIR\control_bom.css"
  Delete "$INSTDIR\.env"

  Delete "$SMPROGRAMS\${PRODUCT_NAME}\Uninstall.lnk"
  Delete "$SMPROGRAMS\${PRODUCT_NAME}\Website.lnk"
  Delete "$DESKTOP\${PRODUCT_NAME}.lnk"
  Delete "$SMPROGRAMS\${PRODUCT_NAME}\${PRODUCT_NAME}.lnk"

  RMDir "$SMPROGRAMS\${PRODUCT_NAME}"
  RMDir /r "$INSTDIR\data"
  RMDir /r "$INSTDIR\migrations"
  RMDir /r "$INSTDIR\logs"
  RMDir "$INSTDIR"

  DeleteRegKey ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}"
  DeleteRegKey HKLM "${PRODUCT_DIR_REGKEY}"
  SetAutoClose true
SectionEnd
