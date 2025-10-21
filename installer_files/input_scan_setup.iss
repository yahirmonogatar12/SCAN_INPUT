
; Script de Inno Setup para Input Scan - Sistema de Escaneo
; Generado automáticamente el 2025-09-29 13:15

#define MyAppName "Input Scan"
#define MyAppVersion "2025.09.29"
#define MyAppPublisher "MES - Manufacturing Execution System"
#define MyAppURL "https://github.com/yahir-sistemas/input-scan"
#define MyAppExeName "Input_Scan.exe"
#define MyAppAssocName MyAppName + " File"
#define MyAppAssocExt ".scan"
#define MyAppAssocKey StringChange(MyAppAssocName, " ", "") + MyAppAssocExt

[Setup]
; NOTA: El valor de AppId identifica de forma única esta aplicación.
; No utilice el mismo valor de AppId en instaladores para otras aplicaciones.
AppId={{F7A8B2C1-3D4E-5F6A-7B8C-9D0E1F2A3B4C}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DisableProgramGroupPage=yes
LicenseFile=LICENSE.txt
InfoBeforeFile=README_INSTALACION.txt
OutputDir=installer_output
OutputBaseFilename=Input_Scan_Setup_v2025.09.29
SetupIconFile=input_scan_icon.ico
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "quicklaunchicon"; Description: "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1; Check: not IsAdminInstallMode

[Files]
Source: "dist\Input_Scan.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "input_scan_icon.ico"; DestDir: "{app}"; Flags: ignoreversion
Source: "logoLogIn.png"; DestDir: "{app}"; Flags: ignoreversion
Source: "control_bom.css"; DestDir: "{app}"; Flags: ignoreversion
Source: ".env"; DestDir: "{app}"; Flags: ignoreversion
Source: "data\*"; DestDir: "{app}\data"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "migrations\*"; DestDir: "{app}\migrations"; Flags: ignoreversion recursesubdirs createallsubdirs
; NOTA: No use "Flags: ignoreversion" en archivos del sistema compartidos

[Registry]
Root: HKA; Subkey: "Software\Classes\{#MyAppAssocExt}\OpenWithProgids"; ValueType: string; ValueName: "{#MyAppAssocKey}"; ValueData: ""; Flags: uninsdeletevalue
Root: HKA; Subkey: "Software\Classes\{#MyAppAssocKey}"; ValueType: string; ValueName: ""; ValueData: "{#MyAppAssocName}"; Flags: uninsdeletekey
Root: HKA; Subkey: "Software\Classes\{#MyAppAssocKey}\DefaultIcon"; ValueType: string; ValueName: ""; ValueData: "{app}\{#MyAppExeName},0"
Root: HKA; Subkey: "Software\Classes\{#MyAppAssocKey}\shell\open\command"; ValueType: string; ValueName: ""; ValueData: '"{app}\{#MyAppExeName}" "%1"'
Root: HKA; Subkey: "Software\Classes\Applications\{#MyAppExeName}\SupportedTypes"; ValueType: string; ValueName: ".myp"; ValueData: ""

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon
Name: "{userappdata}\Microsoft\Internet Explorer\Quick Launch\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: quicklaunchicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\logs"
Type: filesandordirs; Name: "{app}\data\app.db"

[Code]
procedure InitializeWizard;
begin
  WizardForm.LicenseAcceptedRadio.Checked := True;
end;
