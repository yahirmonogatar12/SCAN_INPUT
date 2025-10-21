# Script de compilacion automatica
param([string]$NewVersion = $null)

$versionFile = 'version.txt'

if ($NewVersion) {
    # Escribir sin BOM usando WriteAllText
    [System.IO.File]::WriteAllText($versionFile, $NewVersion, [System.Text.UTF8Encoding]::new($false))
    $version = $NewVersion
} else {
    if (Test-Path $versionFile) {
        $version = Get-Content $versionFile -Raw
        $version = $version.Trim()
    } else {
        # Escribir sin BOM usando WriteAllText
        [System.IO.File]::WriteAllText($versionFile, '1.0.0', [System.Text.UTF8Encoding]::new($false))
        $version = '1.0.0'
    }
}

Write-Host 'Version: '$version -ForegroundColor Cyan

if (-not (Test-Path 'installers')) {
    New-Item -ItemType Directory -Path 'installers' | Out-Null
}

Write-Host 'Limpiando...' -ForegroundColor Yellow
if (Test-Path 'build') { Remove-Item 'build' -Recurse -Force -ErrorAction SilentlyContinue }
if (Test-Path 'dist') { Remove-Item 'dist' -Recurse -Force -ErrorAction SilentlyContinue }

Write-Host 'Compilando con PyInstaller...' -ForegroundColor Cyan
pyinstaller --clean --noconfirm Input_Scan_ONEDIR.spec

if ($LASTEXITCODE -eq 0) {
    Write-Host 'PyInstaller OK' -ForegroundColor Green
    Copy-Item 'CHECK.wav' -Destination 'dist\Input_Scan_Main\' -Force
    Copy-Item 'ERROR.wav' -Destination 'dist\Input_Scan_Main\' -Force
    
    # Asegurar que la carpeta installers existe y es accesible
    if (-not (Test-Path 'installers')) {
        New-Item -ItemType Directory -Path 'installers' -Force | Out-Null
    }
    
    # Eliminar instalador anterior si existe (puede estar bloqueado)
    $oldInstaller = "installers\Input_Scan_Setup_v$version.exe"
    if (Test-Path $oldInstaller) {
        try {
            Remove-Item $oldInstaller -Force -ErrorAction Stop
            Write-Host "Instalador anterior eliminado: $oldInstaller" -ForegroundColor Yellow
        } catch {
            Write-Host "ADVERTENCIA: No se pudo eliminar instalador anterior: $_" -ForegroundColor Yellow
            Write-Host "Intenta cerrar cualquier instalador abierto" -ForegroundColor Yellow
        }
    }
    
    Write-Host 'Compilando instalador NSIS...' -ForegroundColor Cyan
    Write-Host "Salida: installers\Input_Scan_Setup_v$version.exe" -ForegroundColor Gray
    & 'C:\Program Files (x86)\NSIS\makensis.exe' /V4 'Input_Scan_Installer.nsi'
    
    if ($LASTEXITCODE -eq 0) {
        $installerPath = 'installers\Input_Scan_Setup_v' + $version + '.exe'
        if (Test-Path $installerPath) {
            $size = [math]::Round((Get-Item $installerPath).Length / 1MB, 2)
            Write-Host ''
            Write-Host '===== COMPILACION EXITOSA =====' -ForegroundColor Green
            Write-Host 'Instalador: '$installerPath -ForegroundColor White
            Write-Host 'Tamano: '$size' MB' -ForegroundColor White
            
            # Generar update_info.json automáticamente
            Write-Host ''
            Write-Host 'Generando update_info.json...' -ForegroundColor Cyan
            $updateInfo = @{
                version = $version
                installer = "Input_Scan_Setup_v$version.exe"
                release_notes = "Version $version del sistema de escaneo IMD"
                release_date = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
                minimum_version = "1.0.0"
            }
            
            $updateInfoPath = 'installers\update_info.json'
            # Usar UTF8 sin BOM para evitar problemas de lectura en Python
            $updateInfoJson = $updateInfo | ConvertTo-Json
            [System.IO.File]::WriteAllText($updateInfoPath, $updateInfoJson, [System.Text.UTF8Encoding]::new($false))
            Write-Host 'Archivo de actualizacion creado: '$updateInfoPath -ForegroundColor Green
            
            # Copiar a carpeta de red para auto-actualización
            Write-Host ''
            Write-Host 'Copiando a carpeta de red...' -ForegroundColor Cyan
            $networkPath = '\\192.168.1.230\develop\MES\PRODUCCION ASSY\input_scan'
            
            try {
                if (-not (Test-Path $networkPath)) {
                    Write-Host 'Creando carpeta de red...' -ForegroundColor Yellow
                    New-Item -ItemType Directory -Path $networkPath -Force | Out-Null
                }
                
                # Copiar instalador
                $networkInstaller = Join-Path $networkPath "Input_Scan_Setup_v$version.exe"
                Copy-Item $installerPath -Destination $networkInstaller -Force
                Write-Host "Instalador copiado: $networkInstaller" -ForegroundColor Green
                
                # Copiar update_info.json
                $networkUpdateInfo = Join-Path $networkPath 'update_info.json'
                Copy-Item $updateInfoPath -Destination $networkUpdateInfo -Force
                Write-Host "update_info.json copiado: $networkUpdateInfo" -ForegroundColor Green
                
                Write-Host ''
                Write-Host '===== DISTRIBUCION COMPLETA =====' -ForegroundColor Green
                Write-Host 'Los clientes se actualizaran automaticamente' -ForegroundColor White
                
            } catch {
                Write-Host ''
                Write-Host 'ADVERTENCIA: No se pudo copiar a la red' -ForegroundColor Yellow
                Write-Host "Error: $_" -ForegroundColor Yellow
                Write-Host 'Copia manual requerida a: '$networkPath -ForegroundColor Yellow
            }
            
            Write-Host ''
            Start-Process 'explorer.exe' -ArgumentList 'installers'
        } else {
            Write-Host 'ERROR: Instalador no encontrado' -ForegroundColor Red
        }
    } else {
        Write-Host 'ERROR en NSIS' -ForegroundColor Red
    }
} else {
    Write-Host 'ERROR en PyInstaller' -ForegroundColor Red
}
