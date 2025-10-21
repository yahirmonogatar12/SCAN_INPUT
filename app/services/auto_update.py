"""
Sistema de Auto-ActualizaciÃ³n
Verifica y descarga actualizaciones desde una carpeta compartida de red
"""

import os
import sys
import shutil
import logging
import subprocess
import time
from pathlib import Path
from typing import Optional, Tuple
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class AutoUpdater:
    """Maneja la verificaciÃ³n y actualizaciÃ³n automÃ¡tica del programa"""
    
    def __init__(self, network_path: str, current_version: str, network_user: str = None, network_password: str = None):
        """
        Inicializa el actualizador
        
        Args:
            network_path: Ruta de red donde se encuentra la Ãºltima versiÃ³n (ej: \\\\SERVER\\Updates\\InputScan)
            current_version: VersiÃ³n actual del programa (ej: "1.0.0" o "2025.10.10" legacy)
            network_user: Usuario para autenticaciÃ³n de red (opcional)
            network_password: ContraseÃ±a para autenticaciÃ³n de red (opcional)
        """
        self.network_path = Path(network_path)
        self.current_version = current_version.strip()
        self.update_info_file = "update_info.json"
        self.installer_pattern = "Input_Scan_Setup_v*.exe"
        self.network_user = network_user
        self.network_password = network_password
        
    def _authenticate_network(self) -> bool:
        """
        Autentica el acceso a la carpeta de red usando credenciales
        
        Returns:
            bool: True si la autenticaciÃ³n fue exitosa o no es necesaria
        """
        if not self.network_user or not self.network_password:
            logger.info("Sin credenciales de red configuradas, intentando acceso directo")
            return True
        
        try:
            network_path_str = str(self.network_path)
            
            # Extraer la ruta base del share (ej: \\192.168.1.230\develop)
            parts = network_path_str.split('\\')
            if len(parts) >= 4:
                share_path = '\\\\' + parts[2] + '\\' + parts[3]
            else:
                share_path = network_path_str
            
            logger.info(f"Autenticando acceso a: {share_path}")
            
            # Usar comando 'net use' de Windows para autenticar
            cmd = f'net use "{share_path}" /user:{self.network_user} {self.network_password}'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0 or "ya estÃ¡ conectado" in result.stdout.lower() or "command completed successfully" in result.stdout.lower():
                logger.info(" AutenticaciÃ³n de red exitosa")
                return True
            else:
                logger.warning(f"Advertencia en autenticaciÃ³n: {result.stdout} {result.stderr}")
                # Intentar acceso de todas formas, puede que ya estÃ© autenticado
                return True
                
        except Exception as e:
            logger.error(f"Error en autenticaciÃ³n de red: {e}")
            return False
    
    def check_for_updates(self) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Verifica si hay una actualizaciÃ³n disponible
        
        Returns:
            Tuple[bool, Optional[str], Optional[str]]: 
                - True si hay actualizaciÃ³n disponible
                - VersiÃ³n disponible
                - Ruta al instalador
        """
        try:
            # Autenticar acceso a la carpeta de red
            if not self._authenticate_network():
                logger.warning("No se pudo autenticar acceso a carpeta de red")
                return False, None, None
            
            # Verificar si la carpeta de red existe
            if not self.network_path.exists():
                # ✅ Silenciar warning si es path UNC (red) - normal cuando trabajas desde casa
                if str(self.network_path).startswith("\\\\"):
                    logger.debug(f"🏠 Auto-update deshabilitado: fuera de red corporativa ({self.network_path})")
                else:
                    logger.warning(f"Carpeta de actualizaciones no accesible: {self.network_path}")
                return False, None, None
            
            # Buscar archivo de informaciÃ³n de actualizaciÃ³n
            update_info_path = self.network_path / self.update_info_file
            
            if update_info_path.exists():
                # Leer informaciÃ³n de la actualizaciÃ³n
                with open(update_info_path, 'r', encoding='utf-8') as f:
                    update_info = json.load(f)
                
                available_version = update_info.get('version', '')
                installer_name = update_info.get('installer', '')
                release_notes = update_info.get('release_notes', '')
                
                logger.info(f"VersiÃ³n actual: {self.current_version}, VersiÃ³n disponible: {available_version}")
                
                # Comparar versiones
                if self._is_newer_version(available_version, self.current_version):
                    installer_path = self.network_path / installer_name
                    
                    if installer_path.exists():
                        logger.info(f" Nueva versiÃ³n disponible: {available_version}")
                        return True, available_version, str(installer_path)
                    else:
                        logger.warning(f"Instalador no encontrado: {installer_path}")
                        return False, None, None
                else:
                    logger.info(" El programa estÃ¡ actualizado")
                    return False, None, None
            
            else:
                # MÃ©todo alternativo: buscar el instalador mÃ¡s reciente por nombre
                installers = list(self.network_path.glob(self.installer_pattern))
                
                if not installers:
                    logger.info("No se encontraron actualizaciones disponibles")
                    return False, None, None
                
                # Obtener el instalador mÃ¡s reciente
                latest_installer = max(installers, key=lambda p: p.stat().st_mtime)
                
                # Extraer versiÃ³n del nombre del archivo
                # Formato esperado: Input_Scan_Setup_v1.0.0.exe o Input_Scan_Setup_v2025.10.10.exe (legacy)
                filename = latest_installer.stem  # Sin extensiÃ³n
                version_parts = filename.split('_v')
                
                if len(version_parts) == 2:
                    available_version = version_parts[1]
                    
                    if self._is_newer_version(available_version, self.current_version):
                        logger.info(f" Nueva versiÃ³n disponible: {available_version}")
                        return True, available_version, str(latest_installer)
                
                logger.info(" El programa estÃ¡ actualizado")
                return False, None, None
                
        except Exception as e:
            logger.error(f"âŒ Error verificando actualizaciones: {e}")
            return False, None, None
    
    def _is_newer_version(self, available: str, current: str) -> bool:
        """
        Compara dos versiones en formato semÃ¡ntico X.Y.Z (ej: 1.2.0)
        TambiÃ©n soporta formato legacy YYYY.MM.DD
        
        Args:
            available: VersiÃ³n disponible
            current: VersiÃ³n actual
            
        Returns:
            bool: True si available es mÃ¡s reciente que current
        """
        try:
            # Limpiar espacios en blanco y BOM (por si acaso)
            available = available.lstrip('\ufeff').strip()
            current = current.lstrip('\ufeff').strip()
            
            # Convertir versiones a tuplas de enteros para comparaciÃ³n
            available_parts = tuple(map(int, available.split('.')))
            current_parts = tuple(map(int, current.split('.')))
            
            # Normalizar a 3 partes (agregar .0 si falta)
            available_parts = available_parts + (0,) * (3 - len(available_parts))
            current_parts = current_parts + (0,) * (3 - len(current_parts))
            
            return available_parts > current_parts
        except Exception as e:
            logger.error(f"Error comparando versiones '{available}' vs '{current}': {e}")
            return False
    
    def install_update(self, installer_path: str, silent: bool = False) -> bool:
        """
        Instala la actualizaciÃ³n
        
        Args:
            installer_path: Ruta al instalador
            silent: True para instalaciÃ³n silenciosa (sin interacciÃ³n del usuario)
            
        Returns:
            bool: True si la instalaciÃ³n se iniciÃ³ correctamente
        """
        try:
            logger.info(f"ðŸ”„ Iniciando instalaciÃ³n de actualizaciÃ³n: {installer_path}")
            logger.info(f"ðŸ“‚ Instalador existe: {Path(installer_path).exists()}")
            logger.info(f"ðŸ“Š TamaÃ±o del instalador: {Path(installer_path).stat().st_size / 1024 / 1024:.2f} MB")
            
            # Copiar el instalador a una ubicaciÃ³n temporal
            temp_installer = Path(os.environ['TEMP']) / Path(installer_path).name
            
            logger.info(f"Copiando instalador a: {temp_installer}")
            shutil.copy2(installer_path, temp_installer)
            logger.info(f" Instalador copiado. TamaÃ±o: {temp_installer.stat().st_size / 1024 / 1024:.2f} MB")
            
            # Crear un script batch para ejecutar el instalador despuÃ©s de cerrar la app
            batch_path = Path(os.environ['TEMP']) / "update_install.bat"
            
            # Preparar comando de instalaciÃ³n
            if silent:
                installer_args = '/S'
            else:
                installer_args = ''
            
            # Script batch que espera a que la app se cierre y luego instala
            batch_script = f'''@echo off
timeout /t 2 /nobreak >nul
"{temp_installer}" {installer_args}
if exist "{temp_installer}" del "{temp_installer}"
del "%~f0"
'''
            
            logger.info(f"ðŸ“ Creando script de actualizaciÃ³n: {batch_path}")
            with open(batch_path, 'w', encoding='utf-8') as f:
                f.write(batch_script)
            logger.info(f" Script batch creado.")
            
            # CrearPowerShell para lanzar el batch completamente independiente
            ps_path = Path(os.environ['TEMP']) / "launch_update.ps1"
            ps_script = f'Start-Sleep -Seconds 1; Start-Process -FilePath cmd.exe -ArgumentList "/c","{batch_path}" -WindowStyle Hidden'
            
            logger.info(f"CreandoPowerShell launcher: {ps_path}")
            with open(ps_path, 'w', encoding='utf-8') as f:
                f.write(ps_script)
            logger.info(f"PowerShell creado.")
            
            # Ejecutar elPowerShell (que lanzarÃ¡ el batch de forma independiente)
            logger.info(" EjecutandoPowerShell launcher...")
            
            proc = subprocess.Popen(
                ['powershell', '-ExecutionPolicy', 'Bypass', '-WindowStyle', 'Hidden', '-File', str(ps_path)],
                creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP,
                close_fds=True
            )
            
            logger.info(f"PowerShell lanzado con PID: {proc.pid}")
            
            # Dar un pequeÃ±o delay para que el script se inicie
            time.sleep(0.5)
            
            logger.info(" InstalaciÃ³n programada. Cerrando el programa actual...")
            
            return True
            
        except Exception as e:
            logger.error(f"âŒ Error instalando actualizaciÃ³n: {e}", exc_info=True)
            return False
    
    def create_update_info_file(self, version: str, installer_name: str, 
                                release_notes: str, output_path: Optional[Path] = None):
        """
        Crea un archivo de informaciÃ³n de actualizaciÃ³n (para uso del administrador)
        
        Args:
            version: VersiÃ³n del programa (ej: "1.0.0")
            installer_name: Nombre del archivo instalador
            release_notes: Notas de la versiÃ³n
            output_path: Ruta donde guardar el archivo (por defecto: carpeta de red)
        """
        if output_path is None:
            output_path = self.network_path
        
        update_info = {
            "version": version.strip(),
            "installer": installer_name,
            "release_notes": release_notes,
            "release_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "minimum_version": "1.0.0"  # VersiÃ³n mÃ­nima requerida para actualizaciÃ³n automÃ¡tica
        }
        
        output_file = output_path / self.update_info_file
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(update_info, f, indent=2, ensure_ascii=False)
            
            logger.info(f" Archivo de actualizaciÃ³n creado: {output_file}")
            print(f" Archivo de actualizaciÃ³n creado: {output_file}")
            
        except Exception as e:
            logger.error(f"âŒ Error creando archivo de actualizaciÃ³n: {e}")
            print(f"âŒ Error: {e}")


def check_and_update(network_path: str, current_version: str, 
                    auto_install: bool = False, silent: bool = False) -> bool:
    """
    FunciÃ³n de conveniencia para verificar e instalar actualizaciones
    
    Args:
        network_path: Ruta de red donde se encuentra la Ãºltima versiÃ³n
        current_version: VersiÃ³n actual del programa
        auto_install: True para instalar automÃ¡ticamente sin preguntar
        silent: True para instalaciÃ³n silenciosa
        
    Returns:
        bool: True si se iniciÃ³ una actualizaciÃ³n
    """
    updater = AutoUpdater(network_path, current_version)
    
    has_update, new_version, installer_path = updater.check_for_updates()
    
    if has_update and installer_path:
        logger.info(f"ðŸ“¦ Nueva versiÃ³n disponible: {new_version}")
        
        if auto_install:
            # Instalar automÃ¡ticamente
            return updater.install_update(installer_path, silent=silent)
        else:
            # Solo notificar (la UI se encargarÃ¡ de preguntar al usuario)
            return False
    
    return False

