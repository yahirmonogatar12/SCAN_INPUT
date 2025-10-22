-- Migración: Agregar columna result a tabla input_main
-- Fecha: 2025-10-22
-- Propósito: Diferenciar entre escaneos OK y NG (con error)

DELIMITER $$

-- Procedimiento para agregar columnas si no existen
CREATE PROCEDURE add_columns_if_not_exists()
BEGIN
    -- Agregar columna result si no existe
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'input_main' 
        AND COLUMN_NAME = 'result'
    ) THEN
        ALTER TABLE input_main 
        ADD COLUMN result VARCHAR(10) DEFAULT 'OK' 
        COMMENT 'Resultado del escaneo: OK (exitoso) o NG (error)';
    END IF;
    
    -- Agregar columna error_code si no existe
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'input_main' 
        AND COLUMN_NAME = 'error_code'
    ) THEN
        ALTER TABLE input_main 
        ADD COLUMN error_code INT DEFAULT NULL 
        COMMENT 'Código de error: -8 (QR+QR), -9 (BC+BC), -10 (MODELO DIFERENTE), etc.';
    END IF;
    
    -- Agregar columna error_message si no existe
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'input_main' 
        AND COLUMN_NAME = 'error_message'
    ) THEN
        ALTER TABLE input_main 
        ADD COLUMN error_message TEXT DEFAULT NULL 
        COMMENT 'Mensaje de error descriptivo';
    END IF;
END$$

DELIMITER ;

-- Ejecutar el procedimiento
CALL add_columns_if_not_exists();

-- Eliminar el procedimiento
DROP PROCEDURE add_columns_if_not_exists;

-- Desactivar safe mode temporalmente para el UPDATE
SET SQL_SAFE_UPDATES = 0;

-- Actualizar registros existentes a OK (por defecto)
UPDATE input_main SET result = 'OK' WHERE result IS NULL OR result = '';

-- Reactivar safe mode
SET SQL_SAFE_UPDATES = 1;

DELIMITER $$

-- Procedimiento para crear índices si no existen
CREATE PROCEDURE add_indexes_if_not_exists()
BEGIN
    -- Crear índice result si no existe
    IF NOT EXISTS (
        SELECT * FROM information_schema.STATISTICS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'input_main' 
        AND INDEX_NAME = 'idx_input_main_result'
    ) THEN
        CREATE INDEX idx_input_main_result ON input_main(result);
    END IF;
    
    -- Crear índice error_code si no existe
    IF NOT EXISTS (
        SELECT * FROM information_schema.STATISTICS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'input_main' 
        AND INDEX_NAME = 'idx_input_main_error_code'
    ) THEN
        CREATE INDEX idx_input_main_error_code ON input_main(error_code);
    END IF;
END$$

DELIMITER ;

-- Ejecutar el procedimiento
CALL add_indexes_if_not_exists();

-- Eliminar el procedimiento
DROP PROCEDURE add_indexes_if_not_exists;

-- Verificación
SELECT 
    COUNT(*) as total_scans,
    SUM(CASE WHEN result = 'OK' THEN 1 ELSE 0 END) as ok_count,
    SUM(CASE WHEN result = 'NG' THEN 1 ELSE 0 END) as ng_count
FROM input_main;
