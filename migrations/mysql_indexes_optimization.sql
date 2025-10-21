-- ========================================
-- ÍNDICES PARA OPTIMIZACIÓN DE RENDIMIENTO
-- Sistema de Escaneo IMD - Input Scan
-- ========================================
-- Ejecutar estos comandos en tu base de datos MySQL
-- para mejorar significativamente el rendimiento

-- Verificar índices existentes
-- SHOW INDEX FROM input_main;
-- SHOW INDEX FROM plan_main;

-- ========================================
-- TABLA: input_main
-- ========================================

-- Índice para búsquedas por fecha (usado en contadores y reportes)
CREATE INDEX IF NOT EXISTS idx_input_main_fecha 
ON input_main(fecha);

-- Índice compuesto para búsquedas por línea y fecha (muy común en el sistema)
CREATE INDEX IF NOT EXISTS idx_input_main_linea_fecha 
ON input_main(linea, fecha);

-- Índice para ordenamiento por timestamp (usado en logs y auditoría)
CREATE INDEX IF NOT EXISTS idx_input_main_ts 
ON input_main(ts);

-- Índice compuesto para búsquedas específicas de línea con timestamp
CREATE INDEX IF NOT EXISTS idx_input_main_linea_ts 
ON input_main(linea, ts);

-- Índice para búsquedas por número de parte
CREATE INDEX IF NOT EXISTS idx_input_main_nparte 
ON input_main(nparte);

-- Índice para búsquedas por modelo
CREATE INDEX IF NOT EXISTS idx_input_main_modelo 
ON input_main(modelo);

-- Índice compuesto optimizado para dashboard y métricas
CREATE INDEX IF NOT EXISTS idx_input_main_dashboard 
ON input_main(linea, fecha, nparte);


-- ========================================
-- TABLA: plan_main
-- ========================================

-- Índice compuesto para búsquedas de plan por línea y fecha
CREATE INDEX IF NOT EXISTS idx_plan_main_line_date 
ON plan_main(line, working_date);

-- Índice para búsquedas por fecha de trabajo
CREATE INDEX IF NOT EXISTS idx_plan_main_working_date 
ON plan_main(working_date);


-- ========================================
-- TABLA: modelo_ref
-- ========================================

-- Índice para búsquedas rápidas de modelos activos
CREATE INDEX IF NOT EXISTS idx_modelo_ref_activo 
ON modelo_ref(activo);

-- Índice para búsquedas por cliente
CREATE INDEX IF NOT EXISTS idx_modelo_ref_cliente 
ON modelo_ref(cliente);


-- ========================================
-- VERIFICACIÓN
-- ========================================
-- Después de ejecutar, verificar que los índices se crearon correctamente:

-- SELECT 
--     TABLE_NAME,
--     INDEX_NAME,
--     GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS COLUMNS
-- FROM 
--     INFORMATION_SCHEMA.STATISTICS
-- WHERE 
--     TABLE_SCHEMA = DATABASE()
--     AND TABLE_NAME IN ('input_main', 'plan_main', 'modelo_ref')
-- GROUP BY 
--     TABLE_NAME, INDEX_NAME
-- ORDER BY 
--     TABLE_NAME, INDEX_NAME;


-- ========================================
-- ANÁLISIS DE RENDIMIENTO (OPCIONAL)
-- ========================================
-- Para analizar el impacto de los índices, ejecutar:

-- EXPLAIN SELECT linea, COUNT(*) as count, MAX(ts) as last_scan_ts, MAX(nparte) as last_nparte
-- FROM input_main 
-- WHERE DATE(fecha) = CURDATE()
-- GROUP BY linea;

-- Si ves "Using index" en Extra, significa que el índice se está usando correctamente.
