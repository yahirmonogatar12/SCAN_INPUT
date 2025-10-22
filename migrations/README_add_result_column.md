# Migración: Agregar Columna Result

## Fecha: 2025-10-22

## Propósito
Agregar columnas a la tabla `scans` para diferenciar entre escaneos exitosos (OK) y con error (NG).

## Columnas Agregadas
- `result` VARCHAR(10) - Indica 'OK' o 'NG'
- `error_code` INT - Código numérico del error (-8, -9, -10, etc.)
- `error_message` TEXT - Mensaje descriptivo del error

## Cómo Ejecutar

### Opción 1: MySQL Workbench
1. Abrir MySQL Workbench
2. Conectar a la base de datos
3. Abrir el archivo `add_result_column.sql`
4. Ejecutar todo el script

### Opción 2: Línea de Comandos
```bash
mysql -u tu_usuario -p tu_base_de_datos < migrations/add_result_column.sql
```

### Opción 3: Copiar y Pegar
Copiar el contenido de `add_result_column.sql` y ejecutarlo en tu cliente MySQL.

## Verificación
Después de ejecutar, deberías ver:
- 3 nuevas columnas en la tabla `scans`
- Todos los registros existentes con `result = 'OK'`
- 2 nuevos índices creados

## Rollback (Si Necesario)
Si necesitas revertir los cambios:
```sql
ALTER TABLE scans DROP COLUMN result;
ALTER TABLE scans DROP COLUMN error_code;
ALTER TABLE scans DROP COLUMN error_message;
DROP INDEX idx_scans_result ON scans;
DROP INDEX idx_scans_error_code ON scans;
```

## Impacto
- **Compatible hacia atrás**: Los registros existentes se marcarán como 'OK'
- **Sin downtime**: La migración es segura para ejecutar en producción
- **Tamaño**: Mínimo impacto en el tamaño de la tabla
