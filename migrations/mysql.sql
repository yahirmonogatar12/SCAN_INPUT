-- Migración completa a MySQL (InnoDB, utf8mb4) - SIN LOGIN
SET NAMES utf8mb4;
SET time_zone = '+00:00';

-- Ya no se necesita tabla de usuarios (sin login)

-- Tabla modelos_ref eliminada - se consulta tabla externa 'raw' (solo lectura)
-- La tabla 'raw' ya existe en el MES con estructura:
-- part_no, model, project, main_display, c_t, uph, etc.

CREATE TABLE IF NOT EXISTS input_main (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  raw VARCHAR(128) NOT NULL,
  tipo CHAR(1) NOT NULL,
  fecha DATE NOT NULL,
  lote VARCHAR(32) NOT NULL,
  secuencia INT NOT NULL,
  estacion VARCHAR(32) NOT NULL,
  nparte VARCHAR(64) NOT NULL,
  modelo VARCHAR(128) NULL,
  cantidad INT NOT NULL DEFAULT 1,
  linea ENUM('M1', 'M2', 'M3', 'M4', 'D1', 'DD2', 'DD3', 'H1') NOT NULL DEFAULT 'M1',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_escaneo (lote, secuencia, estacion, nparte),
  INDEX idx_fecha (fecha),
  INDEX idx_nparte (nparte),
  INDEX idx_ts (ts),
  INDEX idx_linea (linea)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS produccion_main_input (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fecha DATE NOT NULL,
  linea VARCHAR(32) NOT NULL,
  nparte VARCHAR(64) NOT NULL,
  modelo VARCHAR(128) NULL,
  cantidad_total INT NOT NULL DEFAULT 0,
  uph_target INT NULL,
  uph_real DECIMAL(10,2) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_pd (fecha, linea, nparte),
  INDEX idx_fecha_linea (fecha, linea)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- queue_scans eliminado de MySQL - ahora es caché local en memoria
-- Los reintentos se manejarán en memoria/archivo temporal local
