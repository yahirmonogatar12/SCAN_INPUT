PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS modelos_ref (
    nparte TEXT PRIMARY KEY,
    modelo TEXT NOT NULL,
    uph INTEGER NOT NULL,
    ct REAL NULL,
    activo INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS escaneos_smd (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    raw TEXT NOT NULL,
    tipo TEXT NOT NULL,
    fecha TEXT NOT NULL,
    lote TEXT NOT NULL,
    secuencia INTEGER NOT NULL,
    estacion TEXT NOT NULL,
    nparte TEXT NOT NULL,
    modelo TEXT NULL,
    cantidad INTEGER NOT NULL DEFAULT 1,
    usuario TEXT NOT NULL,
    UNIQUE(lote, secuencia, estacion, nparte)
);

CREATE TABLE IF NOT EXISTS produccion_diaria (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL,
    linea TEXT NOT NULL,
    nparte TEXT NOT NULL,
    modelo TEXT NULL,
    cantidad_total INTEGER NOT NULL DEFAULT 0,
    uph_target INTEGER NULL,
    UNIQUE(fecha, linea, nparte)
);

CREATE TABLE IF NOT EXISTS queue_scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw TEXT NOT NULL,
    username TEXT NOT NULL,
    ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_escaneos_ts ON escaneos_smd(ts DESC);
CREATE INDEX IF NOT EXISTS idx_escaneos_fecha ON escaneos_smd(fecha);
CREATE INDEX IF NOT EXISTS idx_pd_fecha ON produccion_diaria(fecha);
