import sqlite3
from pathlib import Path
from typing import Optional, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ..config import settings
from ..models import ParsedScan, User, ModeloRef, ScanRecord, DailyTotal


DDL = r"""
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
"""


class SQLiteDB:
    def __init__(self, sqlite_path: Path) -> None:
        self.sqlite_path = sqlite_path
        self.tz = ZoneInfo(settings.TZ)
        self.conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(DDL)
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        # Ensure produccion_diaria has nparte column and correct unique index
        cur = self.conn.execute("PRAGMA table_info(produccion_diaria)")
        cols = [row[1] for row in cur.fetchall()]
        if "nparte" not in cols:
            # Rebuild table with new schema using data from escaneos_smd
            with self.conn:
                self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS produccion_diaria_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fecha TEXT NOT NULL,
                        linea TEXT NOT NULL,
                        nparte TEXT NOT NULL,
                        modelo TEXT NULL,
                        cantidad_total INTEGER NOT NULL DEFAULT 0,
                        uph_target INTEGER NULL,
                        UNIQUE(fecha, linea, nparte)
                    );
                    """
                )
                # Populate from escaneos_smd
                self.conn.execute(
                    """
                    INSERT INTO produccion_diaria_new (fecha, linea, nparte, modelo, cantidad_total, uph_target)
                    SELECT e.fecha, e.estacion AS linea, e.nparte, MAX(e.modelo) AS modelo, SUM(e.cantidad) AS cantidad_total, NULL
                    FROM escaneos_smd e
                    GROUP BY e.fecha, e.estacion, e.nparte;
                    """
                )
                # Drop old and rename new
                self.conn.execute("DROP TABLE produccion_diaria")
                self.conn.execute("ALTER TABLE produccion_diaria_new RENAME TO produccion_diaria")

    # Usuarios
    def ensure_admin(self, username: str, password_hash: str) -> None:
        cur = self.conn.execute("SELECT id FROM usuarios WHERE username=?", (username,))
        if cur.fetchone() is None:
            self.create_user(username, password_hash, role="admin", active=True)

    def get_user_by_username(self, username: str) -> Optional[User]:
        cur = self.conn.execute(
            "SELECT id, username, role, active FROM usuarios WHERE username=?",
            (username,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return User(id=row["id"], username=row["username"], role=row["role"], active=bool(row["active"]))

    def get_user_hash(self, username: str) -> Optional[str]:
        cur = self.conn.execute("SELECT password_hash FROM usuarios WHERE username=?", (username,))
        row = cur.fetchone()
        return row[0] if row else None

    def create_user(self, username: str, password_hash: str, role: str = "admin", active: bool = True) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO usuarios(username, password_hash, role, active) VALUES (?,?,?,?)",
                (username, password_hash, role, 1 if active else 0),
            )

    # Modelos
    def get_modelo(self, nparte: str) -> Optional[ModeloRef]:
        cur = self.conn.execute(
            "SELECT nparte, modelo, uph, ct, activo FROM modelos_ref WHERE nparte=?",
            (nparte,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return ModeloRef(nparte=row["nparte"], modelo=row["modelo"], uph=row["uph"], ct=row["ct"], activo=bool(row["activo"]))

    def upsert_modelo(self, nparte: str, modelo: str, uph: int, ct: Optional[float], activo: bool = True) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO modelos_ref(nparte, modelo, uph, ct, activo)
                VALUES(?,?,?,?,?)
                ON CONFLICT(nparte) DO UPDATE SET
                    modelo=excluded.modelo,
                    uph=excluded.uph,
                    ct=excluded.ct,
                    activo=excluded.activo,
                    updated_at=datetime('now')
                """,
                (nparte, modelo, uph, ct, 1 if activo else 0),
            )

    def list_modelos(self) -> List[ModeloRef]:
        cur = self.conn.execute("SELECT nparte, modelo, uph, ct, activo FROM modelos_ref ORDER BY nparte")
        return [
            ModeloRef(nparte=row["nparte"], modelo=row["modelo"], uph=row["uph"], ct=row["ct"], activo=bool(row["activo"]))
            for row in cur.fetchall()
        ]

    def update_modelo(self, nparte: str, modelo: str, uph: int, ct: Optional[float], activo: bool) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE modelos_ref SET modelo=?, uph=?, ct=?, activo=?, updated_at=datetime('now') WHERE nparte=?",
                (modelo, uph, ct, 1 if activo else 0, nparte),
            )

    def delete_modelo(self, nparte: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM modelos_ref WHERE nparte=?", (nparte,))

    # Escaneos y totales
    def insert_scan(self, parsed: ParsedScan, username: str, modelo: Optional[str], uph: Optional[int]) -> None:
        now = datetime.now(tz=self.tz)
        ts = now.isoformat(timespec="seconds")
        today_iso = now.date().isoformat()
        with self.conn:
            # Insert escaneo; ignorar duplicados según constraint
            self.conn.execute(
                """
                INSERT OR IGNORE INTO escaneos_smd
                    (ts, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo, cantidad, usuario)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    ts,
                    parsed.raw,
                    parsed.tipo,
                    today_iso,
                    parsed.lote,
                    parsed.secuencia_int,
                    parsed.estacion,
                    parsed.nparte,
                    modelo,
                    parsed.cantidad,
                    username,
                ),
            )
            # Actualizar acumulados
            self.conn.execute(
                """
                INSERT INTO produccion_diaria(fecha, linea, nparte, modelo, cantidad_total, uph_target)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(fecha, linea, nparte) DO UPDATE SET
                    cantidad_total=produccion_diaria.cantidad_total + excluded.cantidad_total,
                    uph_target=COALESCE(excluded.uph_target, produccion_diaria.uph_target),
                    modelo=COALESCE(excluded.modelo, produccion_diaria.modelo)
                """,
                (today_iso, parsed.estacion, parsed.nparte, modelo, parsed.cantidad, uph),
            )

    def get_last_scans(self, limit: int = 50) -> List[ScanRecord]:
        cur = self.conn.execute(
            """
            SELECT id, ts, raw, lote, secuencia, estacion, nparte, modelo, cantidad, usuario
            FROM escaneos_smd
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        return [
            ScanRecord(
                id=row["id"],
                ts=row["ts"],
                raw=row["raw"],
                lote=row["lote"],
                secuencia=row["secuencia"],
                estacion=row["estacion"],
                nparte=row["nparte"],
                modelo=row["modelo"],
                cantidad=row["cantidad"],
                usuario=row["usuario"],
            )
            for row in rows
        ]

    def get_daily_totals(self, fecha_iso: str) -> List[DailyTotal]:
        # Compute UPH real as counts in the last hour grouped by (linea, nparte)
        now = datetime.now(tz=self.tz)
        ts_since = (now.replace(microsecond=0) - timedelta(hours=1)).isoformat()
        cur = self.conn.execute(
            """
            WITH uh AS (
                SELECT estacion AS linea, nparte, SUM(cantidad) AS cnt
                FROM escaneos_smd
                WHERE ts >= ?
                GROUP BY estacion, nparte
            )
            SELECT pd.fecha, pd.linea, pd.nparte, pd.modelo, pd.cantidad_total, pd.uph_target,
                   COALESCE(uh.cnt, 0) AS uph_real
            FROM produccion_diaria pd
            LEFT JOIN uh ON uh.linea = pd.linea AND uh.nparte = pd.nparte
            WHERE pd.fecha = ?
            ORDER BY pd.linea, pd.nparte
            """,
            (ts_since, fecha_iso),
        )
        rows = cur.fetchall()
        return [
            DailyTotal(
                fecha=row["fecha"],
                linea=row["linea"],
                nparte=row["nparte"],
                modelo=row["modelo"],
                cantidad_total=row["cantidad_total"],
                uph_target=row["uph_target"],
                uph_real=row["uph_real"],
            )
            for row in rows
        ]

    def queue_size(self) -> int:
        cur = self.conn.execute("SELECT COUNT(*) FROM queue_scans")
        return int(cur.fetchone()[0])

    def actualizar_estado_plan(self, part_no: str, nuevo_estado: str) -> bool:
        """
        Actualiza el estado de un plan en SQLite (implementación básica)
        
        Args:
            part_no: Número de parte del plan
            nuevo_estado: Nuevo estado (EN PROGRESO, PAUSADO, TERMINADO)
            
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        # SQLite no tiene tabla plan por defecto, pero podemos crear una implementación básica
        # o simplemente retornar True para compatibilidad
        try:
            # En SQLite usaríamos una tabla similar, pero para este caso 
            # retornamos True ya que el control principal está en MySQL
            return True
        except Exception:
            return False
