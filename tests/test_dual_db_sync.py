import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import patch

from app.config import settings
from app.services.dual_db import DualDatabaseSystem


class DummyMetricsCache:
    def start_background_sync(self, *_args, **_kwargs):
        return None


class FakeDB:
    def __init__(self):
        self.pairs = []
        self.production_updates = []

    def insert_pair_scan(self, data):
        self.pairs.append(data)

    def update_daily_production(self, **kwargs):
        self.production_updates.append(kwargs)


class FakeMySQLCursor:
    def __init__(self):
        self._last_result = [(5,)]
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        query = query.strip().lower()
        if "select count(*) from input_main" in query:
            self._last_result = [(5,)]
        elif "select count(*) from plan_main" in query:
            self._last_result = [(0,)]
        elif "select produced_count from plan_main" in query:
            self._last_result = [(0,)]
        else:
            self._last_result = [(0,)]

    def fetchone(self):
        return self._last_result[0] if self._last_result else None

    def fetchall(self):
        return self._last_result

    def close(self):
        return None


class FakeMySQLConn:
    def __init__(self):
        self.cursor_obj = FakeMySQLCursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeMySQLDB:
    def __init__(self):
        self.conn = FakeMySQLConn()

    @contextmanager
    def get_connection(self):
        yield self.conn


class FakeSQLiteCursor:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class FakeSQLiteConn:
    def __init__(self):
        self._counts = {
            "scans_local": 5,
            "production_totals_local": [],
        }

    def execute(self, query, params=()):
        q = " ".join(query.split()).lower()
        if "select count(*) from scans_local" in q:
            return FakeSQLiteCursor([(self._counts["scans_local"],)])
        if "select plan_id, produced_count" in q:
            return FakeSQLiteCursor(self._counts["production_totals_local"])
        return FakeSQLiteCursor([])

    def commit(self):
        return None


@contextmanager
def patched_dual_db(tmp_path: Path):
    settings.DATA_DIR = tmp_path
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.LOCAL_SQLITE_PATH = tmp_path / "local_cache.db"
    with patch.object(
        DualDatabaseSystem,
        "_start_sync_worker",
        lambda self: None,
    ), patch("app.services.dual_db.init_metrics_cache", return_value=DummyMetricsCache()):
        db = DualDatabaseSystem()
        db.metrics_cache = DummyMetricsCache()
        try:
            yield db
        finally:
            db.stop_sync_worker()


class DualDBSyncTests(unittest.TestCase):
    def test_sync_scans_to_mysql_marks_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            with patched_dual_db(tmp_path) as dual_db:
                fake_db = FakeDB()

                with dual_db._get_sqlite_connection() as conn:
                    qr_id = conn.execute(
                        """
                        INSERT INTO scans_local
                        (ts, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo,
                         cantidad, linea, scan_format, barcode_sequence, is_complete)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'QR', ?, 1)
                        """,
                        (
                            "2025-10-28T07:53:02.541789-06:00",
                            "QR-TEST-PAIR-1",
                            "ASSY",
                            "2025-10-28",
                            "LOT-1",
                            1,
                            "ST01",
                            "NPART-1",
                            "MODEL-X",
                            1,
                            "M1",
                            1,
                        ),
                    ).lastrowid
                    bc_id = conn.execute(
                        """
                        INSERT INTO scans_local
                        (ts, raw, tipo, fecha, lote, secuencia, estacion, nparte, modelo,
                         cantidad, linea, scan_format, barcode_sequence, is_complete)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'BARCODE', ?, 1)
                        """,
                        (
                            "2025-10-28T07:53:02.541789-06:00",
                            "BC-TEST-PAIR-1",
                            "ASSY",
                            "2025-10-28",
                            "LOT-1",
                            1,
                            "ST01",
                            "NPART-1",
                            "MODEL-X",
                            1,
                            "M1",
                            1,
                        ),
                    ).lastrowid
                    conn.execute(
                        "UPDATE scans_local SET linked_scan_id = ? WHERE id = ?",
                        (bc_id, qr_id),
                    )
                    conn.execute(
                        "UPDATE scans_local SET linked_scan_id = ? WHERE id = ?",
                        (qr_id, bc_id),
                    )
                    conn.commit()

                with patch("app.db.get_db", return_value=fake_db):
                    synced = dual_db._sync_scans_to_mysql()

                self.assertEqual(synced, 1)
                self.assertEqual(len(fake_db.pairs), 1)

                with dual_db._get_sqlite_connection() as conn:
                    rows = conn.execute(
                        "SELECT synced_to_mysql FROM scans_local WHERE id IN (?, ?)",
                        (qr_id, bc_id),
                    ).fetchall()
                self.assertEqual([row[0] for row in rows], [1, 1])

    def test_sync_before_shutdown_flushes_pending_pairs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            with patched_dual_db(tmp_path) as dual_db:
                fake_mysql = FakeMySQLDB()
                fake_sqlite_conn = FakeSQLiteConn()

                def fake_sqlite_context(*_args, **_kwargs):
                    class _Ctx:
                        def __enter__(self_inner):
                            return fake_sqlite_conn

                        def __exit__(self_inner, exc_type, exc, tb):
                            return False

                    return _Ctx()

                with patch.object(
                    dual_db,
                    "_aplicar_scans_pendientes",
                    return_value=None,
                ), patch.object(
                    dual_db,
                    "_repair_unlinked_pairs",
                    return_value=(0, 0),
                ), patch.object(
                    dual_db,
                    "_sync_scans_to_mysql",
                    side_effect=[4, 3, 0],
                ), patch.object(
                    dual_db,
                    "_get_sqlite_connection",
                    side_effect=fake_sqlite_context,
                ), patch(
                    "app.db.get_db",
                    return_value=fake_mysql,
                ):
                    result = dual_db.sync_before_shutdown()

                self.assertEqual(result["scans_synced"], 7)
                self.assertEqual(result["errors"], [])


if __name__ == "__main__":
    unittest.main()
