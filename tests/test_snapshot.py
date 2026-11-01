from datetime import date
from pathlib import Path

import duckdb

from abq_qc import db
from abq_qc.ingest import load_measurements, store_raw_file
from abq_qc.qc import run_qc
from abq_qc.snapshot import TABLES, write_snapshot
from abq_qc.summary import build_drift_trend, summarize_day

FIXTURES = Path(__file__).parent / "fixtures"


def build_db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    con = db.connect(":memory:")
    day = date(2026, 8, 1)
    content = (FIXTURES / "abq26213.dat").read_bytes()
    store_raw_file(con, tmp_path / "raw", day, "https://example.test", content, "r")
    load_measurements(con, tmp_path / "raw", day, "r", "v1")
    run_qc(con, day, "r")
    summarize_day(con, day, "r")
    build_drift_trend(con, "r")
    return con


def test_snapshot_writes_every_table_and_anchors_the_audit_chain(tmp_path):
    con = build_db(tmp_path)
    out = tmp_path / "snapshot"

    manifest = write_snapshot(con, out, "abc123")

    assert set(manifest["tables"]) == set(TABLES)
    assert manifest["tables"]["measurements"]["rows"] == 1440
    rows = duckdb.sql(f"SELECT count(*) FROM '{out / 'measurements.parquet'}'").fetchall()[0][0]
    assert rows == 1440
    last_hash = con.execute("SELECT event_hash FROM audit_log ORDER BY event_id DESC").fetchone()
    assert manifest["audit_last_hash"] == last_hash[0]
    assert (manifest["first_day"], manifest["newest_day"]) == ("2026-08-01", "2026-08-01")


def test_same_data_gives_identical_files(tmp_path):
    con = build_db(tmp_path)
    first, second = tmp_path / "a", tmp_path / "b"

    write_snapshot(con, first, "abc123")
    write_snapshot(con, second, "abc123")

    for f in first.iterdir():
        assert f.read_bytes() == (second / f.name).read_bytes(), f.name
