"""Write the database to Parquet files for the hosted dashboard (committed to git).

Rows are sorted and the manifest has no wall-clock time, so writing the same data twice gives
byte-identical files and no git diff.
"""

import hashlib
import json
from pathlib import Path

import duckdb

# table -> sort order. qc_status is a view; it's written out as a table.
TABLES = {
    "source_files": "file_date, downloaded_at",
    "measurements": "ts_utc, source_line",
    "qc_rules": "rule_id, version",
    "qc_flags": "ts_utc, variable, rule_id",
    "overrides": "override_id",
    "qc_status": "ts_utc, variable",
    "audit_log": "event_id",
    "daily_summary": "day",
    "drift_trend": "day",
}


def write_snapshot(con: duckdb.DuckDBPyConnection, out_dir: Path, code_version: str) -> dict:
    """Write one Parquet file per table plus manifest.json. Returns the manifest.

    The manifest records the newest audit event's hash. Committed to git, it anchors the
    audit chain: deleting the newest audit rows later would no longer match it.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = {}
    for table, order in TABLES.items():
        path = out_dir / f"{table}.parquet"
        con.execute(
            f"COPY (SELECT * FROM {table} ORDER BY {order}) TO '{path}' "
            "(FORMAT parquet, COMPRESSION zstd)"
        )
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        tables[table] = {"rows": rows, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    last_event = con.execute(
        "SELECT event_id, event_hash FROM audit_log ORDER BY event_id DESC LIMIT 1"
    ).fetchone()
    first_day, last_day = con.execute("SELECT min(day), max(day) FROM daily_summary").fetchone()
    manifest = {
        "code_version": code_version,
        "first_day": first_day.isoformat() if first_day else None,
        "newest_day": last_day.isoformat() if last_day else None,
        "audit_last_event_id": last_event[0] if last_event else None,
        "audit_last_hash": last_event[1] if last_event else None,
        "tables": tables,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest
