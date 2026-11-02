"""Read-only queries on the committed Parquet snapshot. No Streamlit code here, so it's testable."""

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from abq_qc import db
from abq_qc.snapshot import TABLES

SNAPSHOT_DIR = Path(__file__).resolve().parents[1] / "snapshot"
SENSORS = ["ghi", "dni", "dhi"]
REPO_URL = "https://github.com/anjahelfrich/abq-solar-qc"


def open_snapshot(snapshot_dir: Path = SNAPSHOT_DIR) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with one view per Parquet file. Nothing is copied into memory."""
    con = duckdb.connect(":memory:")
    for table in TABLES:
        path = snapshot_dir / f"{table}.parquet"
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    con.execute("SET TimeZone = 'UTC'")
    return con


def cursor(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """A cursor for one thread (Streamlit runs each session in its own thread).

    Settings don't carry over to cursors, so UTC is set again: CAST(ts_utc AS DATE) must give
    the UTC day, like the pipeline's partitions.
    """
    cur = con.cursor()
    cur.execute("SET TimeZone = 'UTC'")
    return cur


def read_manifest(snapshot_dir: Path = SNAPSHOT_DIR) -> dict:
    return json.loads((snapshot_dir / "manifest.json").read_text())


def coverage(con, tz: str) -> tuple[date, date]:
    """First and last calendar day with data, in time zone tz."""
    first, last = con.execute("SELECT min(ts_utc), max(ts_utc) FROM measurements").fetchone()
    return first.astimezone(ZoneInfo(tz)).date(), last.astimezone(ZoneInfo(tz)).date()


# --- Overview ---------------------------------------------------------------


def daily(con) -> pd.DataFrame:
    """daily_summary plus value availability and the share of daytime minutes flagged.

    Value availability counts sensor values (1440 minutes x 3 sensors) that arrived and aren't
    NOAA's missing-value code. A gap flag on 'all' (a minute with no row) counts for 3 values.
    """
    return con.execute(
        """
        WITH gaps AS (
            SELECT CAST(ts_utc AS DATE) AS day,
                   sum(CASE variable WHEN 'all' THEN 3 ELSE 1 END) AS missing_values
            FROM qc_flags WHERE rule_id = 'gap' GROUP BY 1
        )
        SELECT d.*,
               coalesce(g.missing_values, 0) AS missing_values,
               100.0 * (1440 * 3 - coalesce(g.missing_values, 0)) / (1440 * 3)
                   AS value_availability_pct,
               100.0 * d.flagged_daytime_minutes / nullif(d.daytime_minutes, 0)
                   AS flagged_daytime_pct
        FROM daily_summary d LEFT JOIN gaps g USING (day)
        ORDER BY d.day
        """
    ).df()


def overview_tiles(daily_df: pd.DataFrame) -> dict:
    return {
        "value_availability_pct": 100.0
        * (len(daily_df) * 1440 * 3 - daily_df["missing_values"].sum())
        / (len(daily_df) * 1440 * 3),
        "days_with_gaps": int((daily_df["missing_values"] > 0).sum()),
        "days": len(daily_df),
        "flagged_daytime_pct": 100.0
        * daily_df["flagged_daytime_minutes"].sum()
        / daily_df["daytime_minutes"].sum(),
        "newest_day": daily_df["day"].max(),
    }


# --- Explore ----------------------------------------------------------------


def minutes(con, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
    """One row per minute in [start, end): values, final QC status per sensor, NOAA flags,
    lineage. A minute with no row in the file still appears (its gap flag puts it in qc_status).

    status_<sensor> is the final status after overrides; rules_<sensor> lists the automatic
    rules that fired and overrides_<sensor> the overrides that apply.
    """
    pivot = ",\n".join(
        f"any_value(final_status) FILTER (variable = '{s}') AS status_{s},\n"
        f"any_value(array_to_string(auto_rules, ';')) FILTER (variable = '{s}') AS rules_{s},\n"
        f"any_value(array_to_string(override_ids, ';')) FILTER (variable = '{s}') AS overrides_{s}"
        for s in SENSORS
    )
    select = ",\n".join(
        f"coalesce(st.status_{s}, 'good') AS status_{s}, st.rules_{s}, st.overrides_{s}"
        for s in SENSORS
    )
    return con.execute(
        f"""
        WITH st AS (
            SELECT ts_utc, {pivot}
            FROM qc_status WHERE ts_utc >= $start AND ts_utc < $end
            GROUP BY ts_utc
        ),
        ts AS (
            SELECT ts_utc FROM measurements WHERE ts_utc >= $start AND ts_utc < $end
            UNION SELECT ts_utc FROM st
        )
        SELECT ts.ts_utc, m.ghi, m.dni, m.dhi, m.zenith_deg,
               {select},
               m.noaa_qc_ghi, m.noaa_qc_dni, m.noaa_qc_dhi,
               m.source_sha256, m.source_line, f.url AS source_url, m.run_id, m.code_version
        FROM ts
        LEFT JOIN measurements m USING (ts_utc)
        LEFT JOIN st USING (ts_utc)
        LEFT JOIN (SELECT DISTINCT sha256, url FROM source_files) f
               ON f.sha256 = m.source_sha256
        ORDER BY ts.ts_utc
        """,
        {"start": start_utc, "end": end_utc},
    ).df()


def flags(con, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
    return con.execute(
        "SELECT ts_utc, variable, rule_id, rule_version, severity, value FROM qc_flags "
        "WHERE ts_utc >= ? AND ts_utc < ? ORDER BY ts_utc, variable, rule_id",
        [start_utc, end_utc],
    ).df()


def overrides(con, start_utc: datetime | None = None, end_utc: datetime | None = None):
    """Overrides that overlap [start, end), or all of them."""
    if start_utc is None:
        return con.execute("SELECT * FROM overrides ORDER BY override_id").df()
    return con.execute(
        "SELECT * FROM overrides WHERE start_utc < ? AND end_utc >= ? ORDER BY override_id",
        [end_utc, start_utc],
    ).df()


def local_day_bounds(start: date, end: date, tz: str) -> tuple[datetime, datetime]:
    """UTC range covering local calendar days start..end (both included)."""
    s = pd.Timestamp(start).tz_localize(tz)
    e = pd.Timestamp(end + timedelta(days=1)).tz_localize(tz)
    return s.tz_convert("UTC").to_pydatetime(), e.tz_convert("UTC").to_pydatetime()


def export_csv(df: pd.DataFrame, tz: str) -> bytes:
    """CSV with a local-time column first, then UTC and everything else."""
    out = df.copy()
    out.insert(0, f"time_{tz}", out["ts_utc"].dt.tz_convert(tz).map(_iso))
    out["ts_utc"] = out["ts_utc"].map(_iso)
    return out.to_csv(index=False).encode()


def _iso(ts: pd.Timestamp) -> str:
    return ts.isoformat().replace("+00:00", "Z")


# --- Drift ------------------------------------------------------------------


def drift(con) -> pd.DataFrame:
    return con.execute("SELECT * FROM drift_trend ORDER BY day").df()


# --- Audit & lineage --------------------------------------------------------


def audit_log(con) -> pd.DataFrame:
    return con.execute(
        "SELECT event_id, event_time, event_type, actor, details::VARCHAR AS details, run_id, "
        "prev_hash, event_hash FROM audit_log ORDER BY event_id DESC"
    ).df()


def lookup(con, ts_utc: datetime) -> dict | None:
    """Everything needed to trace one minute back to its source, or None if not stored."""
    row = con.execute(
        """
        SELECT m.*, f.file_date, f.url, f.size_bytes, f.downloaded_at,
               f.run_id AS download_run_id
        FROM measurements m
        LEFT JOIN source_files f ON f.sha256 = m.source_sha256
        WHERE m.ts_utc = ?
        ORDER BY f.downloaded_at LIMIT 1
        """,
        [ts_utc],
    ).df()
    if row.empty:
        return None
    status = con.execute(
        "SELECT variable, auto_rules, override_ids, final_status FROM qc_status "
        "WHERE ts_utc = ? ORDER BY variable",
        [ts_utc],
    ).df()
    return {"row": row.iloc[0].to_dict(), "status": status}


def verify_snapshot(con, snapshot_dir: Path = SNAPSHOT_DIR) -> list[str]:
    """Check the snapshot against its manifest and the audit hash chain. Empty list = verified.

    - each Parquet file's SHA-256 and row count match manifest.json
    - every audit event's hash recomputes (catches edited or deleted events)
    - the newest audit event matches the one the manifest recorded (catches removed events)
    """
    manifest = read_manifest(snapshot_dir)
    problems = []
    for table, expected in manifest["tables"].items():
        path = snapshot_dir / f"{table}.parquet"
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected["sha256"]:
            problems.append(f"{table}.parquet: SHA-256 doesn't match the manifest")
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if rows != expected["rows"]:
            problems.append(f"{table}: {rows} rows, manifest says {expected['rows']}")
    problems += db.verify_audit_chain(con)
    last = con.execute(
        "SELECT event_id, event_hash FROM audit_log ORDER BY event_id DESC LIMIT 1"
    ).fetchone()
    if last != (manifest["audit_last_event_id"], manifest["audit_last_hash"]):
        problems.append("newest audit event doesn't match the one recorded in the manifest")
    return problems


# --- QC rules ---------------------------------------------------------------


def rules(con) -> pd.DataFrame:
    return con.execute(
        "SELECT rule_id, version, severity, description, thresholds::VARCHAR AS thresholds "
        "FROM qc_rules ORDER BY rule_id, version"
    ).df()


def noaa_comparison(con) -> pd.DataFrame:
    return con.execute(
        "SELECT day, noaa_both, noaa_ours_only, noaa_theirs_only FROM daily_summary ORDER BY day"
    ).df()
