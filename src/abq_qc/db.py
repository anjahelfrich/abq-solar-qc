"""DuckDB schema, connection, and the add-only audit log writer. All timestamps are in UTC.

Tables for later steps (daily_summary, drift_trend) are added when those steps are built.
"""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

GENESIS_HASH = "0" * 64

SCHEMA = """
-- One row per downloaded file version. A re-download with identical content adds nothing;
-- one with different content adds a row, so every version stays traceable.
CREATE TABLE IF NOT EXISTS source_files (
    sha256        VARCHAR     NOT NULL,
    file_date     DATE        NOT NULL,
    url           VARCHAR     NOT NULL,
    size_bytes    BIGINT      NOT NULL,
    downloaded_at TIMESTAMPTZ NOT NULL,
    run_id        VARCHAR     NOT NULL,
    PRIMARY KEY (file_date, downloaded_at)
);

-- One row per minute. No primary key on ts_utc on purpose: a file with duplicate minutes
-- is loaded as-is and caught by the no_duplicate_minutes asset check, not rejected.
CREATE TABLE IF NOT EXISTS measurements (
    ts_utc        TIMESTAMPTZ NOT NULL,
    zenith_deg    DOUBLE,
    ghi           DOUBLE,
    dni           DOUBLE,
    dhi           DOUBLE,
    uvb           DOUBLE,
    std_ghi       DOUBLE,
    std_dni       DOUBLE,
    std_dhi       DOUBLE,
    std_uvb       DOUBLE,
    noaa_qc_ghi   TINYINT     NOT NULL,
    noaa_qc_dni   TINYINT     NOT NULL,
    noaa_qc_dhi   TINYINT     NOT NULL,
    noaa_qc_uvb   TINYINT     NOT NULL,
    source_sha256 VARCHAR     NOT NULL,
    source_line   INTEGER     NOT NULL,
    run_id        VARCHAR     NOT NULL,
    code_version  VARCHAR     NOT NULL
);

-- One row per rule version, synced from abq_qc.qc_rules. Rows are added, never changed:
-- old flags keep pointing at the exact thresholds that produced them.
CREATE TABLE IF NOT EXISTS qc_rules (
    rule_id       VARCHAR     NOT NULL,
    version       INTEGER     NOT NULL,
    description   VARCHAR     NOT NULL,
    thresholds    JSON        NOT NULL,
    severity      VARCHAR     NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (rule_id, version)
);

-- One row per failed rule, per variable ('ghi', 'dni', 'dhi', or 'all'), per minute.
-- A minute with no rows here passed every rule.
CREATE TABLE IF NOT EXISTS qc_flags (
    ts_utc        TIMESTAMPTZ NOT NULL,
    variable      VARCHAR     NOT NULL,
    rule_id       VARCHAR     NOT NULL,
    rule_version  INTEGER     NOT NULL,
    severity      VARCHAR     NOT NULL,
    value         DOUBLE,
    run_id        VARCHAR     NOT NULL,
    FOREIGN KEY (rule_id, rule_version) REFERENCES qc_rules (rule_id, version)
);

-- Manual decisions, loaded from overrides/manual_overrides.csv (the CSV is the source of truth,
-- reviewed through git). start_utc and end_utc are both included.
--   accept:   an automatic flag of rule_id was reviewed and is a false alarm
--   flag_bad: data known to be bad (e.g. maintenance), with or without an automatic flag
-- variable 'all' covers every sensor; 'ghi'/'dni'/'dhi' covers that sensor only.
CREATE TABLE IF NOT EXISTS overrides (
    override_id   VARCHAR     PRIMARY KEY,
    start_utc     TIMESTAMPTZ NOT NULL,
    end_utc       TIMESTAMPTZ NOT NULL,
    variable      VARCHAR     NOT NULL,
    rule_id       VARCHAR,
    action        VARCHAR     NOT NULL,
    reason        VARCHAR     NOT NULL,
    author        VARCHAR     NOT NULL,
    source_sha256 VARCHAR     NOT NULL,
    run_id        VARCHAR     NOT NULL
);

-- Add-only. Written only through append_audit(). Each row's event_hash covers its content and
-- the previous row's hash, so editing or deleting an old row breaks the chain.
CREATE TABLE IF NOT EXISTS audit_log (
    event_id      BIGINT      PRIMARY KEY,
    event_time    TIMESTAMPTZ NOT NULL,
    event_type    VARCHAR     NOT NULL,
    actor         VARCHAR     NOT NULL,
    details       JSON        NOT NULL,
    run_id        VARCHAR,
    prev_hash     VARCHAR     NOT NULL,
    event_hash    VARCHAR     NOT NULL
);

-- Final status per sensor value that has an automatic flag or an override. A value not listed
-- here is good. Automatic flags are never removed: an accepted flag stays in auto_rules, and
-- only final_status changes. Flags on variable 'all' (closure, missing minute) count for every
-- sensor, but only an override on 'all' can accept them.
CREATE OR REPLACE VIEW qc_status AS
WITH sensors(variable) AS (VALUES ('ghi'), ('dni'), ('dhi')),
auto AS (
    SELECT f.ts_utc, s.variable, f.rule_id, o.override_id,
           CASE WHEN o.override_id IS NOT NULL THEN 0
                WHEN f.severity = 'missing' THEN 3
                WHEN f.severity = 'bad' THEN 2
                ELSE 1 END AS level
    FROM qc_flags f
    JOIN sensors s ON f.variable IN (s.variable, 'all')
    LEFT JOIN overrides o
      ON o.action = 'accept' AND o.rule_id = f.rule_id
     AND f.ts_utc BETWEEN o.start_utc AND o.end_utc
     AND o.variable IN (f.variable, 'all')
),
manual AS (
    SELECT m.ts_utc, s.variable, NULL AS rule_id, o.override_id, 2 AS level
    FROM overrides o
    JOIN measurements m ON m.ts_utc BETWEEN o.start_utc AND o.end_utc
    JOIN sensors s ON o.variable IN (s.variable, 'all')
    WHERE o.action = 'flag_bad'
)
SELECT ts_utc, variable,
       list_sort(list(DISTINCT rule_id) FILTER (rule_id IS NOT NULL)) AS auto_rules,
       list_sort(list(DISTINCT override_id) FILTER (override_id IS NOT NULL)) AS override_ids,
       CASE max(level) WHEN 3 THEN 'missing' WHEN 2 THEN 'bad' WHEN 1 THEN 'suspect'
            ELSE 'good' END AS final_status
FROM (SELECT * FROM auto UNION ALL SELECT * FROM manual)
GROUP BY ts_utc, variable;
"""


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    """Open (or create) the database and make sure all tables exist."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute(SCHEMA)
    return con


def append_audit(
    con: duckdb.DuckDBPyConnection,
    event_type: str,
    actor: str,
    details: dict[str, Any],
    run_id: str | None,
) -> int:
    """The only way to write to audit_log: it inserts, never updates or deletes.

    Doesn't open a transaction itself, so callers can commit the data change and its audit
    event together.
    """
    last = con.execute(
        "SELECT event_id, event_hash FROM audit_log ORDER BY event_id DESC LIMIT 1"
    ).fetchone()
    event_id, prev_hash = (last[0] + 1, last[1]) if last else (1, GENESIS_HASH)
    event_time = datetime.now(UTC)
    details_json = json.dumps(details, sort_keys=True)
    event_hash = _event_hash(
        event_id, event_time, event_type, actor, details_json, run_id, prev_hash
    )
    con.execute(
        "INSERT INTO audit_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [event_id, event_time, event_type, actor, details_json, run_id, prev_hash, event_hash],
    )
    return event_id


def verify_audit_chain(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Recompute every hash in order. Returns a list of problems (empty = chain intact).

    Catches edited and deleted rows. Removing the newest rows can't be caught from inside the
    table; the snapshot committed to git anchors the latest hash for that.
    """
    rows = con.execute(
        "SELECT event_id, event_time, event_type, actor, details::VARCHAR, run_id, "
        "prev_hash, event_hash FROM audit_log ORDER BY event_id"
    ).fetchall()
    problems = []
    expected_prev, expected_id = GENESIS_HASH, 1
    for event_id, event_time, event_type, actor, details, run_id, prev_hash, event_hash in rows:
        if event_id != expected_id:
            problems.append(f"event {expected_id} is missing (next is {event_id})")
        if prev_hash != expected_prev:
            problems.append(f"event {event_id}: prev_hash doesn't match the event before it")
        details_json = json.dumps(json.loads(details), sort_keys=True)
        actual = _event_hash(
            event_id, event_time, event_type, actor, details_json, run_id, prev_hash
        )
        if actual != event_hash:
            problems.append(f"event {event_id}: content doesn't match its hash")
        expected_prev, expected_id = event_hash, event_id + 1
    return problems


def _event_hash(
    event_id: int,
    event_time: datetime,
    event_type: str,
    actor: str,
    details_json: str,
    run_id: str | None,
    prev_hash: str,
) -> str:
    payload = json.dumps(
        [
            event_id,
            event_time.astimezone(UTC).isoformat(timespec="microseconds"),
            event_type,
            actor,
            details_json,
            run_id,
            prev_hash,
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()
