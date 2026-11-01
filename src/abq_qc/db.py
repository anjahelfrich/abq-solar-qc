"""DuckDB schema and connection. All timestamps are stored in UTC.

Tables for later steps (overrides, audit_log, ...) are added when those steps are built.
"""

from pathlib import Path

import duckdb

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
"""


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    """Open (or create) the database and make sure all tables exist."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute(SCHEMA)
    return con
