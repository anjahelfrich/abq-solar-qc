"""DuckDB schema and connection. All timestamps are stored in UTC.

Tables for later steps (qc_flags, audit_log, ...) are added when those steps are built.
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
"""


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    """Open (or create) the database and make sure all tables exist."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute(SCHEMA)
    return con
