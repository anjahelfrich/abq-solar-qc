"""Store raw SOLRAD files and load them into `measurements`, with lineage on every row.

Plain functions (no Dagster) so they can be tested directly. The Dagster assets in
`abq_qc.assets.ingest` are thin wrappers around them.
"""

import hashlib
import subprocess
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd

from abq_qc.parse import SolradFile, StationHeader, parse_solrad

STATION_LATITUDE = 35.03796
STATION_LONGITUDE = -106.62211
MINUTES_PER_DAY = 1440


@dataclass(frozen=True)
class StoredFile:
    sha256: str
    status: Literal["new", "unchanged", "changed"]
    previous_sha256: str | None
    path: Path


@dataclass(frozen=True)
class LoadedDay:
    sha256: str
    parsed: SolradFile


def raw_file_path(raw_dir: Path, file_date: date, sha256: str) -> Path:
    """Raw files are named by their hash, so any file can be checked by re-hashing it."""
    return raw_dir / file_date.isoformat() / f"{sha256}.dat"


def store_raw_file(
    con: duckdb.DuckDBPyConnection,
    raw_dir: Path,
    file_date: date,
    url: str,
    content: bytes,
    run_id: str,
) -> StoredFile:
    """Save the downloaded bytes unchanged and record a new version if the content changed."""
    sha256 = hashlib.sha256(content).hexdigest()
    path = raw_file_path(raw_dir, file_date, sha256)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(content)
        tmp.rename(path)

    previous = _current_sha256(con, file_date)
    if previous == sha256:
        return StoredFile(sha256, "unchanged", previous, path)

    con.execute(
        "INSERT INTO source_files VALUES (?, ?, ?, ?, ?, ?)",
        [sha256, file_date, url, len(content), datetime.now(UTC), run_id],
    )
    return StoredFile(sha256, "new" if previous is None else "changed", previous, path)


def load_measurements(
    con: duckdb.DuckDBPyConnection,
    raw_dir: Path,
    file_date: date,
    run_id: str,
    code_version: str,
) -> LoadedDay:
    """Parse the current version of one day's file and replace that day's rows.

    Re-running a day gives the same result (delete + insert in one transaction).
    """
    sha256 = _current_sha256(con, file_date)
    if sha256 is None:
        raise LookupError(f"no source file recorded for {file_date}; run raw_solrad_file first")

    content = raw_file_path(raw_dir, file_date, sha256).read_bytes()
    if hashlib.sha256(content).hexdigest() != sha256:
        raise ValueError(f"raw file for {file_date} no longer matches its recorded sha256")

    parsed = parse_solrad(content.decode("ascii"))
    start = datetime.combine(file_date, datetime.min.time(), tzinfo=UTC)
    end = start + timedelta(days=1)
    rows = parsed.rows
    outside = rows[(rows["ts_utc"] < start) | (rows["ts_utc"] >= end)]
    if not outside.empty:
        line = outside["source_line"].iloc[0]
        raise ValueError(f"file for {file_date} has rows from another day (line {line})")

    rows = rows.assign(source_sha256=sha256, run_id=run_id, code_version=code_version)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM measurements WHERE ts_utc >= ? AND ts_utc < ?", [start, end])
        con.register("new_rows", rows)
        con.execute("INSERT INTO measurements BY NAME SELECT * FROM new_rows")
        con.unregister("new_rows")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return LoadedDay(sha256, parsed)


def header_matches_station(header: StationHeader, tolerance_deg: float = 1e-3) -> bool:
    return (
        abs(header.latitude - STATION_LATITUDE) <= tolerance_deg
        and abs(header.longitude - STATION_LONGITUDE) <= tolerance_deg
    )


def duplicate_minutes(rows: pd.DataFrame) -> list[pd.Timestamp]:
    return sorted(rows.loc[rows["ts_utc"].duplicated(), "ts_utc"].unique())


def code_version() -> str:
    """Git commit of the running code, with '-dirty' if tracked files have uncommitted edits."""
    repo = Path(__file__).parent
    try:
        commit = _git(repo, "rev-parse", "--short=12", "HEAD")
        dirty = _git(repo, "status", "--porcelain", "--untracked-files=no")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return f"{commit}-dirty" if dirty else commit


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()


def _current_sha256(con: duckdb.DuckDBPyConnection, file_date: date) -> str | None:
    row = con.execute(
        "SELECT sha256 FROM source_files WHERE file_date = ? ORDER BY downloaded_at DESC LIMIT 1",
        [file_date],
    ).fetchone()
    return row[0] if row else None
