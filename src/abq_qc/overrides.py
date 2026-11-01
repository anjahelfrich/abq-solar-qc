"""Manual overrides: read and validate overrides/manual_overrides.csv, load it into `overrides`.

The CSV is the source of truth and changes only through git commits, so the commit history is
the reviewed record of who changed what and why. Plain functions, wrapped by
`abq_qc.assets.overrides`.
"""

import csv
import hashlib
import io
from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb
import pandas as pd

from abq_qc.db import append_audit
from abq_qc.qc_rules import ALL, CLOSURE, LIMITS, STUCK, VARIABLES

COLUMNS = (
    "override_id",
    "start_utc",
    "end_utc",
    "variable",
    "rule_id",
    "action",
    "reason",
    "author",
)
ACTIONS = ("accept", "flag_bad")
# Gaps can't be accepted (there's no value to keep) and drift isn't a per-minute flag
ACCEPTABLE_RULES = (LIMITS.rule_id, CLOSURE.rule_id, STUCK.rule_id)


@dataclass(frozen=True)
class OverridesFile:
    sha256: str
    rows: pd.DataFrame  # only valid if problems is empty
    problems: list[str]


def read_overrides(content: bytes) -> OverridesFile:
    """Parse and validate the CSV. Every problem is reported with its line number."""
    sha256 = hashlib.sha256(content).hexdigest()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
    if tuple(reader.fieldnames or ()) != COLUMNS:
        problem = f"header must be: {','.join(COLUMNS)}"
        return OverridesFile(sha256, pd.DataFrame(columns=list(COLUMNS)), [problem])

    records, problems, seen_ids = [], [], set()
    for row in reader:
        if None in row:  # DictReader puts fields beyond the header under the key None
            problems.append(
                f"line {reader.line_num}: more fields than the header "
                "(put text containing commas in double quotes)"
            )
            continue
        row = {k: (v or "").strip() for k, v in row.items()}
        errors = _row_errors(row, seen_ids)
        problems.extend(f"line {reader.line_num}: {e}" for e in errors)
        seen_ids.add(row["override_id"])
        if not errors:
            records.append(
                {
                    **row,
                    "start_utc": _parse_utc(row["start_utc"]),
                    "end_utc": _parse_utc(row["end_utc"]),
                    "rule_id": row["rule_id"] or None,
                }
            )

    rows = pd.DataFrame.from_records(records, columns=list(COLUMNS))
    rows = rows.astype({"start_utc": "datetime64[us, UTC]", "end_utc": "datetime64[us, UTC]"})
    return OverridesFile(sha256, rows, problems)


def load_overrides(con: duckdb.DuckDBPyConnection, file: OverridesFile, run_id: str) -> bool:
    """Replace the overrides table with the CSV's rows. Logs `overrides_loaded` if the CSV
    changed since the last load. Returns whether it changed."""
    if file.problems:
        raise ValueError("refusing to load an overrides file with problems")
    previous = _loaded_overrides(con)
    current = {r.override_id: r for r in file.rows.itertuples(index=False)}
    previous_sha = con.execute("SELECT any_value(source_sha256) FROM overrides").fetchone()[0]
    if previous_sha == file.sha256:
        return False

    rows = file.rows.assign(source_sha256=file.sha256, run_id=run_id)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM overrides")
        con.register("new_overrides", rows)
        con.execute("INSERT INTO overrides BY NAME SELECT * FROM new_overrides")
        con.unregister("new_overrides")
        append_audit(
            con,
            "overrides_loaded",
            "pipeline",
            {
                "sha256": file.sha256,
                "previous_sha256": previous_sha,
                "override_count": len(rows),
                "added": sorted(current.keys() - previous.keys()),
                "removed": sorted(previous.keys() - current.keys()),
                "changed": sorted(
                    k for k in current.keys() & previous.keys() if _row(current[k]) != previous[k]
                ),
            },
            run_id,
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return True


def _row_errors(row: dict[str, str], seen_ids: set[str]) -> list[str]:
    errors = []
    if not row["override_id"]:
        errors.append("override_id is empty")
    elif row["override_id"] in seen_ids:
        errors.append(f"override_id {row['override_id']!r} is used twice")

    start, end = _parse_utc(row["start_utc"]), _parse_utc(row["end_utc"])
    for name, value in (("start_utc", start), ("end_utc", end)):
        if value is None:
            errors.append(f"{name} {row[name]!r} must look like 2026-08-01T21:35:00Z (UTC)")
    if start and end and start > end:
        errors.append("start_utc is after end_utc")

    if row["variable"] not in (*VARIABLES, ALL):
        errors.append(f"variable must be one of {', '.join((*VARIABLES, ALL))}")
    if row["action"] not in ACTIONS:
        errors.append(f"action must be one of {', '.join(ACTIONS)}")
    elif row["action"] == "accept" and row["rule_id"] not in ACCEPTABLE_RULES:
        errors.append(f"accept needs rule_id, one of {', '.join(ACCEPTABLE_RULES)}")
    elif row["action"] == "flag_bad" and row["rule_id"]:
        errors.append("flag_bad doesn't take a rule_id")

    for name in ("reason", "author"):
        if not row[name]:
            errors.append(f"{name} is empty")
    return errors


def _parse_utc(value: str) -> datetime | None:
    """Only explicit UTC ('Z') is accepted, so a local time can't slip in by mistake."""
    if not value.endswith("Z"):
        return None
    try:
        return datetime.fromisoformat(value).astimezone(UTC)
    except ValueError:
        return None


def _loaded_overrides(con: duckdb.DuckDBPyConnection) -> dict[str, tuple]:
    rows = con.execute(f"SELECT {', '.join(COLUMNS)} FROM overrides").fetchall()
    return {r[0]: _row(r) for r in rows}


def _row(r) -> tuple:
    values = [None if pd.isna(v) else v for v in r]
    values[1], values[2] = (pd.Timestamp(v).tz_convert(UTC) for v in values[1:3])
    return tuple(values)
