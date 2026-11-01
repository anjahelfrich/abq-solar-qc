"""Run the QC rules for one day, store the flags, and log the run and any overrides that
apply in the audit log. Plain functions, wrapped by `abq_qc.assets.qc`."""

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import duckdb
import pandas as pd

from abq_qc.db import append_audit
from abq_qc.qc_rules import RULES, daytime_flagged_share, run_minute_rules


class RuleChangedWithoutNewVersion(ValueError):
    pass


@dataclass(frozen=True)
class QcDay:
    flags: pd.DataFrame
    flagged_daytime_minutes: int
    daytime_minutes: int
    overrides_applied: pd.DataFrame  # override_id, action, author, reason, minutes

    @property
    def daytime_flagged_share(self) -> float:
        return self.flagged_daytime_minutes / self.daytime_minutes if self.daytime_minutes else 0.0

    def counts_by_rule(self) -> dict[str, int]:
        counts = self.flags["rule_id"].value_counts()
        return {r.rule_id: int(counts.get(r.rule_id, 0)) for r in RULES if r.rule_id != "drift"}


def sync_rules(con: duckdb.DuckDBPyConnection) -> None:
    """Add any new rule versions to qc_rules. Refuses a changed rule that kept its version,
    so a flag's (rule_id, rule_version) always means one fixed set of thresholds."""
    for rule in RULES:
        thresholds = json.dumps(rule.thresholds, sort_keys=True)
        stored = con.execute(
            "SELECT description, thresholds::VARCHAR, severity FROM qc_rules "
            "WHERE rule_id = ? AND version = ?",
            [rule.rule_id, rule.version],
        ).fetchone()
        if stored is None:
            con.execute(
                "INSERT INTO qc_rules VALUES (?, ?, ?, ?, ?, ?)",
                [
                    rule.rule_id,
                    rule.version,
                    rule.description,
                    thresholds,
                    rule.severity,
                    datetime.now(UTC),
                ],
            )
        elif (stored[0], json.loads(stored[1]), stored[2]) != (
            rule.description,
            json.loads(thresholds),
            rule.severity,
        ):
            raise RuleChangedWithoutNewVersion(
                f"rule {rule.rule_id!r} v{rule.version} differs from the stored version; "
                "bump its version in qc_rules.py"
            )


def run_qc(con: duckdb.DuckDBPyConnection, day: date, run_id: str) -> QcDay:
    """Run the per-minute rules on one day of measurements and replace that day's flags."""
    start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    end = start + timedelta(days=1)
    rows = con.execute(
        "SELECT * FROM measurements WHERE ts_utc >= ? AND ts_utc < ? ORDER BY ts_utc",
        [start, end],
    ).df()
    rows["ts_utc"] = rows["ts_utc"].astype("datetime64[us, UTC]")

    flags = run_minute_rules(rows, day).assign(run_id=run_id)

    sync_rules(con)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM qc_flags WHERE ts_utc >= ? AND ts_utc < ?", [start, end])
        con.register("new_flags", flags)
        con.execute("INSERT INTO qc_flags BY NAME SELECT * FROM new_flags")
        con.unregister("new_flags")
        result = QcDay(flags, *daytime_flagged_share(rows, flags), _applied(con, start, end))
        _log_run(con, day, result, run_id)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return result


def _applied(con: duckdb.DuckDBPyConnection, start: datetime, end: datetime) -> pd.DataFrame:
    """Overrides that touch this day, and how many minutes each one affects."""
    return con.execute(
        """
        SELECT o.override_id, o.action, o.author, o.reason, count(DISTINCT s.ts_utc) AS minutes
        FROM overrides o
        JOIN qc_status s ON list_contains(s.override_ids, o.override_id)
        WHERE s.ts_utc >= ? AND s.ts_utc < ?
        GROUP BY ALL
        ORDER BY o.override_id
        """,
        [start, end],
    ).df()


def _log_run(con: duckdb.DuckDBPyConnection, day: date, result: QcDay, run_id: str) -> None:
    overrides_sha = con.execute("SELECT any_value(source_sha256) FROM overrides").fetchone()[0]
    append_audit(
        con,
        "qc_run",
        "pipeline",
        {
            "day": day.isoformat(),
            "rule_versions": {r.rule_id: r.version for r in RULES},
            "flag_counts": result.counts_by_rule(),
            "overrides_sha256": overrides_sha,
        },
        run_id,
    )
    for o in result.overrides_applied.itertuples(index=False):
        append_audit(
            con,
            "override_applied",
            o.author,
            {
                "day": day.isoformat(),
                "override_id": o.override_id,
                "action": o.action,
                "reason": o.reason,
                "minutes": int(o.minutes),
            },
            run_id,
        )
