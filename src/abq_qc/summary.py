"""Per-day summary and the cross-day drift trend. Plain functions, wrapped by
`abq_qc.assets.summary`."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import duckdb
import numpy as np
import pandas as pd

from abq_qc.qc_rules import (
    ALL,
    DRIFT,
    GAP,
    RULES,
    VARIABLES,
    clear_sky,
    clear_sky_closure_ratio,
    daytime_flagged_share,
    drift,
)

MINUTES_PER_DAY = GAP.thresholds["minutes_per_day"]
BASELINE_START, BASELINE_END = date(2026, 8, 1), date(2026, 8, 31)


def summarize(rows: pd.DataFrame, flags: pd.DataFrame, day: date) -> dict:
    """One day's numbers from its `measurements` rows and `qc_flags` rows."""
    received = rows["ts_utc"].nunique()
    flagged_daytime, daytime = daytime_flagged_share(rows, flags)
    counts = flags["rule_id"].value_counts()
    return {
        "day": day,
        "received_minutes": received,
        "availability_pct": round(received / MINUTES_PER_DAY * 100, 2),
        "daytime_minutes": daytime,
        "flagged_daytime_minutes": flagged_daytime,
        **{
            f"flags_{r.rule_id}": int(counts.get(r.rule_id, 0))
            for r in RULES
            if r.rule_id != DRIFT.rule_id
        },
        "clear_sky_minutes": int(clear_sky(rows).sum()),
        "clear_sky_closure_ratio": clear_sky_closure_ratio(rows),
        **noaa_agreement(rows, flags),
    }


def noaa_agreement(rows: pd.DataFrame, flags: pd.DataFrame) -> dict[str, int]:
    """Sensor values (GHI/DNI/DHI x received minute) flagged by both, only us, only NOAA.

    Our side is any automatic flag on that sensor or on 'all' (closure counts for all three).
    Overrides are ignored: this compares automatic QC with automatic QC.
    """
    both = ours_only = theirs_only = 0
    for var in VARIABLES:
        ours = rows["ts_utc"].isin(flags.loc[flags["variable"].isin([var, ALL]), "ts_utc"])
        theirs = rows[f"noaa_qc_{var}"] != 0
        both += int((ours & theirs).sum())
        ours_only += int((ours & ~theirs).sum())
        theirs_only += int((~ours & theirs).sum())
    return {"noaa_both": both, "noaa_ours_only": ours_only, "noaa_theirs_only": theirs_only}


def summarize_day(con: duckdb.DuckDBPyConnection, day: date, run_id: str) -> dict:
    """Summarize one day from the database and replace its daily_summary row."""
    start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    end = start + timedelta(days=1)
    rows = con.execute(
        "SELECT * FROM measurements WHERE ts_utc >= ? AND ts_utc < ?", [start, end]
    ).df()
    flags = con.execute(
        "SELECT ts_utc, variable, rule_id FROM qc_flags WHERE ts_utc >= ? AND ts_utc < ?",
        [start, end],
    ).df()
    summary = summarize(rows, flags, day) | {"run_id": run_id}
    _replace(con, "daily_summary", pd.DataFrame([summary]), "day = ?", [day])
    return summary


@dataclass(frozen=True)
class DriftResult:
    trend: pd.DataFrame  # one row per day, see the drift_trend table
    baseline: float
    baseline_days: int

    def latest(self) -> pd.Series | None:
        """Newest day that has a 7-day median (None if no day has enough clear-sky days)."""
        known = self.trend.dropna(subset=["rolling_median"])
        return known.iloc[-1] if len(known) else None


def build_drift_trend(con: duckdb.DuckDBPyConnection, run_id: str) -> DriftResult:
    """Recompute drift for every day in daily_summary and replace the drift_trend table."""
    daily = con.execute("SELECT day, clear_sky_closure_ratio FROM daily_summary ORDER BY day").df()
    ratio = pd.Series(
        daily["clear_sky_closure_ratio"].to_numpy(dtype="float64"),
        index=pd.to_datetime(daily["day"]),
    )
    in_baseline = (ratio.index.date >= BASELINE_START) & (ratio.index.date <= BASELINE_END)
    baseline_ratios = ratio[in_baseline].dropna()
    baseline = float(baseline_ratios.median()) if len(baseline_ratios) else np.nan

    trend = drift(ratio, baseline).rename_axis("day").reset_index()
    trend["day"] = trend["day"].dt.date
    trend["run_id"] = run_id

    _replace(con, "drift_trend", trend)
    return DriftResult(trend, baseline, len(baseline_ratios))


def newest_day(con: duckdb.DuckDBPyConnection) -> date | None:
    return con.execute("SELECT max(day) FROM daily_summary").fetchone()[0]


def _replace(
    con: duckdb.DuckDBPyConnection,
    table: str,
    frame: pd.DataFrame,
    where: str = "true",
    params: list | None = None,
) -> None:
    """Delete the matching rows and insert `frame` in one transaction. NaN is stored as NULL."""
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM {table} WHERE {where}", params or [])
        con.register("new_rows", frame)
        con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM new_rows")
        con.unregister("new_rows")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
