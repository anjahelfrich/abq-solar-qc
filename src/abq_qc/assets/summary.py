"""Summary assets: one row per day, then the drift trend across all days."""

import math
from datetime import UTC, date, datetime

import dagster as dg

from abq_qc.assets.ingest import daily
from abq_qc.assets.qc import qc_flags
from abq_qc.qc_rules import DRIFT
from abq_qc.resources import Storage
from abq_qc.summary import build_drift_trend, newest_day, summarize_day

MIN_AVAILABILITY_PCT = 95.0
MAX_DAYS_BEHIND = 3
DRIFT_WARN_PCT = DRIFT.thresholds["warn_pct"]


@dg.asset(
    partitions_def=daily,
    group_name="summary",
    kinds={"duckdb"},
    deps=[qc_flags],
    check_specs=[
        dg.AssetCheckSpec(
            "availability",
            asset="daily_summary",
            description=f"At least {MIN_AVAILABILITY_PCT:.0f}% of the day's 1440 minutes "
            "were received.",
        ),
    ],
)
def daily_summary(context: dg.AssetExecutionContext, storage: Storage) -> dg.MaterializeResult:
    """Availability, flags per rule, clear-sky closure ratio and NOAA agreement for one day."""
    day = date.fromisoformat(context.partition_key)
    with storage.connect() as con:
        s = summarize_day(con, day, context.run.run_id)

    ratio = s["clear_sky_closure_ratio"]
    return dg.MaterializeResult(
        metadata={
            "availability_pct": s["availability_pct"],
            "clear_sky_minutes": s["clear_sky_minutes"],
            "clear_sky_closure_ratio": "no clear sky" if math.isnan(ratio) else round(ratio, 4),
            "noaa_both": s["noaa_both"],
            "noaa_ours_only": s["noaa_ours_only"],
            "noaa_theirs_only": s["noaa_theirs_only"],
        },
        check_results=[
            dg.AssetCheckResult(
                check_name="availability",
                passed=s["availability_pct"] >= MIN_AVAILABILITY_PCT,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={
                    "availability_pct": s["availability_pct"],
                    "received_minutes": s["received_minutes"],
                },
            )
        ],
    )


@dg.asset_check(
    asset=daily_summary,
    description=f"The newest summarized day is at most {MAX_DAYS_BEHIND} days old. "
    "Runs on its own schedule, so it still warns when the pipeline itself has stopped.",
)
def newest_day_fresh(storage: Storage) -> dg.AssetCheckResult:
    today = datetime.now(UTC).date()
    with storage.connect() as con:
        newest = newest_day(con)
    days_behind = (today - newest).days if newest else None
    return dg.AssetCheckResult(
        passed=days_behind is not None and days_behind <= MAX_DAYS_BEHIND,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "newest_day": newest.isoformat() if newest else "none",
            "days_behind": days_behind if days_behind is not None else -1,
            "max_days_behind": MAX_DAYS_BEHIND,
        },
    )


@dg.asset(
    group_name="summary",
    kinds={"duckdb"},
    deps=[daily_summary],
    check_specs=[
        dg.AssetCheckSpec(
            "drift_within_band",
            asset="drift_trend",
            description=f"The newest 7-day median clear-sky closure ratio is within "
            f"+-{DRIFT_WARN_PCT:g}% of the August baseline.",
        ),
    ],
)
def drift_trend(context: dg.AssetExecutionContext, storage: Storage) -> dg.MaterializeResult:
    """7-day median clear-sky closure ratio per day, compared with the August baseline."""
    with storage.connect() as con:
        result = build_drift_trend(con, context.run.run_id)

    latest = result.latest()
    warn_days = result.trend.loc[result.trend["warn"], "day"]
    check_meta = {
        "baseline": "none" if math.isnan(result.baseline) else round(result.baseline, 4),
        "baseline_days": result.baseline_days,
        "days_outside_band": len(warn_days),
        "first_days_outside_band": ", ".join(d.isoformat() for d in warn_days[:10]),
    }
    if latest is not None:
        check_meta |= {
            "latest_day": latest["day"].isoformat(),
            "latest_deviation_pct": round(float(latest["deviation_pct"]), 2),
        }
    return dg.MaterializeResult(
        metadata={"days": len(result.trend), "baseline_days": result.baseline_days},
        check_results=[
            dg.AssetCheckResult(
                check_name="drift_within_band",
                passed=latest is not None and not bool(latest["warn"]),
                severity=dg.AssetCheckSeverity.WARN,
                metadata=check_meta,
            )
        ],
    )
