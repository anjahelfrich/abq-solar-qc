"""QC asset: run the per-minute rules on one day of measurements."""

from datetime import date

import dagster as dg

from abq_qc.assets.ingest import daily, irradiance_1min
from abq_qc.qc import run_qc
from abq_qc.resources import Storage

MAX_DAYTIME_FLAGGED_SHARE = 0.10


@dg.asset(
    partitions_def=daily,
    group_name="qc",
    kinds={"duckdb"},
    deps=[irradiance_1min],
    check_specs=[
        dg.AssetCheckSpec(
            "daytime_flagged_share",
            asset="qc_flags",
            description=f"At most {MAX_DAYTIME_FLAGGED_SHARE:.0%} of received daytime minutes "
            "(zenith < 90 deg) have a QC flag. Gaps are left out; availability has its own check.",
        ),
    ],
)
def qc_flags(context: dg.AssetExecutionContext, storage: Storage) -> dg.MaterializeResult:
    """One row per failed rule, per variable, per minute. Rule versions are in qc_rules."""
    day = date.fromisoformat(context.partition_key)
    with storage.connect() as con:
        result = run_qc(con, day, context.run.run_id)

    share = result.daytime_flagged_share
    counts = result.counts_by_rule()
    return dg.MaterializeResult(
        metadata={"flags": len(result.flags), **{f"flags_{k}": v for k, v in counts.items()}},
        check_results=[
            dg.AssetCheckResult(
                check_name="daytime_flagged_share",
                passed=share <= MAX_DAYTIME_FLAGGED_SHARE,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={
                    "share_pct": round(share * 100, 1),
                    "threshold_pct": MAX_DAYTIME_FLAGGED_SHARE * 100,
                    "flagged_daytime_minutes": result.flagged_daytime_minutes,
                    "daytime_minutes": result.daytime_minutes,
                },
            )
        ],
    )
