"""Dagster entry point: `dagster dev -m abq_qc.definitions`."""

import os
from datetime import timedelta

import dagster as dg

from abq_qc.assets.ingest import irradiance_1min, raw_solrad_file
from abq_qc.assets.overrides import manual_overrides
from abq_qc.assets.qc import qc_flags
from abq_qc.assets.snapshot import snapshot
from abq_qc.assets.summary import daily_summary, drift_trend, newest_day_fresh
from abq_qc.resources import SolradSource, Storage

# NOAA posts each day's file 1-2 days late, so the 18:00 UTC run processes the day before
# yesterday.
DAYS_LATE = 2

daily_pipeline = dg.define_asset_job(
    "daily_pipeline",
    selection=dg.AssetSelection.all() - dg.AssetSelection.checks(newest_day_fresh),
    description="Download, parse, QC and summarize one day, then rebuild drift and snapshot.",
)

freshness = dg.define_asset_job(
    "freshness",
    selection=dg.AssetSelection.checks(newest_day_fresh),
    description="Warn if the newest day is too old. Separate from daily_pipeline so it still "
    "runs when that job fails.",
)


@dg.schedule(job=daily_pipeline, cron_schedule="0 18 * * *", execution_timezone="UTC")
def daily_pipeline_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    day = context.scheduled_execution_time.date() - timedelta(days=DAYS_LATE)
    return dg.RunRequest(run_key=day.isoformat(), partition_key=day.isoformat())


freshness_schedule = dg.ScheduleDefinition(
    job=freshness, cron_schedule="0 19 * * *", execution_timezone="UTC"
)

defs = dg.Definitions(
    assets=[
        raw_solrad_file,
        irradiance_1min,
        manual_overrides,
        qc_flags,
        daily_summary,
        drift_trend,
        snapshot,
    ],
    asset_checks=[newest_day_fresh],
    jobs=[daily_pipeline, freshness],
    schedules=[daily_pipeline_schedule, freshness_schedule],
    resources={
        "noaa": SolradSource(),
        "storage": Storage(data_dir=os.environ.get("ABQ_QC_DATA_DIR", "data")),
    },
    # DuckDB allows one writer at a time, so steps run in one process, one after another.
    executor=dg.in_process_executor,
)
