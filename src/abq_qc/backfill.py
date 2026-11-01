"""Backfill a date range: `python -m abq_qc.backfill 2026-08-01 2026-10-31` (both included).

Runs go through the Dagster instance in $DAGSTER_HOME, so they show up in the Dagster UI.
Order: overrides once, then each day's ingest -> QC -> summary, then drift_trend once.
"""

import argparse
import sys
from datetime import date, timedelta

import dagster as dg

from abq_qc.assets.summary import newest_day_fresh
from abq_qc.definitions import defs

PER_DAY = ["raw_solrad_file", "irradiance_1min", "qc_flags", "daily_summary"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("start", type=date.fromisoformat)
    parser.add_argument("end", type=date.fromisoformat)
    args = parser.parse_args(argv)

    instance = dg.DagsterInstance.get()

    def run(keys: list[str], partition_key: str | None = None):
        # The freshness check has its own schedule; mid-backfill it would only warn
        return dg.materialize(
            [*defs.assets, *defs.asset_checks],
            selection=dg.AssetSelection.assets(*keys) - dg.AssetSelection.checks(newest_day_fresh),
            partition_key=partition_key,
            resources=defs.resources,
            instance=instance,
            tags={"source": "make-backfill"},
            raise_on_error=False,
        )

    if not _ok(run(["manual_overrides"]), "manual_overrides"):
        return 1

    failed = []
    day = args.start
    while day <= args.end:
        if not _ok(run(PER_DAY, day.isoformat()), day.isoformat()):
            failed.append(day)
        day += timedelta(days=1)

    _ok(run(["drift_trend"]), "drift_trend")
    print(f"done: {(args.end - args.start).days + 1 - len(failed)} days ok, {len(failed)} failed")
    return 1 if failed else 0


def _ok(result: dg.ExecuteInProcessResult, label: str) -> bool:
    failures = [e for e in result.all_events if e.is_step_failure]
    blocked = [e for e in result.get_asset_check_evaluations() if not e.passed]
    if failures:
        message = failures[0].step_failure_data.error.message.strip().splitlines()[0]
        print(f"{label}: FAILED  {message}")
    elif blocked:
        print(f"{label}: ok, failed checks: {', '.join(e.check_name for e in blocked)}")
    else:
        print(f"{label}: ok")
    return result.success and not failures


if __name__ == "__main__":
    sys.exit(main())
