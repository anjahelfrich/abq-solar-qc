"""Overrides asset: the reviewed CSV of manual decisions, validated and loaded into DuckDB."""

from pathlib import Path

import dagster as dg

from abq_qc.overrides import load_overrides, read_overrides
from abq_qc.resources import Storage


@dg.asset(
    group_name="qc",
    kinds={"duckdb", "csv"},
    check_specs=[
        dg.AssetCheckSpec(
            "valid_rows",
            asset="manual_overrides",
            blocking=True,
            description="Every row has a unique id, UTC times with start <= end, a known "
            "variable and action, a rule_id for accept, and a reason and author.",
        ),
    ],
)
def manual_overrides(context: dg.AssetExecutionContext, storage: Storage) -> dg.MaterializeResult:
    """Manual overrides from overrides/manual_overrides.csv. Changed only through git commits.

    If the CSV has problems, the table is left as it was and the blocking check stops QC.
    """
    file = read_overrides(Path(storage.overrides_csv).read_bytes())
    changed = False
    if not file.problems:
        with storage.connect() as con:
            changed = load_overrides(con, file, context.run.run_id)

    return dg.MaterializeResult(
        metadata={
            "sha256": file.sha256,
            "overrides": len(file.rows),
            "loaded": not file.problems,
            "changed": changed,
        },
        data_version=dg.DataVersion(file.sha256),
        check_results=[
            dg.AssetCheckResult(
                check_name="valid_rows",
                passed=not file.problems,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={"problems": "\n".join(file.problems) or "none"},
            )
        ],
    )
