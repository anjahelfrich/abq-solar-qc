"""Snapshot asset: Parquet copy of the database for the hosted dashboard."""

from pathlib import Path

import dagster as dg

from abq_qc.assets.overrides import manual_overrides
from abq_qc.assets.summary import drift_trend
from abq_qc.ingest import code_version
from abq_qc.resources import Storage
from abq_qc.snapshot import write_snapshot


@dg.asset(group_name="publish", kinds={"parquet"}, deps=[drift_trend, manual_overrides])
def snapshot(storage: Storage) -> dg.MaterializeResult:
    """snapshot/<table>.parquet + manifest.json. Commit these to git to update the hosted app."""
    out_dir = Path(storage.snapshot_dir)
    with storage.connect() as con:
        manifest = write_snapshot(con, out_dir, code_version())
    return dg.MaterializeResult(
        metadata={
            "path": dg.MetadataValue.path(str(out_dir)),
            "newest_day": manifest["newest_day"] or "none",
            "audit_last_hash": manifest["audit_last_hash"] or "none",
            **{f"rows_{t}": v["rows"] for t, v in manifest["tables"].items()},
        }
    )
