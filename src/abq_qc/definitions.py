"""Dagster entry point: `dagster dev -m abq_qc.definitions`."""

import os

import dagster as dg

from abq_qc.assets.ingest import irradiance_1min, raw_solrad_file
from abq_qc.resources import SolradSource, Storage

defs = dg.Definitions(
    assets=[raw_solrad_file, irradiance_1min],
    resources={
        "noaa": SolradSource(),
        "storage": Storage(data_dir=os.environ.get("ABQ_QC_DATA_DIR", "data")),
    },
    # DuckDB allows one writer at a time, so steps run in one process, one after another.
    executor=dg.in_process_executor,
)
