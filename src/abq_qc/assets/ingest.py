"""Ingest assets: download one day's NOAA file, then parse it into `measurements`."""

from datetime import date

import dagster as dg

from abq_qc.ingest import (
    MINUTES_PER_DAY,
    STATION_LATITUDE,
    STATION_LONGITUDE,
    code_version,
    duplicate_minutes,
    header_matches_station,
    load_measurements,
    store_raw_file,
)
from abq_qc.resources import FileNotPostedError, SolradSource, Storage

daily = dg.DailyPartitionsDefinition(start_date="2026-08-01", timezone="UTC")


@dg.asset(partitions_def=daily, group_name="ingest", kinds={"duckdb"})
def raw_solrad_file(
    context: dg.AssetExecutionContext, noaa: SolradSource, storage: Storage
) -> dg.MaterializeResult:
    """NOAA's file for one UTC day, saved unchanged under data/raw/ and logged in source_files."""
    day = date.fromisoformat(context.partition_key)
    url = noaa.url_for(day)
    try:
        content = noaa.fetch(url)
    except FileNotPostedError as e:
        raise dg.Failure(
            description=f"NOAA hasn't posted {url} yet. Files usually appear 1-2 days late; "
            "re-run this day later."
        ) from e

    with storage.connect() as con:
        stored = store_raw_file(con, storage.raw_dir, day, url, content, context.run.run_id)

    context.log.info(f"{url}: {stored.status} ({stored.sha256[:12]})")
    return dg.MaterializeResult(
        metadata={
            "url": dg.MetadataValue.url(url),
            "sha256": stored.sha256,
            "previous_sha256": stored.previous_sha256 or "",
            "status": stored.status,
            "size_bytes": len(content),
            "path": dg.MetadataValue.path(str(stored.path)),
        }
    )


@dg.asset(
    partitions_def=daily,
    group_name="ingest",
    kinds={"duckdb"},
    deps=[raw_solrad_file],
    check_specs=[
        dg.AssetCheckSpec(
            "station_header",
            asset="irradiance_1min",
            blocking=True,
            description="File header latitude/longitude match the Albuquerque station.",
        ),
        dg.AssetCheckSpec(
            "no_duplicate_minutes",
            asset="irradiance_1min",
            blocking=True,
            description="Each minute appears at most once.",
        ),
    ],
)
def irradiance_1min(context: dg.AssetExecutionContext, storage: Storage) -> dg.MaterializeResult:
    """1-minute rows for one UTC day, each traceable to its file, line, run and commit."""
    day = date.fromisoformat(context.partition_key)
    with storage.connect() as con:
        loaded = load_measurements(con, storage.raw_dir, day, context.run.run_id, code_version())

    header, rows = loaded.parsed.header, loaded.parsed.rows
    dupes = duplicate_minutes(rows)
    received = rows["ts_utc"].nunique()

    return dg.MaterializeResult(
        metadata={
            "rows": len(rows),
            "missing_minutes": MINUTES_PER_DAY - received,
            "source_sha256": loaded.sha256,
        },
        check_results=[
            dg.AssetCheckResult(
                check_name="station_header",
                passed=header_matches_station(header),
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={
                    "file_lat_lon": f"{header.latitude}, {header.longitude}",
                    "expected_lat_lon": f"{STATION_LATITUDE}, {STATION_LONGITUDE}",
                    "station_name": header.name,
                },
            ),
            dg.AssetCheckResult(
                check_name="no_duplicate_minutes",
                passed=not dupes,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={
                    "duplicate_count": len(dupes),
                    "first_duplicates": ", ".join(str(t) for t in dupes[:5]),
                },
            ),
        ],
    )
