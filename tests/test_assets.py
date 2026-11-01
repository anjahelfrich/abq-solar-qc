"""Run the Dagster ingest assets for one day, with NOAA replaced by a local folder."""

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest

from abq_qc import db
from abq_qc.assets.ingest import irradiance_1min, raw_solrad_file
from abq_qc.assets.overrides import manual_overrides
from abq_qc.assets.qc import qc_flags
from abq_qc.assets.snapshot import snapshot
from abq_qc.assets.summary import daily_summary, drift_trend, newest_day_fresh
from abq_qc.definitions import daily_pipeline_schedule, defs
from abq_qc.resources import SolradSource, Storage

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def noaa_dir(tmp_path) -> Path:
    """A local copy of NOAA's folder layout: <base>/2026/abq26213.dat."""
    (tmp_path / "noaa" / "2026").mkdir(parents=True)
    shutil.copy(FIXTURES / "abq26213.dat", tmp_path / "noaa" / "2026")
    return tmp_path / "noaa"


@pytest.fixture
def overrides_csv(tmp_path) -> Path:
    path = tmp_path / "manual_overrides.csv"
    path.write_text(
        "override_id,start_utc,end_utc,variable,rule_id,action,reason,author\n"
        "OVR-0001,2026-08-01T21:35:00Z,2026-08-01T23:17:00Z,dni,limits,accept,"
        "thermal offset,Anja Helfrich\n"
    )
    return path


ALL_ASSETS = [
    raw_solrad_file,
    irradiance_1min,
    manual_overrides,
    qc_flags,
    daily_summary,
    drift_trend,
    snapshot,
]


def storage(tmp_path: Path) -> Storage:
    return Storage(
        data_dir=str(tmp_path / "data"),
        overrides_csv=str(tmp_path / "manual_overrides.csv"),
        snapshot_dir=str(tmp_path / "snapshot"),
    )


def run_day(tmp_path: Path, noaa_dir: Path, day: str = "2026-08-01"):
    return dg.materialize(
        ALL_ASSETS,
        partition_key=day,
        resources={"noaa": SolradSource(base_url=noaa_dir.as_uri()), "storage": storage(tmp_path)},
        raise_on_error=False,
    )


def checks(result) -> dict[str, bool]:
    return {e.check_name: e.passed for e in result.get_asset_check_evaluations()}


def test_one_day_end_to_end(tmp_path, noaa_dir, overrides_csv):
    result = run_day(tmp_path, noaa_dir)

    assert result.success
    assert checks(result) == {
        "station_header": True,
        "no_duplicate_minutes": True,
        "valid_rows": True,
        "daytime_flagged_share": True,
        "audit_chain_intact": True,
        "availability": True,
        "drift_within_band": False,  # one day isn't enough for a 7-day median
    }
    manifest = json.loads((tmp_path / "snapshot" / "manifest.json").read_text())
    assert manifest["tables"]["daily_summary"]["rows"] == 1
    con = db.connect(tmp_path / "data" / "abq_qc.duckdb")
    n, run_id = con.execute("SELECT count(*), any_value(run_id) FROM measurements").fetchone()
    assert (n, run_id) == (1440, result.run_id)
    flag_runs = con.execute("SELECT DISTINCT run_id FROM qc_flags").fetchall()
    assert flag_runs == [(result.run_id,)]
    events = [
        r[0] for r in con.execute("SELECT event_type FROM audit_log ORDER BY event_id").fetchall()
    ]
    # The two upstream assets don't depend on each other, so either can run first
    assert set(events[:2]) == {"file_ingested", "overrides_loaded"}
    assert events[2:] == ["qc_run", "override_applied"]


def test_invalid_overrides_csv_blocks_qc(tmp_path, noaa_dir, overrides_csv):
    overrides_csv.write_text(overrides_csv.read_text().replace("21:35:00Z", "21:35:00"))

    result = run_day(tmp_path, noaa_dir)

    assert checks(result)["valid_rows"] is False
    materialized = {e.asset_key.path[-1] for e in result.get_asset_materialization_events()}
    assert "qc_flags" not in materialized


def test_duplicate_minute_fails_the_check(tmp_path, noaa_dir, overrides_csv):
    path = noaa_dir / "2026" / "abq26213.dat"
    lines = path.read_text().splitlines()
    path.write_text("\n".join([*lines, lines[100]]) + "\n")

    result = run_day(tmp_path, noaa_dir)

    assert checks(result) == {
        "station_header": True,
        "no_duplicate_minutes": False,
        "valid_rows": True,
    }
    # The blocking check stops QC from running on bad data
    materialized = {e.asset_key.path[-1] for e in result.get_asset_materialization_events()}
    assert "qc_flags" not in materialized


def test_missing_file_gives_a_clear_failure(tmp_path, noaa_dir, overrides_csv):
    result = run_day(tmp_path, noaa_dir, day="2026-08-02")

    assert not result.success
    failure = next(e for e in result.all_events if e.is_step_failure)
    assert "hasn't posted" in failure.step_failure_data.error.message


def test_freshness_check(tmp_path, noaa_dir, overrides_csv, monkeypatch):
    run_day(tmp_path, noaa_dir)

    def check_on(today: datetime) -> dg.AssetCheckResult:
        monkeypatch.setattr(
            "abq_qc.assets.summary.datetime", type("dt", (), {"now": lambda tz: today})
        )
        return newest_day_fresh(storage=storage(tmp_path))

    assert check_on(datetime(2026, 8, 4, tzinfo=UTC)).passed
    assert not check_on(datetime(2026, 8, 5, tzinfo=UTC)).passed


def test_schedule_processes_the_day_before_yesterday():
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 3, 18, tzinfo=UTC)
    )
    request = daily_pipeline_schedule(context)
    assert request.partition_key == "2026-11-01"


def test_definitions_load():
    dg.Definitions.validate_loadable(defs)


def test_drift_trend_check_on_a_month_of_ratios(tmp_path):
    con = db.connect(tmp_path / "data" / "abq_qc.duckdb")
    con.execute(
        "INSERT INTO daily_summary SELECT d::DATE, 1440, 100, 800, 0, 0, 0, 0, 0, 100, "
        "CASE WHEN d < DATE '2026-09-01' THEN 1.0 ELSE 1.03 END, 0, 0, 0, 'r' "
        "FROM range(DATE '2026-08-01', DATE '2026-09-11', INTERVAL 1 DAY) t(d)"
    )
    con.close()

    result = dg.materialize([drift_trend], resources={"storage": storage(tmp_path)})

    [check] = result.get_asset_check_evaluations()
    assert check.passed is False
    assert check.metadata["latest_day"].value == "2026-09-10"
    assert check.metadata["latest_deviation_pct"].value == 3.0
