"""Run the Dagster ingest assets for one day, with NOAA replaced by a local folder."""

import shutil
from pathlib import Path

import dagster as dg
import pytest

from abq_qc import db
from abq_qc.assets.ingest import irradiance_1min, raw_solrad_file
from abq_qc.assets.overrides import manual_overrides
from abq_qc.assets.qc import qc_flags
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


def run_day(tmp_path: Path, noaa_dir: Path, day: str = "2026-08-01"):
    return dg.materialize(
        [raw_solrad_file, irradiance_1min, manual_overrides, qc_flags],
        partition_key=day,
        resources={
            "noaa": SolradSource(base_url=noaa_dir.as_uri()),
            "storage": Storage(
                data_dir=str(tmp_path / "data"),
                overrides_csv=str(tmp_path / "manual_overrides.csv"),
            ),
        },
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
    }
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
