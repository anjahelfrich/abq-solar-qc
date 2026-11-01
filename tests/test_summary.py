import math
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from abq_qc import db
from abq_qc.ingest import load_measurements, store_raw_file
from abq_qc.qc import run_qc
from abq_qc.summary import build_drift_trend, noaa_agreement, summarize_day

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def con():
    c = db.connect(":memory:")
    yield c
    c.close()


def load_and_qc(con, raw_dir: Path, fixture: str, day: date) -> None:
    store_raw_file(
        con, raw_dir, day, "https://example.test", (FIXTURES / fixture).read_bytes(), "r"
    )
    load_measurements(con, raw_dir, day, "r", "v1")
    run_qc(con, day, "r")


def test_full_day_summary(con, tmp_path):
    day = date(2026, 8, 1)
    load_and_qc(con, tmp_path, "abq26213.dat", day)

    s = summarize_day(con, day, "run-1")

    assert (s["received_minutes"], s["availability_pct"]) == (1440, 100.0)
    assert (s["flags_limits"], s["flags_closure"], s["flags_gap"], s["flags_stuck"]) == (
        23,
        47,
        0,
        0,
    )
    assert s["clear_sky_minutes"] == 356
    assert s["clear_sky_closure_ratio"] == pytest.approx(1.0055, abs=1e-4)
    # 47 closure minutes x 3 sensors + 23 DNI limits = 164 values; NOAA flagged 4 of them
    assert (s["noaa_both"], s["noaa_ours_only"], s["noaa_theirs_only"]) == (4, 160, 0)


def test_partial_day_summary_and_null_ratio(con, tmp_path):
    day = date(2026, 3, 4)
    load_and_qc(con, tmp_path, "abq26063.dat", day)

    s = summarize_day(con, day, "run-1")

    assert (s["received_minutes"], s["availability_pct"], s["flags_gap"]) == (892, 61.94, 548)
    stored = con.execute("SELECT clear_sky_closure_ratio FROM daily_summary").fetchone()[0]
    assert stored is None  # no clear-sky minutes: NULL, not NaN


def test_rerun_replaces_the_days_row(con, tmp_path):
    day = date(2026, 8, 1)
    load_and_qc(con, tmp_path, "abq26213.dat", day)
    summarize_day(con, day, "run-1")
    summarize_day(con, day, "run-2")

    assert con.execute("SELECT run_id FROM daily_summary").fetchall() == [("run-2",)]


def test_noaa_agreement_counts_sensor_values():
    t0, t1 = pd.Timestamp("2026-08-01 18:00", tz="UTC"), pd.Timestamp("2026-08-01 18:01", tz="UTC")
    rows = pd.DataFrame(
        {
            "ts_utc": [t0, t1],
            "noaa_qc_ghi": [0, 1],
            "noaa_qc_dni": [1, 0],
            "noaa_qc_dhi": [0, 0],
        }
    )
    # A closure flag on 'all' at t0 counts for all three sensors
    flags = pd.DataFrame({"ts_utc": [t0], "variable": ["all"], "rule_id": ["closure"]})

    assert noaa_agreement(rows, flags) == {
        "noaa_both": 1,  # t0 dni
        "noaa_ours_only": 2,  # t0 ghi, t0 dhi
        "noaa_theirs_only": 1,  # t1 ghi
    }


def insert_ratios(con, ratios: dict[date, float | None]) -> None:
    """daily_summary rows where only the clear-sky ratio matters."""
    con.register("ratios", pd.DataFrame(ratios.items(), columns=["day", "ratio"]))
    con.execute(
        "INSERT INTO daily_summary SELECT day, 1440, 100, 800, 0, 0, 0, 0, 0, 100, ratio, "
        "0, 0, 0, 'r' FROM ratios"
    )


def test_drift_trend_warns_when_the_7_day_median_leaves_the_band(con):
    aug = {date(2026, 8, 1) + timedelta(days=i): 1.00 for i in range(31)}
    aug[date(2026, 8, 15)] = None  # cloudy day, no clear-sky ratio
    sep = {date(2026, 9, 1) + timedelta(days=i): 1.03 for i in range(10)}
    insert_ratios(con, aug | sep)

    result = build_drift_trend(con, "run-d")

    assert result.baseline == pytest.approx(1.00)
    assert result.baseline_days == 30
    trend = result.trend.set_index("day")
    assert not trend.loc[date(2026, 8, 31), "warn"]
    # The 7-day window is mostly September by Sep 4, so the median is 1.03: +3%
    assert trend.loc[date(2026, 9, 4), "warn"]
    latest = result.latest()
    assert latest["day"] == date(2026, 9, 10)
    assert latest["deviation_pct"] == pytest.approx(3.0)
    assert con.execute("SELECT count(*) FROM drift_trend").fetchone()[0] == 41


def test_drift_trend_without_august_has_no_baseline(con):
    insert_ratios(con, {date(2026, 9, 1) + timedelta(days=i): 1.0 for i in range(5)})

    result = build_drift_trend(con, "run-d")

    assert math.isnan(result.baseline)
    assert not result.trend["warn"].any()
