"""Each rule on small hand-made cases: one that should pass, one that should fail."""

import math
from datetime import UTC, date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from abq_qc import qc_rules
from abq_qc.qc_rules import FLAG_COLUMNS

DAY = date(2026, 8, 1)
NOON = datetime(2026, 8, 1, 18, 0, tzinfo=UTC)  # ~solar noon in Albuquerque


def minutes(n: int, start: datetime = NOON, **cols) -> pd.DataFrame:
    """n consecutive minutes. Scalars repeat; lists give one value per minute.
    Defaults: a clear midday sky that passes every rule."""
    defaults = {"zenith_deg": 30.0, "dni": 900.0, "dhi": 100.0}
    data = {**defaults, **cols}
    ts = pd.date_range(start, periods=n, freq="min", unit="us")
    rows = pd.DataFrame({"ts_utc": ts, **{k: np.broadcast_to(v, n) for k, v in data.items()}})
    # Vary defaults a little so they aren't "stuck"
    for var in ("dni", "dhi"):
        if var not in cols:
            rows[var] = rows[var] + np.arange(n) * 0.1
    if "ghi" not in cols:
        rows["ghi"] = qc_rules.component_sum(rows)
    return rows


def flagged(flags: pd.DataFrame) -> list[tuple[str, str]]:
    return sorted(zip(flags["ts_utc"].dt.strftime("%H:%M"), flags["variable"], strict=True))


# limits


def test_limits_pass():
    day = minutes(3, ghi=[1000.0, -3.9, 0.0], dni=[900.0, 0.0, -3.9], dhi=[100.0, 0.0, 0.0])
    night = minutes(2, start=NOON.replace(hour=6), zenith_deg=110.0, ghi=-9.9, dni=0.0, dhi=-9.9)
    assert qc_rules.limits(pd.concat([day, night])).empty


def test_limits_fail():
    # Aug 1, zenith 30: GHI max ~1770, DNI max ~1320, DHI max ~1110
    rows = minutes(
        4,
        ghi=[1900.0, 500.0, -4.1, 500.0],
        dni=[900.0, 1400.0, 0.0, 400.0],
        dhi=[100.0, 100.0, 0.0, 1200.0],
    )
    night = minutes(1, start=NOON.replace(hour=6), zenith_deg=110.0, ghi=-10.1, dni=0.0, dhi=0.0)

    flags = qc_rules.limits(pd.concat([rows, night]))

    assert flagged(flags) == [
        ("06:00", "ghi"),
        ("18:00", "ghi"),
        ("18:01", "dni"),
        ("18:02", "ghi"),
        ("18:03", "dhi"),
    ]
    assert set(flags["severity"]) == {"bad"}


# closure


def test_closure_pass():
    rows = minutes(3)
    rows["ghi"] = qc_rules.component_sum(rows) * [1.0, 1.079, 0.921]
    # Low sun: wider tolerance. Small sum: not tested at all.
    low_sun = minutes(1, zenith_deg=85.0, dni=300.0, dhi=60.0)
    low_sun["ghi"] = qc_rules.component_sum(low_sun) * 1.14
    dim = minutes(1, dni=0.0, dhi=40.0, ghi=80.0)

    assert qc_rules.closure(pd.concat([rows, low_sun, dim])).empty


def test_closure_fail():
    rows = minutes(2)
    rows["ghi"] = qc_rules.component_sum(rows) * [1.09, 0.85]
    low_sun = minutes(1, start=NOON + timedelta(minutes=2), zenith_deg=85.0, dni=300.0, dhi=60.0)
    low_sun["ghi"] = qc_rules.component_sum(low_sun) * 1.16

    flags = qc_rules.closure(pd.concat([rows, low_sun]))

    assert flagged(flags) == [("18:00", "all"), ("18:01", "all"), ("18:02", "all")]
    assert flags["value"].round(2).tolist() == [1.09, 0.85, 1.16]


# gap


def test_gap_pass():
    full_day = minutes(1440, start=datetime(2026, 8, 1, tzinfo=UTC))
    assert qc_rules.gap(full_day, DAY).empty


def test_gap_fail():
    full_day = minutes(1440, start=datetime(2026, 8, 1, tzinfo=UTC))
    rows = full_day.drop(index=[0, 600, 1439])
    rows.loc[700, "dni"] = math.nan

    flags = qc_rules.gap(rows, DAY)

    assert flagged(flags) == [
        ("00:00", "all"),
        ("10:00", "all"),
        ("11:40", "dni"),
        ("23:59", "all"),
    ]
    assert flags["value"].isna().all()


# stuck


def test_stuck_pass():
    rows = pd.concat(
        [
            minutes(9, dhi=80.0),  # 9 minutes: one short of a run
            minutes(15, start=NOON.replace(hour=6), zenith_deg=110.0, ghi=0.0, dni=0.0, dhi=0.0),
        ]
    )
    # 10 equal values, but a missing minute splits them into two runs
    split = minutes(11, start=NOON.replace(hour=20), dhi=80.0).drop(index=5)

    assert qc_rules.stuck(pd.concat([rows, split])).empty


def test_stuck_fail():
    rows = minutes(12, ghi=[500.0, *[612.4] * 10, 500.0])

    flags = qc_rules.stuck(rows)

    assert len(flags) == 10
    assert set(flags["variable"]) == {"ghi"}
    assert flags["ts_utc"].min() == NOON + timedelta(minutes=1)


# drift


def daily(ratios: list[float]) -> pd.Series:
    return pd.Series(ratios, index=pd.date_range("2026-09-01", periods=len(ratios)))


def test_drift_pass():
    result = qc_rules.drift(daily([1.00, 1.01, 0.99, 1.015, 0.985, 1.00, 1.01, 1.019]), 1.0)
    assert not result["warn"].any()


def test_drift_fail():
    # The tracker slowly slips: the ratio creeps up 0.6% a day
    result = qc_rules.drift(daily([1.00 + 0.006 * i for i in range(14)]), 1.0)

    first_warning = result.index[result["warn"]][0]
    assert first_warning == pd.Timestamp("2026-09-08")
    assert result.loc[first_warning, "deviation_pct"] == pytest.approx(2.4)


def test_drift_needs_three_days_of_data_in_the_window():
    result = qc_rules.drift(daily([1.10, 1.10, 1.10, math.nan]), 1.0)
    assert result["rolling_median"].isna().tolist() == [True, True, False, False]
    assert result["warn"].tolist() == [False, False, True, True]


def test_clear_sky_ratio_uses_only_clear_high_sun_minutes():
    clear = minutes(3)
    clear["ghi"] = qc_rules.component_sum(clear) * 1.02
    cloudy = minutes(3, dni=100.0, dhi=300.0)  # DHI/GHI > 0.2
    low_sun = minutes(3, zenith_deg=70.0)
    for skipped in (cloudy, low_sun):
        skipped["ghi"] = qc_rules.component_sum(skipped) * 1.5

    ratio = qc_rules.clear_sky_closure_ratio(pd.concat([clear, cloudy, low_sun]))

    assert ratio == pytest.approx(1.02)


# all rules together


def test_run_minute_rules_returns_the_flag_columns_with_rule_versions():
    rows = minutes(1440, start=datetime(2026, 8, 1, tzinfo=UTC)).drop(index=0)
    rows.loc[1, "ghi"] = 5000.0

    flags = qc_rules.run_minute_rules(rows, DAY)

    assert tuple(flags.columns) == FLAG_COLUMNS
    assert set(zip(flags["rule_id"], flags["rule_version"], strict=True)) == {
        ("gap", 1),
        ("limits", 1),
        ("closure", 1),
    }


def test_daytime_share_skips_night_and_gaps():
    night = minutes(4, start=NOON.replace(hour=6), zenith_deg=110.0, ghi=0.0, dni=0.0, dhi=0.0)
    rows = pd.concat([minutes(4), night], ignore_index=True)
    rows.loc[0, "ghi"] = 5000.0  # limits + closure on the same minute
    flags = qc_rules.run_minute_rules(rows, DAY)

    assert qc_rules.daytime_flagged_share(rows, flags) == (1, 4)
