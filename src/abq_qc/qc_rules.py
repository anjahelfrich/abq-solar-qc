"""The 5 QC rules, as pure functions on one day of `measurements` rows.

Each per-minute rule returns a flags frame (see FLAG_COLUMNS) holding only failures.
Drift is per day and looks across days, so it has its own functions and is not written
to qc_flags (the drift_trend asset uses it).

Thresholds live in each rule's `thresholds` dict, next to its version: changing a
threshold means bumping the version, and `abq_qc.qc.sync_rules` refuses a change without one.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

VARIABLES = ("ghi", "dni", "dhi")
ALL = "all"  # flag about the whole minute, not one sensor
FLAG_COLUMNS = ("ts_utc", "variable", "rule_id", "rule_version", "severity", "value")
DAYTIME_ZENITH_DEG = 90.0


@dataclass(frozen=True)
class Rule:
    rule_id: str
    version: int
    description: str
    thresholds: dict[str, Any]
    severity: str


LIMITS = Rule(
    "limits",
    1,
    "BSRN physically possible limits. Upper: GHI <= Sa*1.5*mu0^1.2 + 100, "
    "DHI <= Sa*0.95*mu0^1.2 + 50, DNI <= Sa. Lower: -4 W/m2 by day, -10 W/m2 at night.",
    {
        "solar_constant": 1361.0,
        "ghi_factor": 1.5,
        "ghi_offset": 100.0,
        "dhi_factor": 0.95,
        "dhi_offset": 50.0,
        "mu0_exponent": 1.2,
        "day_min": -4.0,
        "night_min": -10.0,
    },
    "bad",
)
CLOSURE = Rule(
    "closure",
    1,
    "GHI / (DNI*cosZ + DHI) within +-8% for zenith < 75 deg and +-15% for 75-93 deg, "
    "only where the sum > 50 W/m2. Flags the whole minute (variable 'all'); value = ratio.",
    {
        "low_zenith_max": 75.0,
        "low_zenith_tolerance": 0.08,
        "high_zenith_max": 93.0,
        "high_zenith_tolerance": 0.15,
        "min_sum": 50.0,
    },
    "suspect",
)
GAP = Rule(
    "gap",
    1,
    "Expected minute (1440 per UTC day) with no row (variable 'all'), "
    "or a row whose value is NOAA's missing value (that variable only).",
    {"minutes_per_day": 1440},
    "missing",
)
STUCK = Rule(
    "stuck",
    1,
    "Same value for 10+ consecutive minutes while > 5 W/m2. The 1-second std dev = 0 "
    "condition was dropped: std is reported in ~0.084 steps, so steady clear-sky DHI "
    "often reads 0 for 10+ minutes.",
    {"min_run_minutes": 10, "min_value": 5.0},
    "suspect",
)
DRIFT = Rule(
    "drift",
    1,
    "Daily median closure ratio on clear-sky minutes (zenith < 60 deg, GHI > 50, DHI/GHI < 0.2). "
    "Warns when the 7-day rolling median is more than 2% from the August baseline. Per day.",
    {
        "zenith_max": 60.0,
        "ghi_min": 50.0,
        "diffuse_fraction_max": 0.2,
        "window_days": 7,
        "min_days_in_window": 3,
        "warn_pct": 2.0,
        "baseline": "median of August daily ratios",
    },
    "warning",
)

RULES = (LIMITS, CLOSURE, GAP, STUCK, DRIFT)


def run_minute_rules(rows: pd.DataFrame, day: date) -> pd.DataFrame:
    """All per-minute rules for one UTC day of `measurements` rows."""
    flags = [limits(rows), closure(rows), gap(rows, day), stuck(rows)]
    return pd.concat(flags, ignore_index=True).sort_values(["ts_utc", "rule_id", "variable"])


def limits(rows: pd.DataFrame) -> pd.DataFrame:
    t = LIMITS.thresholds
    sa = extraterrestrial_irradiance(rows["ts_utc"])
    mu0 = np.cos(np.radians(rows["zenith_deg"])).clip(lower=0) ** t["mu0_exponent"]
    upper = {
        "ghi": sa * t["ghi_factor"] * mu0 + t["ghi_offset"],
        "dni": sa,
        "dhi": sa * t["dhi_factor"] * mu0 + t["dhi_offset"],
    }
    lower = np.where(rows["zenith_deg"] < DAYTIME_ZENITH_DEG, t["day_min"], t["night_min"])

    parts = []
    for var in VARIABLES:
        v = rows[var]
        bad = (v > upper[var]) | (v < lower)
        parts.append(_flags(rows.loc[bad, "ts_utc"], var, LIMITS, v[bad]))
    return pd.concat(parts, ignore_index=True)


def closure(rows: pd.DataFrame) -> pd.DataFrame:
    t = CLOSURE.thresholds
    ratio = closure_ratio(rows)
    zen = rows["zenith_deg"]
    total = component_sum(rows)
    tolerance = np.where(
        zen < t["low_zenith_max"], t["low_zenith_tolerance"], t["high_zenith_tolerance"]
    )
    applies = (total > t["min_sum"]) & (zen < t["high_zenith_max"])
    bad = applies & ((ratio - 1).abs() > tolerance)
    return _flags(rows.loc[bad, "ts_utc"], ALL, CLOSURE, ratio[bad])


def gap(rows: pd.DataFrame, day: date) -> pd.DataFrame:
    start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    expected = pd.date_range(
        start, periods=GAP.thresholds["minutes_per_day"], freq="min", unit="us"
    )
    missing = expected.difference(pd.DatetimeIndex(rows["ts_utc"]))
    parts = [_flags(pd.Series(missing), ALL, GAP, np.nan)]
    for var in VARIABLES:
        empty = rows[var].isna()
        parts.append(_flags(rows.loc[empty, "ts_utc"], var, GAP, np.nan))
    return pd.concat(parts, ignore_index=True)


def stuck(rows: pd.DataFrame) -> pd.DataFrame:
    t = STUCK.thresholds
    rows = rows.sort_values("ts_utc")
    next_minute = rows["ts_utc"].diff() == pd.Timedelta(minutes=1)
    parts = []
    for var in VARIABLES:
        v = rows[var]
        # A run ends when the value changes or a minute is missing (NaN != NaN also ends it)
        run_id = ((v != v.shift()) | ~next_minute).cumsum()
        run_length = v.groupby(run_id).transform("size")
        bad = (run_length >= t["min_run_minutes"]) & (v > t["min_value"])
        parts.append(_flags(rows.loc[bad, "ts_utc"], var, STUCK, v[bad]))
    return pd.concat(parts, ignore_index=True)


def clear_sky_closure_ratio(rows: pd.DataFrame) -> float:
    """Drift input: median closure ratio over one day's clear-sky minutes (NaN if none)."""
    t = DRIFT.thresholds
    clear = (
        (rows["zenith_deg"] < t["zenith_max"])
        & (rows["ghi"] > t["ghi_min"])
        & (rows["dhi"] / rows["ghi"] < t["diffuse_fraction_max"])
    )
    return float(closure_ratio(rows)[clear].median())


def drift(daily_ratio: pd.Series, baseline: float) -> pd.DataFrame:
    """7-day rolling median of the daily clear-sky ratio vs the baseline.

    `daily_ratio` is indexed by UTC date; days without clear-sky minutes may be NaN or absent.
    """
    t = DRIFT.thresholds
    s = daily_ratio.copy()
    s.index = pd.DatetimeIndex(s.index)
    s = s.sort_index()
    rolling = s.rolling(
        timedelta(days=t["window_days"]), min_periods=t["min_days_in_window"]
    ).median()
    deviation_pct = (rolling / baseline - 1) * 100
    return pd.DataFrame(
        {
            "daily_ratio": s,
            "rolling_median": rolling,
            "baseline": baseline,
            "deviation_pct": deviation_pct,
            "warn": deviation_pct.abs() > t["warn_pct"],
        }
    )


def daytime_flagged_share(rows: pd.DataFrame, flags: pd.DataFrame) -> tuple[int, int]:
    """(flagged, total) received daytime minutes with any flag. Gaps are left out:
    availability has its own check."""
    daytime = set(rows.loc[rows["zenith_deg"] < DAYTIME_ZENITH_DEG, "ts_utc"])
    flagged = set(flags.loc[flags["rule_id"] != GAP.rule_id, "ts_utc"]) & daytime
    return len(flagged), len(daytime)


def component_sum(rows: pd.DataFrame) -> pd.Series:
    """DNI*cosZ + DHI: what GHI should be."""
    return rows["dni"] * np.cos(np.radians(rows["zenith_deg"])) + rows["dhi"]


def closure_ratio(rows: pd.DataFrame) -> pd.Series:
    return rows["ghi"] / component_sum(rows)


def extraterrestrial_irradiance(ts: pd.Series) -> pd.Series:
    """Sa: solar constant corrected for Earth-Sun distance (about +-3.3% over the year)."""
    doy = ts.dt.day_of_year
    return LIMITS.thresholds["solar_constant"] * (1 + 0.033 * np.cos(2 * np.pi * doy / 365))


def _flags(ts: pd.Series, variable: str, rule: Rule, value) -> pd.DataFrame:
    ts = pd.Series(ts).reset_index(drop=True).astype("datetime64[us, UTC]")
    values = value.to_numpy() if isinstance(value, pd.Series) else value
    return pd.DataFrame(
        {
            "ts_utc": ts,
            "variable": pd.Series(variable, index=ts.index, dtype="object"),
            "rule_id": pd.Series(rule.rule_id, index=ts.index, dtype="object"),
            "rule_version": pd.Series(rule.version, index=ts.index, dtype="int32"),
            "severity": pd.Series(rule.severity, index=ts.index, dtype="object"),
            "value": pd.Series(values, index=ts.index, dtype="float64"),
        }
    )
