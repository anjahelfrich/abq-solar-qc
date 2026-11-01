from pathlib import Path

import pandas as pd
import pytest

from abq_qc.parse import COLUMNS, SolradParseError, parse_solrad

FIXTURES = Path(__file__).parent / "fixtures"
FULL_DAY = "abq26213.dat"  # 2026-08-01, complete
PARTIAL_DAY = "abq26063.dat"  # 2026-03-04, file stops at 14:51 UTC


def load(name: str) -> str:
    return (FIXTURES / name).read_text()


@pytest.fixture(scope="module")
def full_day():
    return parse_solrad(load(FULL_DAY))


@pytest.fixture(scope="module")
def partial_day():
    return parse_solrad(load(PARTIAL_DAY))


def test_header(full_day):
    h = full_day.header
    assert h.name == "Albuquerque"
    assert (h.latitude, h.longitude) == (35.03796, -106.62211)
    assert h.elevation_m == 1617
    assert h.hours_to_lst == -7
    assert h.version == 1


def test_full_day_has_every_minute(full_day):
    rows = full_day.rows
    assert list(rows.columns) == list(COLUMNS)
    assert len(rows) == 1440
    expected = pd.date_range("2026-08-01", periods=1440, freq="min", tz="UTC")
    assert (rows["ts_utc"].to_numpy() == expected.to_numpy()).all()


def test_dtypes(full_day):
    rows = full_day.rows
    assert str(rows["ts_utc"].dtype) == "datetime64[us, UTC]"
    assert rows["ghi"].dtype == "float64"
    assert rows["noaa_qc_dni"].dtype == "int8"


def test_first_row_values_and_lineage(full_day):
    first = full_day.rows.iloc[0]
    assert first["zenith_deg"] == 64.94
    assert (first["ghi"], first["dni"], first["dhi"]) == (437.0, 772.3, 121.3)
    assert first["std_dni"] == 1.942
    assert first["source_line"] == 3  # lines 1-2 are the header

    last = full_day.rows.iloc[-1]
    assert last["source_line"] == 1442


def test_source_line_points_at_the_right_line(full_day):
    lines = load(FULL_DAY).splitlines()
    row = full_day.rows.iloc[1000]
    raw = lines[row["source_line"] - 1].split()
    assert int(raw[4]) * 60 + int(raw[5]) == row["ts_utc"].hour * 60 + row["ts_utc"].minute
    assert float(raw[8]) == row["ghi"]


def test_noaa_flags_are_kept(full_day):
    # NOAA flagged DNI (about -11 W/m2 with the sun up) for 4 minutes on this day
    flagged = full_day.rows[full_day.rows["noaa_qc_dni"] != 0]
    assert flagged["ts_utc"].dt.strftime("%H:%M").tolist() == ["22:20", "22:21", "22:22", "22:23"]
    assert (flagged["noaa_qc_dni"] == 1).all()
    assert (flagged["dni"] < -10).all()


def test_partial_day_keeps_only_minutes_present(partial_day):
    rows = partial_day.rows
    assert len(rows) == 892
    assert rows["ts_utc"].iloc[0] == pd.Timestamp("2026-03-04 00:00", tz="UTC")
    assert rows["ts_utc"].iloc[-1] == pd.Timestamp("2026-03-04 14:51", tz="UTC")
    assert rows["ts_utc"].is_monotonic_increasing
    assert rows["ts_utc"].is_unique


def test_missing_value_becomes_nan_and_keeps_flag():
    lines = load(FULL_DAY).splitlines()[:3]
    f = lines[2].split()
    f[10], f[11] = "-9999.9", "1"  # DNI value and its flag
    lines[2] = " ".join(f)

    row = parse_solrad("\n".join(lines)).rows.iloc[0]
    assert pd.isna(row["dni"])
    assert row["noaa_qc_dni"] == 1
    assert row["ghi"] == 437.0


def test_short_line_raises_with_line_number():
    lines = load(FULL_DAY).splitlines()[:5]
    lines[3] = lines[3].rsplit(maxsplit=1)[0]  # drop the last field
    with pytest.raises(SolradParseError, match="line 4: expected 22 fields, got 21"):
        parse_solrad("\n".join(lines))


def test_bad_header_raises():
    lines = load(FULL_DAY).splitlines()[:3]
    lines[1] = "35.03796 -106.62211 1617"
    with pytest.raises(SolradParseError, match="line 2"):
        parse_solrad("\n".join(lines))


def test_day_of_year_must_match_date():
    lines = load(FULL_DAY).splitlines()[:3]
    lines[2] = lines[2].replace(" 2026 213 ", " 2026 214 ", 1)
    with pytest.raises(SolradParseError, match="line 3: day of year 214"):
        parse_solrad("\n".join(lines))
