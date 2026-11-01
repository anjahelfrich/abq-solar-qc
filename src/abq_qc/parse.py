"""Parse a NOAA SOLRAD daily file (one UTC day of 1-minute data) into typed rows.

File format: https://gml.noaa.gov/aftp/data/radiation/solrad/README_SOLRAD.txt
"""

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

MISSING = -9999.9
HEADER_LINES = 2

# Measured values in file order. Each one is followed by its NOAA QC flag.
MEASURED = ("ghi", "dni", "dhi", "uvb")
STD_DEVS = ("std_ghi", "std_dni", "std_dhi", "std_uvb")
# year jday month day hour min dt zen, 5 (value, flag) pairs, 4 std devs.
# The 5th pair is UVB sensor temperature; NOAA doesn't document its unit, so it's not kept.
N_FIELDS = 22

COLUMNS = (
    "ts_utc",
    "zenith_deg",
    *MEASURED,
    *STD_DEVS,
    *(f"noaa_qc_{m}" for m in MEASURED),
    "source_line",
)


class SolradParseError(ValueError):
    pass


@dataclass(frozen=True)
class StationHeader:
    name: str
    latitude: float
    longitude: float
    elevation_m: int
    hours_to_lst: int
    version: int


@dataclass(frozen=True)
class SolradFile:
    header: StationHeader
    rows: pd.DataFrame


def parse_solrad(text: str) -> SolradFile:
    """Parse the full text of one SOLRAD .dat file.

    - `ts_utc` is the end of each 1-minute averaging period, as NOAA writes it.
    - `-9999.9` (NOAA's missing value) becomes NaN; the NOAA flag is kept as-is.
    - `source_line` is the 1-based line number in the file, for lineage.
    - Missing minutes are simply absent (the file has no placeholder rows).

    Raises SolradParseError, with the line number, on any malformed line.
    """
    lines = text.splitlines()
    if len(lines) < HEADER_LINES:
        raise SolradParseError("file is shorter than the 2-line header")
    header = _parse_header(lines[0], lines[1])

    records = []
    for line_no, line in enumerate(lines[HEADER_LINES:], start=HEADER_LINES + 1):
        if line.strip():
            records.append(_parse_row(line, line_no))

    rows = pd.DataFrame.from_records(records, columns=list(COLUMNS))
    rows = rows.astype(
        {
            "ts_utc": "datetime64[us, UTC]",
            "zenith_deg": "float64",
            **{c: "float64" for c in (*MEASURED, *STD_DEVS)},
            **{f"noaa_qc_{m}": "int8" for m in MEASURED},
            "source_line": "int32",
        }
    )
    return SolradFile(header=header, rows=rows)


def _parse_header(name_line: str, meta_line: str) -> StationHeader:
    parts = meta_line.split()
    if len(parts) != 6 or parts[4] != "version":
        raise SolradParseError(
            f"line 2: expected 'lat lon elev h_to_lst version N', got {meta_line!r}"
        )
    try:
        return StationHeader(
            name=name_line.strip(),
            latitude=float(parts[0]),
            longitude=float(parts[1]),
            elevation_m=int(parts[2]),
            hours_to_lst=int(parts[3]),
            version=int(parts[5]),
        )
    except ValueError as e:
        raise SolradParseError(f"line 2: {e}") from e


def _parse_row(line: str, line_no: int) -> tuple:
    f = line.split()
    if len(f) != N_FIELDS:
        raise SolradParseError(f"line {line_no}: expected {N_FIELDS} fields, got {len(f)}")
    try:
        year, jday, month, day, hour, minute = (int(x) for x in f[:6])
        zenith = _value(f[7])
        values = tuple(_value(f[i]) for i in range(8, 16, 2))
        flags = tuple(int(f[i]) for i in range(9, 16, 2))
        stds = tuple(_value(x) for x in f[18:])
    except ValueError as e:
        raise SolradParseError(f"line {line_no}: {e}") from e

    ts = datetime(year, 1, 1, tzinfo=UTC) + timedelta(days=jday - 1, hours=hour, minutes=minute)
    if (ts.month, ts.day) != (month, day):
        raise SolradParseError(
            f"line {line_no}: day of year {jday} does not match month/day {month}/{day}"
        )
    return (ts, zenith, *values, *stds, *flags, line_no)


def _value(s: str) -> float:
    v = float(s)
    return math.nan if v == MISSING else v
