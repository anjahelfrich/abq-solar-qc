import re
from pathlib import Path

import pytest

from abq_qc import db
from abq_qc.overrides import COLUMNS as CSV_COLUMNS
from abq_qc.snapshot import TABLES

DOC = Path(__file__).parents[1] / "docs" / "data_dictionary.md"
CSV_SECTION = "overrides/manual_overrides.csv"


def documented_columns() -> dict[str, list[str]]:
    """Section name -> column names from the first table under each `## ` heading."""
    sections: dict[str, list[str]] = {}
    current, in_table, done = None, False, False
    for line in DOC.read_text().splitlines():
        if line.startswith("## "):
            current = re.search(r"`([^`]+)`", line).group(1)
            sections[current] = []
            in_table = done = False
        elif current and not done:
            if line.startswith("|"):
                in_table = True
                if m := re.match(r"\| `(\w+)` \|", line):
                    sections[current].append(m.group(1))
            elif in_table:
                done = True
    return sections


@pytest.mark.parametrize("table", TABLES)
def test_every_column_is_documented(table):
    con = db.connect(":memory:")
    actual = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
    assert documented_columns()[table] == actual


def test_overrides_csv_is_documented():
    assert documented_columns()[CSV_SECTION] == list(CSV_COLUMNS)


def test_every_section_is_a_real_table():
    assert set(documented_columns()) == {*TABLES, CSV_SECTION}
