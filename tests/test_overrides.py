import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from abq_qc import db
from abq_qc.ingest import load_measurements, store_raw_file
from abq_qc.overrides import COLUMNS, load_overrides, read_overrides
from abq_qc.qc import run_qc

FIXTURES = Path(__file__).parent / "fixtures"
DAY = date(2026, 8, 1)
HEADER = ",".join(COLUMNS)
DNI_ACCEPT = (
    "OVR-0001,2026-08-01T21:35:00Z,2026-08-01T23:17:00Z,dni,limits,accept,thermal offset,Anja"
)


def csv(*rows: str) -> bytes:
    return "\n".join([HEADER, *rows]).encode() + b"\n"


@pytest.fixture
def con():
    c = db.connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def day(con, tmp_path):
    """Aug 1 loaded: 23 DNI limits flags 21:35-23:17 UTC and 47 closure flags."""
    content = (FIXTURES / "abq26213.dat").read_bytes()
    store_raw_file(con, tmp_path, DAY, "https://example.test", content, "run-ingest")
    load_measurements(con, tmp_path, DAY, "run-ingest", "v1")
    return DAY


def apply(con, content: bytes, run_id: str = "run-ovr"):
    file = read_overrides(content)
    assert file.problems == []
    load_overrides(con, file, run_id)
    return run_qc(con, DAY, "run-qc")


def events(con, event_type: str) -> list[dict]:
    rows = con.execute(
        "SELECT actor, details FROM audit_log WHERE event_type = ? ORDER BY event_id",
        [event_type],
    ).fetchall()
    return [{"actor": a, **json.loads(d)} for a, d in rows]


def test_valid_file_parses():
    file = read_overrides(
        csv(
            DNI_ACCEPT,
            "OVR-0002,2026-08-02T10:00:00Z,2026-08-02T10:15:00Z,all,,flag_bad,cleaning,Anja",
        )
    )
    assert file.problems == []
    assert list(file.rows["override_id"]) == ["OVR-0001", "OVR-0002"]
    assert pd.isna(file.rows["rule_id"].iloc[1])


def test_repo_overrides_file_is_valid():
    path = Path(__file__).parents[1] / "overrides" / "manual_overrides.csv"
    assert read_overrides(path.read_bytes()).problems == []


@pytest.mark.parametrize(
    ("row", "problem"),
    [
        (DNI_ACCEPT.replace("21:35:00Z", "21:35:00"), "start_utc '2026-08-01T21:35:00'"),
        (DNI_ACCEPT.replace("23:17", "20:00"), "start_utc is after end_utc"),
        (DNI_ACCEPT.replace(",dni,", ",uvb,"), "variable must be one of"),
        (DNI_ACCEPT.replace(",accept,", ",delete,"), "action must be one of"),
        (DNI_ACCEPT.replace(",limits,", ",gap,"), "accept needs rule_id"),
        (DNI_ACCEPT.replace(",accept,", ",flag_bad,"), "flag_bad doesn't take a rule_id"),
        (DNI_ACCEPT.replace(",thermal offset,", ",,"), "reason is empty"),
    ],
)
def test_invalid_row_is_reported_with_its_line(row, problem):
    problems = read_overrides(csv(row)).problems
    assert len(problems) == 1
    assert problems[0].startswith("line 2: ")
    assert problem in problems[0]


def test_duplicate_id_and_wrong_header():
    assert read_overrides(csv(DNI_ACCEPT, DNI_ACCEPT)).problems == [
        "line 3: override_id 'OVR-0001' is used twice"
    ]
    assert read_overrides(b"id,start\n").problems[0].startswith("header must be")
    unquoted = DNI_ACCEPT.replace("thermal offset", "thermal offset, by hand")
    assert "more fields than the header" in read_overrides(csv(unquoted)).problems[0]


def test_accept_keeps_the_flag_and_changes_final_status(con, day):
    result = apply(con, csv(DNI_ACCEPT))

    # The automatic flags are untouched
    assert result.counts_by_rule()["limits"] == 23
    status = con.execute(
        "SELECT variable, auto_rules, override_ids, final_status, count(*) FROM qc_status "
        "WHERE 'limits' = ANY(auto_rules) GROUP BY ALL"
    ).fetchall()
    assert status == [("dni", ["limits"], ["OVR-0001"], "good", 23)]
    assert events(con, "override_applied") == [
        {
            "actor": "Anja",
            "day": "2026-08-01",
            "override_id": "OVR-0001",
            "action": "accept",
            "reason": "thermal offset",
            "minutes": 23,
        }
    ]


def test_sensor_override_cannot_accept_a_whole_minute_flag(con, day):
    """Closure flags are on 'all'; accepting them on 'ghi' alone isn't enough."""
    apply(con, csv("OVR-0001,2026-08-01T00:00:00Z,2026-08-01T23:59:00Z,ghi,closure,accept,x,Anja"))

    status = con.execute(
        "SELECT DISTINCT final_status FROM qc_status WHERE 'closure' = ANY(auto_rules)"
    ).fetchall()
    assert status == [("suspect",)]
    assert events(con, "override_applied") == []


def test_flag_bad_marks_values_without_an_automatic_flag(con, day):
    apply(
        con, csv("OVR-0002,2026-08-01T18:00:00Z,2026-08-01T18:14:00Z,all,,flag_bad,cleaning,Anja")
    )

    rows = con.execute(
        "SELECT variable, count(*) FROM qc_status WHERE final_status = 'bad' "
        "AND list_contains(override_ids, 'OVR-0002') GROUP BY ALL ORDER BY 1"
    ).fetchall()
    assert rows == [("dhi", 15), ("dni", 15), ("ghi", 15)]
    assert events(con, "override_applied")[0]["minutes"] == 15


def test_loading_logs_what_changed(con):
    load_overrides(con, read_overrides(csv(DNI_ACCEPT)), "run-1")
    load_overrides(con, read_overrides(csv(DNI_ACCEPT)), "run-2")  # same file: no event
    edited = DNI_ACCEPT.replace("thermal offset", '"thermal offset, checked by hand"')
    new = "OVR-0002,2026-08-02T10:00:00Z,2026-08-02T10:15:00Z,all,,flag_bad,cleaning,Anja"
    load_overrides(con, read_overrides(csv(edited, new)), "run-3")
    load_overrides(con, read_overrides(csv(new)), "run-4")

    loaded = [(e["added"], e["removed"], e["changed"]) for e in events(con, "overrides_loaded")]
    assert loaded == [
        (["OVR-0001"], [], []),
        (["OVR-0002"], [], ["OVR-0001"]),
        ([], ["OVR-0001"], []),
    ]
    assert con.execute("SELECT override_id, run_id FROM overrides").fetchall() == [
        ("OVR-0002", "run-4")
    ]


def test_qc_run_records_rule_versions_and_overrides_file(con, day):
    file = read_overrides(csv(DNI_ACCEPT))
    load_overrides(con, file, "run-ovr")
    run_qc(con, DAY, "run-qc")

    (qc_run,) = events(con, "qc_run")
    assert qc_run["overrides_sha256"] == file.sha256
    assert qc_run["rule_versions"]["closure"] == 1
    assert qc_run["flag_counts"] == {"limits": 23, "closure": 47, "gap": 0, "stuck": 0}
