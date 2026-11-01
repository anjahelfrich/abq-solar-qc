from datetime import date
from pathlib import Path

import pytest

from abq_qc import db
from abq_qc.ingest import load_measurements, store_raw_file
from abq_qc.qc import run_qc

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def con():
    c = db.connect(":memory:")
    yield c
    c.close()


def add_events(con, n: int) -> None:
    for i in range(n):
        db.append_audit(con, "test_event", "pipeline", {"i": i, "value": 1.5}, f"run-{i}")


def test_empty_and_fresh_chains_verify(con):
    assert db.verify_audit_chain(con) == []
    add_events(con, 3)
    assert db.verify_audit_chain(con) == []


def test_chain_links_each_event_to_the_one_before(con):
    add_events(con, 3)
    rows = con.execute("SELECT prev_hash, event_hash FROM audit_log ORDER BY event_id").fetchall()
    assert rows[0][0] == db.GENESIS_HASH
    assert [r[0] for r in rows[1:]] == [r[1] for r in rows[:-1]]


def test_edited_event_is_detected(con):
    add_events(con, 3)
    con.execute('UPDATE audit_log SET details = \'{"i": 99, "value": 1.5}\' WHERE event_id = 2')
    assert db.verify_audit_chain(con) == ["event 2: content doesn't match its hash"]


def test_deleted_event_is_detected(con):
    add_events(con, 3)
    con.execute("DELETE FROM audit_log WHERE event_id = 2")
    problems = db.verify_audit_chain(con)
    assert "event 2 is missing (next is 3)" in problems
    assert "event 3: prev_hash doesn't match the event before it" in problems


def test_rerunning_a_day_adds_events_and_never_changes_old_ones(con, tmp_path):
    day = date(2026, 8, 1)
    content = (FIXTURES / "abq26213.dat").read_bytes()
    store_raw_file(con, tmp_path, day, "https://example.test", content, "run-1")
    load_measurements(con, tmp_path, day, "run-1", "v1")
    run_qc(con, day, "run-1")
    before = con.execute("SELECT * FROM audit_log ORDER BY event_id").fetchall()

    store_raw_file(con, tmp_path, day, "https://example.test", content, "run-2")
    load_measurements(con, tmp_path, day, "run-2", "v1")
    run_qc(con, day, "run-2")
    after = con.execute("SELECT * FROM audit_log ORDER BY event_id").fetchall()

    assert after[: len(before)] == before
    assert [r[2] for r in after[len(before) :]] == ["qc_run"]
    assert db.verify_audit_chain(con) == []
