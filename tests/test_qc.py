from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from abq_qc import db, qc_rules
from abq_qc.ingest import load_measurements, store_raw_file
from abq_qc.qc import RuleChangedWithoutNewVersion, run_qc, sync_rules

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def con():
    c = db.connect(":memory:")
    yield c
    c.close()


def load(con, raw_dir: Path, fixture: str, day: date) -> None:
    content = (FIXTURES / fixture).read_bytes()
    store_raw_file(con, raw_dir, day, "https://example.test", content, "run-ingest")
    load_measurements(con, raw_dir, day, "run-ingest", "v1")


def test_sync_rules_adds_each_version_once(con):
    sync_rules(con)
    sync_rules(con)
    rows = con.execute("SELECT rule_id, version FROM qc_rules ORDER BY rule_id").fetchall()
    assert rows == sorted((r.rule_id, r.version) for r in qc_rules.RULES)


def test_sync_rules_refuses_a_changed_threshold_without_a_new_version(con, monkeypatch):
    sync_rules(con)
    looser = replace(qc_rules.CLOSURE, thresholds={**qc_rules.CLOSURE.thresholds, "min_sum": 20})
    monkeypatch.setattr("abq_qc.qc.RULES", (looser,))

    with pytest.raises(RuleChangedWithoutNewVersion, match="closure"):
        sync_rules(con)

    monkeypatch.setattr("abq_qc.qc.RULES", (replace(looser, version=2),))
    sync_rules(con)
    versions = con.execute("SELECT version FROM qc_rules WHERE rule_id = 'closure'").fetchall()
    assert versions == [(1,), (2,)]


def test_real_day_flags(con, tmp_path):
    day = date(2026, 8, 1)
    load(con, tmp_path, "abq26213.dat", day)

    result = run_qc(con, day, "run-qc")

    # Monsoon afternoon clouds: closure fails, and DNI dips below -4 W/m2 under thick cloud
    assert result.counts_by_rule() == {"limits": 23, "closure": 47, "gap": 0, "stuck": 0}
    assert (result.flagged_daytime_minutes, result.daytime_minutes) == (70, 834)
    stored = con.execute("SELECT count(*), any_value(run_id) FROM qc_flags").fetchone()
    assert stored == (70, "run-qc")


def test_partial_day_gets_gap_flags(con, tmp_path):
    day = date(2026, 3, 4)
    load(con, tmp_path, "abq26063.dat", day)

    result = run_qc(con, day, "run-qc")

    assert result.counts_by_rule()["gap"] == 1440 - 892


def test_rerun_replaces_the_day(con, tmp_path):
    day = date(2026, 8, 1)
    load(con, tmp_path, "abq26213.dat", day)
    run_qc(con, day, "run-1")
    run_qc(con, day, "run-2")

    rows = con.execute("SELECT count(*), count(DISTINCT run_id) FROM qc_flags").fetchone()
    assert rows == (70, 1)
