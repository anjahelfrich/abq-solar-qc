import json
import math
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest
import queries as q
from streamlit.testing.v1 import AppTest

from abq_qc import db
from abq_qc.ingest import load_measurements, store_raw_file
from abq_qc.overrides import COLUMNS, load_overrides, read_overrides
from abq_qc.qc import run_qc
from abq_qc.snapshot import write_snapshot
from abq_qc.summary import build_drift_trend, summarize_day

FIXTURES = Path(__file__).parent / "fixtures"
ROOT = Path(__file__).parents[1]
AUG1, MAR4 = date(2026, 8, 1), date(2026, 3, 4)
OVR = "OVR-0001,2026-08-01T21:35:00Z,2026-08-01T23:17:00Z,dni,limits,accept,thermal offset,Anja"


@pytest.fixture(scope="module")
def snap(tmp_path_factory) -> Path:
    """Snapshot of Aug 1 (full day, one override) and Mar 4 (548 minutes not in the file)."""
    tmp = tmp_path_factory.mktemp("snap")
    con = db.connect(":memory:")
    overrides = read_overrides(("\n".join([",".join(COLUMNS), OVR]) + "\n").encode())
    load_overrides(con, overrides, "r")
    for day, name in [(MAR4, "abq26063.dat"), (AUG1, "abq26213.dat")]:
        content = (FIXTURES / name).read_bytes()
        store_raw_file(con, tmp / "raw", day, f"https://example.test/{name}", content, "r")
        load_measurements(con, tmp / "raw", day, "r", "v1")
        run_qc(con, day, "r")
        summarize_day(con, day, "r")
    build_drift_trend(con, "r")
    write_snapshot(con, tmp / "snapshot", "abc123")
    return tmp / "snapshot"


@pytest.fixture
def con(snap):
    return q.cursor(q.open_snapshot(snap))


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def test_value_availability_counts_a_missing_minute_as_three_values(con):
    daily = q.daily(con)
    daily = daily.set_index(daily["day"].dt.date)

    assert daily.loc[AUG1, "value_availability_pct"] == 100.0
    assert daily.loc[MAR4, "missing_values"] == 3 * (1440 - 892)
    assert math.isclose(daily.loc[MAR4, "value_availability_pct"], 100 * 892 / 1440)
    tiles = q.overview_tiles(q.daily(con))
    assert tiles["days_with_gaps"] == 1


def test_minutes_includes_missing_minutes_and_override_status(con):
    m = q.minutes(con, utc(2026, 3, 4), utc(2026, 3, 5))
    assert len(m) == 1440
    missing = m[m["source_line"].isna()]
    assert len(missing) == 1440 - 892
    assert (missing["status_ghi"] == "missing").all() and missing["ghi"].isna().all()

    m = q.minutes(con, utc(2026, 8, 1, 21, 35), utc(2026, 8, 1, 21, 36))
    row = m.iloc[0]
    assert (row["status_dni"], row["rules_dni"], row["overrides_dni"]) == (
        "good",
        "limits",
        "OVR-0001",
    )
    assert row["source_url"].endswith("abq26213.dat")


def test_local_day_bounds_and_csv_include_utc():
    start, end = q.local_day_bounds(AUG1, AUG1, "America/Denver")
    assert (start, end) == (utc(2026, 8, 1, 6), utc(2026, 8, 2, 6))


def test_csv_has_local_and_utc_time(con):
    m = q.minutes(con, utc(2026, 8, 1, 6), utc(2026, 8, 1, 6, 2))
    lines = q.export_csv(m, "America/Denver").decode().splitlines()
    assert lines[0].startswith("time_America/Denver,ts_utc,ghi,")
    assert lines[1].startswith("2026-08-01T00:00:00-06:00,2026-08-01T06:00:00Z,")


def test_lookup_traces_a_minute_to_its_file_and_line(con):
    found = q.lookup(con, utc(2026, 8, 1, 21, 35))
    r = found["row"]
    assert r["url"] == "https://example.test/abq26213.dat"
    assert r["source_line"] == 1298
    assert r["code_version"] == "v1"
    assert q.lookup(con, utc(2026, 1, 1)) is None


def test_untouched_snapshot_verifies(snap):
    assert q.verify_snapshot(q.open_snapshot(snap), snap) == []


def test_changed_file_fails_verification(snap, tmp_path):
    copy = _copy(snap, tmp_path)
    _rewrite(copy / "measurements.parquet", "UPDATE t SET ghi = ghi + 1 WHERE source_line = 3")

    problems = q.verify_snapshot(q.open_snapshot(copy), copy)
    assert problems == ["measurements.parquet: SHA-256 doesn't match the manifest"]


def test_edited_audit_event_fails_even_with_a_matching_manifest(snap, tmp_path):
    copy = _copy(snap, tmp_path)
    path = copy / "audit_log.parquet"
    _rewrite(path, "UPDATE t SET actor = 'someone else' WHERE event_id = 2")
    manifest = json.loads((copy / "manifest.json").read_text())
    manifest["tables"]["audit_log"]["sha256"] = q.hashlib.sha256(path.read_bytes()).hexdigest()
    (copy / "manifest.json").write_text(json.dumps(manifest))

    problems = q.verify_snapshot(q.open_snapshot(copy), copy)
    assert "event 2: content doesn't match its hash" in problems


def test_removed_newest_audit_event_fails_verification(snap, tmp_path):
    copy = _copy(snap, tmp_path)
    _rewrite(
        copy / "audit_log.parquet",
        "DELETE FROM t WHERE event_id = (SELECT max(event_id) FROM t)",
    )

    problems = q.verify_snapshot(q.open_snapshot(copy), copy)
    assert "newest audit event doesn't match the one recorded in the manifest" in problems


@pytest.mark.parametrize(
    "page",
    ["views/overview.py", "views/explore.py", "views/drift.py", "views/audit.py", "views/rules.py"],
)
def test_page_renders_on_the_committed_snapshot(page):
    at = AppTest.from_file(str(ROOT / "dashboard" / "app.py"), default_timeout=60)
    at.run()
    at.switch_page(page).run()
    assert not at.exception, [e.message for e in at.exception]
    assert at.title[0].value


def test_audit_page_reports_committed_snapshot_verified():
    at = AppTest.from_file(str(ROOT / "dashboard" / "app.py"), default_timeout=60)
    at.run()
    at.switch_page("views/audit.py").run()
    assert at.success and at.success[0].value.startswith("Verified")


def _copy(snap: Path, tmp_path: Path) -> Path:
    out = tmp_path / "snapshot"
    out.mkdir()
    for f in snap.iterdir():
        (out / f.name).write_bytes(f.read_bytes())
    return out


def _rewrite(path: Path, sql: str) -> None:
    c = duckdb.connect()
    c.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path}')")
    c.execute(sql)
    c.execute(f"COPY t TO '{path}' (FORMAT parquet)")
