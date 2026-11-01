import hashlib
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from abq_qc import db
from abq_qc.ingest import (
    duplicate_minutes,
    header_matches_station,
    load_measurements,
    raw_file_path,
    store_raw_file,
)
from abq_qc.parse import parse_solrad

FIXTURES = Path(__file__).parent / "fixtures"
DAY = date(2026, 8, 1)
URL = "https://example.test/abq26213.dat"


@pytest.fixture
def content() -> bytes:
    return (FIXTURES / "abq26213.dat").read_bytes()


@pytest.fixture
def con():
    c = db.connect(":memory:")
    yield c
    c.close()


def source_rows(con):
    return con.execute("SELECT sha256 FROM source_files ORDER BY downloaded_at").fetchall()


def test_new_file_is_saved_unchanged_and_recorded(con, tmp_path, content):
    stored = store_raw_file(con, tmp_path, DAY, URL, content, "run-1")

    assert stored.status == "new"
    assert stored.sha256 == hashlib.sha256(content).hexdigest()
    assert stored.path == raw_file_path(tmp_path, DAY, stored.sha256)
    assert stored.path.read_bytes() == content
    row = con.execute("SELECT file_date, url, size_bytes, run_id FROM source_files").fetchone()
    assert row == (DAY, URL, len(content), "run-1")


def test_same_content_again_adds_nothing(con, tmp_path, content):
    store_raw_file(con, tmp_path, DAY, URL, content, "run-1")
    stored = store_raw_file(con, tmp_path, DAY, URL, content, "run-2")

    assert stored.status == "unchanged"
    assert len(source_rows(con)) == 1


def test_changed_content_keeps_both_versions(con, tmp_path, content):
    first = store_raw_file(con, tmp_path, DAY, URL, content, "run-1")
    edited = content.replace(b"437.0", b"438.0", 1)
    second = store_raw_file(con, tmp_path, DAY, URL, edited, "run-2")

    assert second.status == "changed"
    assert second.previous_sha256 == first.sha256
    assert first.path.read_bytes() == content
    assert second.path.read_bytes() == edited
    assert [r[0] for r in source_rows(con)] == [first.sha256, second.sha256]


def test_new_and_changed_files_are_audited(con, tmp_path, content):
    first = store_raw_file(con, tmp_path, DAY, URL, content, "run-1")
    store_raw_file(con, tmp_path, DAY, URL, content, "run-2")
    second = store_raw_file(con, tmp_path, DAY, URL, content + b"\n", "run-3")

    events = con.execute(
        "SELECT event_type, details->>'sha256', details->>'previous_sha256', "
        "(details->>'data_rows')::INT, run_id FROM audit_log ORDER BY event_id"
    ).fetchall()
    assert events == [
        ("file_ingested", first.sha256, None, 1440, "run-1"),
        ("file_changed", second.sha256, first.sha256, None, "run-3"),
    ]


def test_reverting_to_an_old_version_counts_as_a_change(con, tmp_path, content):
    store_raw_file(con, tmp_path, DAY, URL, content, "run-1")
    store_raw_file(con, tmp_path, DAY, URL, content + b"\n", "run-2")
    back = store_raw_file(con, tmp_path, DAY, URL, content, "run-3")

    assert back.status == "changed"
    assert len(source_rows(con)) == 3


def test_load_writes_rows_with_lineage(con, tmp_path, content):
    stored = store_raw_file(con, tmp_path, DAY, URL, content, "run-1")
    load_measurements(con, tmp_path, DAY, "run-1", "abc123")

    n, shas, runs, versions = con.execute(
        "SELECT count(*), count(DISTINCT source_sha256), any_value(run_id), "
        "any_value(code_version) FROM measurements"
    ).fetchone()
    assert (n, shas, runs, versions) == (1440, 1, "run-1", "abc123")

    # Follow the lineage of one value back to the raw file
    ghi, sha, line = con.execute(
        "SELECT ghi, source_sha256, source_line FROM measurements "
        "WHERE ts_utc = '2026-08-01 12:00:00+00'"
    ).fetchone()
    raw_line = raw_file_path(tmp_path, DAY, sha).read_text().splitlines()[line - 1].split()
    assert (raw_line[4], raw_line[5]) == ("12", "0")
    assert float(raw_line[8]) == ghi
    assert sha == stored.sha256


def test_reload_replaces_the_day(con, tmp_path, content):
    store_raw_file(con, tmp_path, DAY, URL, content, "run-1")
    load_measurements(con, tmp_path, DAY, "run-1", "v1")
    load_measurements(con, tmp_path, DAY, "run-2", "v2")

    rows = con.execute("SELECT count(*), any_value(run_id) FROM measurements").fetchone()
    assert rows == (1440, "run-2")


def test_load_uses_the_newest_version(con, tmp_path, content):
    store_raw_file(con, tmp_path, DAY, URL, content, "run-1")
    edited = content.replace(b"437.0", b"438.0", 1)
    store_raw_file(con, tmp_path, DAY, URL, edited, "run-2")
    load_measurements(con, tmp_path, DAY, "run-2", "v1")

    first_ghi = con.execute("SELECT ghi FROM measurements ORDER BY ts_utc LIMIT 1").fetchone()
    assert first_ghi == (438.0,)


def test_load_refuses_a_tampered_raw_file(con, tmp_path, content):
    stored = store_raw_file(con, tmp_path, DAY, URL, content, "run-1")
    stored.path.write_bytes(content.replace(b"437.0", b"999.0", 1))

    with pytest.raises(ValueError, match="no longer matches"):
        load_measurements(con, tmp_path, DAY, "run-2", "v1")


def test_load_refuses_rows_from_another_day(con, tmp_path, content):
    store_raw_file(con, tmp_path, date(2026, 8, 2), URL, content, "run-1")

    with pytest.raises(ValueError, match="rows from another day"):
        load_measurements(con, tmp_path, date(2026, 8, 2), "run-1", "v1")


def test_load_without_a_source_file_fails(con, tmp_path):
    with pytest.raises(LookupError, match="no source file"):
        load_measurements(con, tmp_path, DAY, "run-1", "v1")


def test_header_check(content):
    header = parse_solrad(content.decode()).header
    assert header_matches_station(header)
    assert header_matches_station(replace(header, latitude=35.04, longitude=-106.62))
    assert not header_matches_station(replace(header, latitude=36.6))


def test_duplicate_minutes(content):
    lines = content.decode().splitlines()
    rows = parse_solrad("\n".join([*lines, lines[100]])).rows
    assert [str(t) for t in duplicate_minutes(rows)] == ["2026-08-01 01:38:00+00:00"]
    assert duplicate_minutes(parse_solrad(content.decode()).rows) == []
