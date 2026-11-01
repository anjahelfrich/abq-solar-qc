"""Dagster resources: where the data lives, and where it comes from."""

import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import dagster as dg
import duckdb

from abq_qc import db

USER_AGENT = "abq-qc/0.1 (+https://github.com/anjahelfrich/abq-solar-qc)"


class Storage(dg.ConfigurableResource):
    data_dir: str = "data"
    overrides_csv: str = "overrides/manual_overrides.csv"
    snapshot_dir: str = "snapshot"

    @property
    def raw_dir(self) -> Path:
        return Path(self.data_dir) / "raw"

    @contextmanager
    def connect(self):
        con: duckdb.DuckDBPyConnection = db.connect(Path(self.data_dir) / "abq_qc.duckdb")
        try:
            yield con
        finally:
            con.close()


class FileNotPostedError(Exception):
    pass


class SolradSource(dg.ConfigurableResource):
    base_url: str = "https://gml.noaa.gov/aftp/data/radiation/solrad/abq"

    def url_for(self, day: date) -> str:
        doy = day.timetuple().tm_yday
        return f"{self.base_url}/{day:%Y}/abq{day:%y}{doy:03d}.dat"

    def fetch(self, url: str) -> bytes:
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotPostedError(url) from e
            raise
        except urllib.error.URLError as e:
            # file:// URLs (used in tests) report a missing file this way
            if isinstance(e.reason, FileNotFoundError):
                raise FileNotPostedError(url) from e
            raise
