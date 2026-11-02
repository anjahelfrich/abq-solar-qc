"""Shared by all pages: the snapshot connection (one per process) and the chosen time zone."""

import pandas as pd
import queries as q
import streamlit as st

TIME_ZONES = [
    "America/Denver",
    "UTC",
    "America/Phoenix",
    "America/Los_Angeles",
    "America/Chicago",
    "America/New_York",
]


@st.cache_resource
def _snapshot():
    return q.open_snapshot()


def con():
    return q.cursor(_snapshot())


@st.cache_data
def manifest() -> dict:
    return q.read_manifest()


def tz() -> str:
    return st.session_state.get("tz", TIME_ZONES[0])


def local(ts: pd.Series) -> pd.Series:
    """UTC timestamps as text in the chosen time zone, e.g. '2026-08-01 15:35'."""
    return ts.dt.tz_convert(tz()).dt.strftime("%Y-%m-%d %H:%M")
