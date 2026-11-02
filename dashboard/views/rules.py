import charts
import pandas as pd
import queries as q
import state
import streamlit as st

st.title("QC rules")
st.caption("How do the checks work?")

con = state.con()

st.markdown(
    """
Flags mark data as questionable; nothing is deleted. Each value's **final status** is the
worst of its automatic flags, unless a manual override applies:

- `accept`: a reviewed false alarm, so the final status is **good** (the automatic flag stays).
- `flag_bad`: data known to be bad, e.g. during maintenance, with or without an automatic flag.

A rule's thresholds are fixed per version, so `(rule_id, rule_version)` on a flag always
points to one set of thresholds. The pipeline refuses to run if a rule changed but kept its
version.
"""
)
st.dataframe(
    q.rules(con),
    hide_index=True,
    column_config={
        "description": st.column_config.TextColumn(width="large"),
        "thresholds": st.column_config.JsonColumn(width="medium"),
    },
)

st.subheader("Comparison with NOAA's flags")
n = q.noaa_comparison(con)
c1, c2, c3 = st.columns(3)
c1.metric("Both flagged", f"{n['noaa_both'].sum():,}", border=True)
c2.metric("Ours only", f"{n['noaa_ours_only'].sum():,}", border=True)
c3.metric("NOAA only", f"{n['noaa_theirs_only'].sum():,}", border=True)
st.plotly_chart(charts.noaa_agreement(n))
st.caption(
    "Per sensor value (GHI, DNI, DHI × minute), received rows only, automatic flags without "
    "overrides. Our closure test flags far more than NOAA does: it marks all three sensors "
    "whenever they disagree by more than 8%, which happens often under moving clouds."
)

st.subheader("Not included and why")
NOT_INCLUDED = [
    (
        "Calibration records",
        "Not publicly available for this station, and we're not using "
        "dummy data. Drift is tracked from the measurements instead",
    ),
    ("Backup-sensor comparison", "The station has one sensor per measurement"),
    ("Temperature and wind QC", "The station doesn't measure them"),
    (
        "pvlib clear-sky model",
        "Time. A simple clear-sky filter (low diffuse fraction) is enough for the drift check",
    ),
    ("UVB QC", "Not central to solar resource data. Stored but not checked"),
    (
        "PostgreSQL / TimescaleDB",
        "DuckDB keeps hosting free and simple. See docs/database_choice.md",
    ),
    (
        "Scheduled runs in the hosted demo",
        "Streamlit Cloud only hosts the app. The schedule runs locally",
    ),
    ("Editing flags in the dashboard, user logins", "Overrides go through git instead"),
]
st.table(pd.DataFrame(NOT_INCLUDED, columns=["Left out", "Why"]).set_index("Left out"))
