from datetime import timedelta

import charts
import queries as q
import state
import streamlit as st

MAX_DAYS = 7

st.title("Explore")
st.caption("What happened on this day? Stored 1-minute values with their QC results.")

con, tz = state.con(), state.tz()
first_local, last_local = q.coverage(con, tz)

picked = st.date_input(
    f"Days ({tz}, up to {MAX_DAYS})",
    value=(last_local, last_local),
    min_value=first_local,
    max_value=last_local,
    width=320,
)
if len(picked) < 2:
    st.info("Pick an end date.")
    st.stop()
start, end = picked
if (end - start).days >= MAX_DAYS:
    end = start + timedelta(days=MAX_DAYS - 1)
    st.warning(f"Showing the first {MAX_DAYS} days: {start} to {end}.")

start_utc, end_utc = q.local_day_bounds(start, end, tz)
m = q.minutes(con, start_utc, end_utc)
flags = q.flags(con, start_utc, end_utc)
ovr = q.overrides(con, start_utc, end_utc)

values = len(m) * len(q.SENSORS)
status = m[[f"status_{s}" for s in q.SENSORS]]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Minutes", f"{len(m):,}", border=True)
c2.metric("Suspect values", f"{(status == 'suspect').sum().sum():,}", border=True)
c3.metric("Bad values", f"{(status == 'bad').sum().sum():,}", border=True)
c4.metric("Missing values", f"{(status == 'missing').sum().sum():,}", border=True)

st.plotly_chart(charts.irradiance(m, tz))
st.caption(
    "Final status after overrides, per sensor value. A closure flag marks all three sensors, "
    "because the test can't tell which one is off. Breaks in a line are missing values. "
    "Small negative values at night are normal (the sensors cool by radiating to the sky)."
)

st.download_button(
    "Download CSV",
    q.export_csv(m, tz),
    file_name=f"abq_{start}_{end}.csv",
    mime="text/csv",
    icon=":material/download:",
    help=f"One row per minute: time in {tz} and UTC, values, final status, rules and overrides "
    "per sensor, NOAA flags, and lineage (source file, hash, line, run, code version).",
)

flags_tab, ovr_tab, data_tab = st.tabs(
    [f"Flags ({len(flags):,})", f"Overrides ({len(ovr)})", "Data"]
)
with flags_tab:
    st.caption("Automatic flags, as the rules wrote them. `all` = the whole minute.")
    flags.insert(0, f"time ({tz})", state.local(flags["ts_utc"]))
    st.dataframe(flags.drop(columns="ts_utc"), hide_index=True)
with ovr_tab:
    st.caption("Manual decisions from `overrides/manual_overrides.csv`, reviewed in git.")
    for col in ["start_utc", "end_utc"]:
        ovr[col] = state.local(ovr[col])
    st.dataframe(
        ovr.rename(columns={"start_utc": f"start ({tz})", "end_utc": f"end ({tz})"}),
        hide_index=True,
    )
with data_tab:
    table = m.copy()
    table.insert(0, f"time ({tz})", state.local(table["ts_utc"]))
    st.dataframe(table.drop(columns="ts_utc"), hide_index=True)
