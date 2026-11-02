import charts
import queries as q
import state
import streamlit as st

st.title("Overview")
st.caption("Is the data OK? One row per UTC day from the pipeline's daily summary.")

daily = q.daily(state.con())
t = q.overview_tiles(daily)

c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Values available",
    f"{t['value_availability_pct']:.2f}%",
    border=True,
    help="Share of expected sensor values (1440 minutes × GHI, DNI, DHI per day) that arrived "
    "and aren't NOAA's missing-value code.",
)
c2.metric(
    "Days with gaps",
    f"{t['days_with_gaps']} of {t['days']}",
    border=True,
    help="Days with at least one missing sensor value.",
)
c3.metric(
    "Daytime minutes flagged",
    f"{t['flagged_daytime_pct']:.1f}%",
    border=True,
    help="Received daytime minutes (zenith < 90°) with at least one automatic flag, gaps not "
    "counted. Mostly closure flags under fast-moving clouds.",
)
c4.metric("Newest data", t["newest_day"].strftime("%b %d, %Y"), border=True)

st.subheader("Values available per day")
st.plotly_chart(
    charts.daily_threshold(
        daily,
        "value_availability_pct",
        95,
        warn_above=False,
        name="Values available",
        y_title="% of expected values",
        as_line=True,
    )
)
st.caption(
    "NOAA sent all 1440 rows on every day, so the pipeline's `availability` check (rows "
    "received) passes everywhere. Missing values hide inside those rows: this chart counts them."
)

st.subheader("Daytime minutes flagged per day")
st.plotly_chart(
    charts.daily_threshold(
        daily,
        "flagged_daytime_pct",
        10,
        warn_above=True,
        name="Flagged",
        y_title="% of daytime minutes",
    )
)
st.caption(
    "Same threshold as the `daytime_flagged_share` asset check. Days above it are mostly stormy "
    "monsoon days, where clouds make the three sensors disagree for a minute at a time."
)

with st.expander("Table view"):
    st.dataframe(
        daily[
            [
                "day",
                "received_minutes",
                "missing_values",
                "value_availability_pct",
                "daytime_minutes",
                "flagged_daytime_minutes",
                "flagged_daytime_pct",
                "flags_limits",
                "flags_closure",
                "flags_gap",
                "flags_stuck",
            ]
        ],
        hide_index=True,
        column_config={
            "day": st.column_config.DateColumn("day (UTC)"),
            "value_availability_pct": st.column_config.NumberColumn(format="%.2f"),
            "flagged_daytime_pct": st.column_config.NumberColumn(format="%.1f"),
        },
    )
