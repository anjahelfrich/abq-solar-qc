"""ABQ Solar QC dashboard. Run with `make app`. Reads the committed Parquet snapshot only."""

import state
import streamlit as st

st.set_page_config(page_title="ABQ Solar QC", page_icon=":material/wb_sunny:", layout="wide")

page = st.navigation(
    [
        st.Page("views/overview.py", title="Overview", icon=":material/dashboard:", default=True),
        st.Page("views/explore.py", title="Explore", icon=":material/show_chart:"),
        st.Page("views/drift.py", title="Drift", icon=":material/trending_flat:"),
        st.Page("views/audit.py", title="Audit & lineage", icon=":material/fact_check:"),
        st.Page("views/rules.py", title="QC rules", icon=":material/rule:"),
    ]
)

with st.sidebar:
    st.selectbox(
        "Time zone",
        state.TIME_ZONES,
        key="tz",
        help="For charts, tables and downloads. Data is stored in UTC; downloads include both.",
    )
    m = state.manifest()
    st.caption(
        f"NOAA SOLRAD Albuquerque, 1-minute data, {m['first_day']} to {m['newest_day']} (UTC "
        f"days). Snapshot from code version `{m['code_version']}`."
    )

page.run()
