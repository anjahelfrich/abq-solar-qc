import json

import charts
import queries as q
import state
import streamlit as st

st.title("Drift")
st.caption("Are the sensors and tracker holding calibration?")

con = state.con()
d = q.drift(con)
rules = q.rules(con)
th = json.loads(rules.loc[rules["rule_id"] == "drift", "thresholds"].iloc[-1])
warn_pct = th["warn_pct"]

judged = d.dropna(subset=["rolling_median"])
latest = judged.iloc[-1] if not judged.empty else None
outside = d[d["warn"].fillna(False).astype(bool)]

c1, c2, c3 = st.columns(3)
c1.metric("August baseline", f"{d['baseline'].dropna().iloc[0]:.4f}", border=True)
if latest is not None:
    c2.metric(
        f"7-day median, {latest['day']:%b %d}",
        f"{latest['rolling_median']:.4f}",
        delta=f"{latest['deviation_pct']:+.2f}% vs baseline",
        delta_color="off",
        border=True,
    )
c3.metric(f"Days outside ±{warn_pct:g}%", len(outside), border=True)

st.plotly_chart(charts.drift(d, warn_pct))

st.markdown(
    f"""
**How to read it.** GHI should equal DNI·cos(zenith) + DHI: global light is direct sun on a
flat surface plus diffuse sky light. The station measures all three, so their ratio is a
built-in cross-check. Each dot is one day's median ratio over clear-sky minutes (zenith
< {th["zenith_max"]:g}°, DHI/GHI < {th["diffuse_fraction_max"]:g}), where the sensors agree
best. Calibration drift moves the 7-day median out of the gray band.

- **Ratio creeping above 1:** DNI or DHI reads low, e.g. the tracker slipping off the sun or a
  dirty pyrheliometer window.
- **Ratio creeping below 1:** GHI reads low, or the diffuse sensor's shade lets direct sun in.

The Dagster check `drift_within_band` judges the newest day with a 7-day median and lists past
days outside the band in its metadata.
"""
)

with st.expander("Table view"):
    st.dataframe(
        d.drop(columns="run_id"),
        hide_index=True,
        column_config={"day": st.column_config.DateColumn("day (UTC)")},
    )
