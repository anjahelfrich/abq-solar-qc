from datetime import datetime, time

import pandas as pd
import queries as q
import state
import streamlit as st

st.title("Audit & lineage")
st.caption("Where did this value come from, and has anything been changed since?")

con, tz = state.con(), state.tz()

st.subheader("Snapshot integrity")
problems = q.verify_snapshot(con)
m = state.manifest()
if problems:
    listed = "\n".join(f"- {p}" for p in problems)
    st.error(f"Snapshot does **not** match its manifest:\n\n{listed}")
else:
    st.success(
        f"Verified: all {len(m['tables'])} Parquet files match the SHA-256 hashes in "
        f"`manifest.json`, all {m['audit_last_event_id']} audit events recompute their hash "
        "chain, and the newest event matches the one the manifest recorded.",
        icon=":material/verified:",
    )
st.caption(
    "Recomputed on every page load. The manifest is committed to git, so its history is the "
    "record of what each snapshot contained."
)

st.subheader("Trace a minute")
first, last = q.coverage(con, tz)
c1, c2 = st.columns(2)
day = c1.date_input(f"Date ({tz})", value=last, min_value=first, max_value=last)
at = c2.time_input(f"Time ({tz})", value=time(12, 0), step=60)
ts_utc = pd.Timestamp(datetime.combine(day, at)).tz_localize(tz).tz_convert("UTC")
found = q.lookup(con, ts_utc.to_pydatetime())

if found is None:
    st.info(f"No stored row for {ts_utc:%Y-%m-%d %H:%M} UTC.")
else:
    r = found["row"]
    st.markdown(
        f"Minute ending **{ts_utc.tz_convert(tz):%Y-%m-%d %H:%M} {tz}** "
        f"({ts_utc:%Y-%m-%d %H:%M} UTC), zenith {r['zenith_deg']:.2f}°"
    )
    status = found["status"].set_index("variable")
    values = pd.DataFrame(
        [
            {
                "sensor": s.upper(),
                "W/m²": r[s],
                "NOAA flag": r[f"noaa_qc_{s}"],
                "our rules": ", ".join(status.loc[s, "auto_rules"]) if s in status.index else "",
                "overrides": ", ".join(status.loc[s, "override_ids"] or [])
                if s in status.index
                else "",
                "final status": status.loc[s, "final_status"] if s in status.index else "good",
            }
            for s in q.SENSORS
        ]
    )
    st.dataframe(values, hide_index=True)

    commit = r["code_version"]
    file_name = r["url"].rsplit("/", 1)[-1]
    st.markdown(
        f"""
| Lineage | |
| --- | --- |
| Source file | [{file_name}]({r["url"]}) (NOAA, UTC day {r["file_date"]:%Y-%m-%d}) |
| Line in file | {r["source_line"]} |
| File SHA-256 | `{r["source_sha256"]}` |
| File size | {r["size_bytes"]:,} bytes |
| Downloaded | {pd.Timestamp(r["downloaded_at"]).tz_convert(tz):%Y-%m-%d %H:%M:%S} {tz} |
| Download run | `{r["download_run_id"]}` |
| Parse run | `{r["run_id"]}` |
| Code version | [`{commit}`]({q.REPO_URL}/commit/{commit}) |
"""
    )
    st.caption(
        "NOAA may revise a file after we download it. The pipeline then keeps both versions "
        "and logs a `file_changed` event, so the hash above always names the exact bytes used."
    )

st.subheader("Audit log")
log = q.audit_log(con)
types = st.multiselect("Event types", sorted(log["event_type"].unique()), placeholder="All")
if types:
    log = log[log["event_type"].isin(types)]
log.insert(1, f"time ({tz})", state.local(log["event_time"]))
st.dataframe(
    log.drop(columns="event_time"),
    hide_index=True,
    column_config={"details": st.column_config.JsonColumn(width="large")},
)
st.caption(
    "Rows are only ever added. Each event stores the hash of its own content plus the previous "
    "event's hash, so editing or deleting an old row breaks the chain."
)
