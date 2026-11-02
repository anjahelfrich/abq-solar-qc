"""Plotly figures. Colors: categorical slots 1-3 for the sensors, reserved status colors for QC
results (always with their own marker shape and a legend label, never color alone)."""

import pandas as pd
import plotly.graph_objects as go

SENSOR_COLORS = {"ghi": "#2a78d6", "dni": "#eb6834", "dhi": "#1baf7a"}
SENSOR_NAMES = {"ghi": "GHI", "dni": "DNI", "dhi": "DHI"}
# status -> (color, marker symbol, legend label)
STATUS_MARKS = {
    "suspect": ("#fab219", "triangle-up", "Suspect"),
    "bad": ("#d03b3b", "x", "Bad"),
    "accepted": ("#0ca30c", "circle-open", "Flag accepted (override)"),
}
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
WARN = "#fab219"
MUTED = "#898781"
GRID = "#e1e0d9"
BAND = "rgba(137, 135, 129, 0.15)"


def _layout(fig: go.Figure, y_title: str, height: int = 320) -> go.Figure:
    fig.update_layout(
        height=height,
        margin={"l": 10, "r": 10, "t": 30, "b": 10},
        legend={"orientation": "h", "y": 1.02, "yanchor": "bottom", "x": 0},
        plot_bgcolor="#fcfcfb",
        paper_bgcolor="#fcfcfb",
        font={"color": "#52514e"},
        bargap=0.15,
    )
    fig.update_xaxes(showgrid=False, linecolor="#c3c2b7")
    fig.update_yaxes(title=y_title, gridcolor=GRID, zeroline=False)
    return fig


def daily_threshold(
    df: pd.DataFrame,
    col: str,
    threshold: float,
    warn_above: bool,
    name: str,
    y_title: str,
    as_line: bool = False,
) -> go.Figure:
    """One value per day against an asset check threshold. Days the check warns on are
    highlighted and named in the legend."""
    warn = df[col] > threshold if warn_above else df[col] < threshold
    fig = go.Figure()
    hover = "%{x|%b %d}: %{y:.1f}%<extra></extra>"
    if as_line:
        fig.add_scatter(
            x=df["day"],
            y=df[col],
            mode="lines+markers",
            name=name,
            hovertemplate=hover,
            line={"color": SERIES[0], "width": 2},
            marker={"size": 6},
        )
        fig.add_scatter(
            x=df.loc[warn, "day"],
            y=df.loc[warn, col],
            mode="markers",
            name="Check warns",
            hovertemplate=hover,
            marker={
                "color": WARN,
                "size": 13,
                "symbol": "triangle-up",
                "line": {"color": "#fcfcfb", "width": 1},
            },
        )
    else:
        for mask, label, color in [(~warn, name, SERIES[0]), (warn, "Check warns", WARN)]:
            fig.add_bar(
                x=df.loc[mask, "day"],
                y=df.loc[mask, col],
                name=label,
                marker={"color": color, "cornerradius": 4},
                hovertemplate=hover,
            )
    fig.add_hline(
        y=threshold,
        line={"color": MUTED, "width": 1},
        showlegend=True,
        name=f"Check threshold ({threshold:g}%)",
    )
    return _layout(fig, y_title)


def irradiance(m: pd.DataFrame, tz: str) -> go.Figure:
    """GHI/DNI/DHI lines with QC results marked. Breaks in a line are missing values."""
    t = m["ts_utc"].dt.tz_convert(tz)
    fig = go.Figure()
    for s in SENSOR_COLORS:
        fig.add_trace(
            go.Scattergl(
                x=t,
                y=m[s],
                mode="lines",
                name=SENSOR_NAMES[s],
                line={"color": SENSOR_COLORS[s], "width": 2},
                hovertemplate=f"{SENSOR_NAMES[s]} %{{y:.1f}} W/m²<extra></extra>",
            )
        )
    for status, (color, symbol, label) in STATUS_MARKS.items():
        parts = []
        for s in SENSOR_COLORS:
            if status == "accepted":
                mask = (m[f"status_{s}"] == "good") & m[f"overrides_{s}"].notna()
            else:
                mask = m[f"status_{s}"] == status
            if mask.any():
                parts.append(
                    pd.DataFrame(
                        {
                            "t": t[mask],
                            "y": m.loc[mask, s],
                            "sensor": SENSOR_NAMES[s],
                            "rules": m.loc[mask, f"rules_{s}"].fillna("-"),
                            "overrides": m.loc[mask, f"overrides_{s}"].fillna("-"),
                        }
                    )
                )
        if not parts:
            continue
        p = pd.concat(parts)
        fig.add_trace(
            go.Scattergl(
                x=p["t"],
                y=p["y"],
                mode="markers",
                name=label,
                marker={
                    "color": color,
                    "symbol": symbol,
                    "size": 11,
                    "line": {
                        "color": color if symbol.endswith("open") else "#fcfcfb",
                        "width": 1,
                    },
                },
                customdata=p[["sensor", "rules", "overrides"]].to_numpy(),
                hovertemplate=(
                    f"{label}: %{{customdata[0]}} %{{y:.1f}} W/m², "
                    "rules %{customdata[1]}, overrides %{customdata[2]}<extra></extra>"
                ),
            )
        )
    fig.update_layout(hovermode="x unified")
    fig = _layout(fig, "W/m²", height=420)
    fig.update_xaxes(title=f"time ({tz})")
    return fig


def drift(d: pd.DataFrame, warn_pct: float) -> go.Figure:
    """Daily clear-sky closure ratio, its 7-day median, and the ±band around the baseline."""
    baseline = d["baseline"].dropna().iloc[0] if d["baseline"].notna().any() else None
    fig = go.Figure()
    if baseline is not None:
        lo, hi = baseline * (1 - warn_pct / 100), baseline * (1 + warn_pct / 100)
        fig.add_hrect(
            y0=lo,
            y1=hi,
            fillcolor=BAND,
            line_width=0,
            showlegend=True,
            name=f"Baseline ±{warn_pct:g}%",
        )
        fig.add_hline(
            y=baseline,
            line={"color": MUTED, "width": 1},
            showlegend=True,
            name="August baseline",
        )
        values = pd.concat([d["daily_ratio"], d["rolling_median"]]).dropna()
        pad = (hi - lo) * 0.1
        fig.update_yaxes(range=[min(values.min(), lo) - pad, max(values.max(), hi) + pad])
    fig.add_scatter(
        x=d["day"],
        y=d["daily_ratio"],
        mode="markers",
        name="Daily ratio",
        marker={"color": SERIES[0], "size": 7},
        hovertemplate="%{x|%b %d}: daily %{y:.4f}<extra></extra>",
    )
    fig.add_scatter(
        x=d["day"],
        y=d["rolling_median"],
        mode="lines",
        name="7-day median",
        line={"color": SERIES[1], "width": 2},
        hovertemplate="%{x|%b %d}: 7-day median %{y:.4f}<extra></extra>",
    )
    warn = d["warn"].fillna(False).astype(bool)
    if warn.any():
        fig.add_scatter(
            x=d.loc[warn, "day"],
            y=d.loc[warn, "rolling_median"],
            mode="markers",
            name="Outside band",
            marker={"color": WARN, "size": 10, "symbol": "triangle-up"},
        )
    return _layout(fig, "GHI ÷ (DNI·cos Z + DHI)", height=380)


def noaa_agreement(n: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for col, label, color in [
        ("noaa_both", "Both flagged", SERIES[0]),
        ("noaa_ours_only", "Ours only", SERIES[1]),
        ("noaa_theirs_only", "NOAA only", SERIES[2]),
    ]:
        fig.add_bar(
            x=n["day"],
            y=n[col],
            name=label,
            marker={"color": color, "line": {"color": "#fcfcfb", "width": 1}},
            hovertemplate=f"%{{x|%b %d}}: {label} %{{y}}<extra></extra>",
        )
    fig.update_layout(barmode="stack")
    fig.update_layout(legend_traceorder="normal")
    return _layout(fig, "sensor values flagged")
