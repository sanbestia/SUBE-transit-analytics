"""
dashboard/utils.py — Pure, testable helper functions for the SUBE dashboard.

All functions here are free of Streamlit dependencies so they can be
unit-tested without a running Streamlit session.

Functions are imported by app.py:
    from dashboard.utils import (
        load_monthly, load_daily_totals, load_modal_split, load_yoy,
        load_heatmap, load_amba_recovery, load_top_empresas, load_by_provincia,
        add_event_annotations, add_fare_annotations,
        mode_color_map, hex_to_rgb, compute_mom_pct,
    )
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DASHBOARD_MODES, EVENTS, FARE_HIKES, MODE_COLORS


# ── DB query functions ─────────────────────────────────────────────────────
# These accept a DuckDB connection and return a DataFrame.
# They are decorated with @st.cache_data in app.py — the decorator cannot
# live here because it would import streamlit at module level.

def load_monthly(conn) -> pd.DataFrame:
    """Monthly ridership totals per mode (COLECTIVO, TREN, SUBTE)."""
    return conn.execute("""
        SELECT * FROM monthly_transactions
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


def load_daily_totals(conn) -> pd.DataFrame:
    """Daily ridership aggregated per (date, mode), suspicious rows excluded."""
    return conn.execute("""
        SELECT fecha, year, month, day_of_week, modo,
               SUM(cantidad_usos) AS cantidad_usos
        FROM daily_transactions
        WHERE NOT is_suspicious
          AND modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        GROUP BY fecha, year, month, day_of_week, modo
        ORDER BY fecha
    """).df()


def load_modal_split(conn) -> pd.DataFrame:
    """Monthly modal split (% share per mode)."""
    return conn.execute("""
        SELECT * FROM v_modal_split
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


def load_yoy(conn) -> pd.DataFrame:
    """Year-over-year % change per mode, monthly."""
    return conn.execute("""
        SELECT * FROM v_yoy_monthly
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


def load_heatmap(conn) -> pd.DataFrame:
    """Average ridership by weekday × calendar month."""
    return conn.execute("SELECT * FROM v_weekday_heatmap").df()


def load_amba_by_mode(conn) -> pd.DataFrame:
    """
    Monthly ridership by AMBA × mode, without a pre-computed recovery index.
    Used when the dashboard needs to filter by mode before aggregating.
    """
    return conn.execute("""
        SELECT month_start, amba, modo, SUM(total_usos) AS total
        FROM monthly_by_provincia
        WHERE amba IN ('SI', 'NO')
          AND modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        GROUP BY month_start, amba, modo
        ORDER BY month_start, amba, modo
    """).df()


def load_amba_recovery(conn) -> pd.DataFrame:
    """
    Monthly ridership totals for AMBA and Interior, with a recovery index
    computed relative to January 2020 (= 100).
    """
    return conn.execute("""
        WITH base AS (
            SELECT amba, SUM(total_usos) AS jan2020
            FROM monthly_by_provincia
            WHERE month_start = '2020-01-01'
            GROUP BY amba
        )
        SELECT p.month_start, p.amba,
               SUM(p.total_usos) AS total,
               ROUND(100.0 * SUM(p.total_usos) / MAX(b.jan2020), 1) AS recovery_index
        FROM monthly_by_provincia p
        JOIN base b ON p.amba = b.amba
        WHERE p.amba IN ('SI', 'NO')
        GROUP BY p.month_start, p.amba
        ORDER BY p.month_start, p.amba
    """).df()


def load_top_empresas(conn) -> pd.DataFrame:
    """Top 10 operators by cumulative ridership (all-time)."""
    return conn.execute("""
        SELECT nombre_empresa, modo, total_usos
        FROM top_empresas
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY total_usos DESC
        LIMIT 10
    """).df()


def load_by_provincia(conn) -> pd.DataFrame:
    """Total trips per province, all time, excluding unknown jurisdictions."""
    return conn.execute("""
        SELECT provincia, SUM(total_usos) AS total
        FROM monthly_by_provincia
        WHERE provincia NOT IN ('JN', 'NAN', 'SN', 'SD')
          AND provincia IS NOT NULL
        GROUP BY provincia
        ORDER BY total DESC
    """).df()


def load_historical_monthly(conn) -> pd.DataFrame:
    """
    Pre-2020 AMBA monthly ridership from monthly_historical.
    Returns empty DataFrame if the table doesn't exist yet
    (i.e. ingest_historical.py hasn't been run).
    """
    try:
        return conn.execute("""
            SELECT month_start, modo, total_usos, amba, era, source
            FROM monthly_historical
            WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
            ORDER BY month_start, modo
        """).df()
    except Exception:
        return pd.DataFrame(
            columns=["month_start", "modo", "total_usos", "amba", "era", "source"]
        )


def load_combined_monthly(conn) -> pd.DataFrame:
    """
    Full monthly ridership series: pre-2020 historical + post-2020 pipeline.

    Unions monthly_historical (2013/2016 → 2019-10, AMBA only) with
    monthly_transactions (2020-01 → present, AMBA + Interior).

    Mode coverage:
        COLECTIVO : 2013-01 → present
        SUBTE     : 2016-01 → present
        TREN      : 2016-01 → present

    Returns DataFrame with columns:
        month_start, modo, total_usos
    (era and source columns are dropped — callers don't need provenance)

    Falls back to monthly_transactions only if monthly_historical doesn't exist.
    """
    try:
        return conn.execute("""
            SELECT month_start, modo, total_usos
            FROM monthly_historical
            WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')

            UNION ALL

            SELECT month_start, modo, total_usos
            FROM monthly_transactions
            WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')

            ORDER BY month_start, modo
        """).df()
    except Exception:
        # monthly_historical doesn't exist — fall back to post-2020 only
        return conn.execute("""
            SELECT month_start, modo, total_usos
            FROM monthly_transactions
            WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
            ORDER BY month_start, modo
        """).df()


# ── Data transform helpers ─────────────────────────────────────────────────

def compute_mom_pct(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'mom_pct' column: month-over-month % change per mode.

    Args:
        df: DataFrame with columns ['modo', 'month_start', 'total_usos'],
            sorted by ['modo', 'month_start'].

    Returns the same DataFrame with 'mom_pct' appended and NaN rows dropped.
    """
    df = df.sort_values(["modo", "month_start"]).copy()
    df["mom_pct"] = df.groupby("modo")["total_usos"].pct_change() * 100
    return df.dropna(subset=["mom_pct"])


def index_to_baseline(
    df: pd.DataFrame,
    baseline_date: str,
    value_col: str = "total_usos",
    group_col: str = "modo",
) -> pd.DataFrame:
    """
    Index values to 100 at a given baseline date, per group.

    Args:
        df           : DataFrame with columns [group_col, 'month_start', value_col]
        baseline_date: ISO date string, e.g. '2020-01-01'
        value_col    : column containing the values to index
        group_col    : column to group by (e.g. 'modo', 'amba')

    Returns the DataFrame with an 'index_val' column appended.
    NaN rows (groups with no baseline observation) are dropped.
    """
    base = (
        df[df["month_start"] == baseline_date]
        .set_index(group_col)[value_col]
    )
    df = df.copy()
    df["index_val"] = df.apply(
        lambda r: (r[value_col] / base[r[group_col]]) * 100
        if r[group_col] in base.index else float("nan"),
        axis=1,
    )
    return df.dropna(subset=["index_val"])


# ── Chart annotation helpers ───────────────────────────────────────────────

def _staggered_annotations(
    fig: go.Figure,
    entries: list[dict],
    line_dash: str = "dot",
    x_min=None,
    x_max=None,
    position: str = "top",
) -> go.Figure:
    """
    Draw non-overlapping vertical lines with staggered labels on a Plotly figure.

    Each entry in `entries` must have keys: ts (pd.Timestamp), label (str),
    hover (str), color (str).

    position="top"    — labels start at 0.98 and step downward  (events)
    position="bottom" — labels start at 0.02 and step upward    (fare hikes)

    Staggering algorithm:
        Each label is assigned a y position in paper coordinates (0–1).
        Starting from BASE_Y, it steps away if it would overlap an already-placed
        label. Two labels clash when they are both close in time (their rendered
        text widths overlap) AND close in y (within one step of each other).
        DAYS_PER_CHAR converts character count to an approximate time-axis width.
    """
    # Approximate width of one character in days on the time axis —
    # used to estimate whether two text labels would overlap horizontally.
    DAYS_PER_CHAR = 11
    # Vertical gap between stagger levels in paper coordinates (0–1).
    LINE_STEP     = 0.05

    if position == "bottom":
        BASE_Y   = 0.02   # fare hike labels start near the bottom
        STEP_DIR = +1     # and step upward to avoid overlap
        yanchor  = "bottom"
    else:
        BASE_Y   = 0.98   # event labels start near the top
        STEP_DIR = -1     # and step downward
        yanchor  = "top"

    # Infer x range from existing traces if not supplied
    if x_min is None or x_max is None:
        all_x = []
        for trace in fig.data:
            xs = getattr(trace, "x", None)
            if xs is not None:
                all_x.extend([v for v in xs if v is not None])
        if all_x:
            x_min = x_min or min(all_x)
            x_max = x_max or max(all_x)

    # Only draw annotations within the visible x range
    if x_min is not None and x_max is not None:
        entries = [
            e for e in entries
            if pd.Timestamp(x_min) <= e["ts"] <= pd.Timestamp(x_max)
        ]

    if not entries:
        return fig

    dates  = [e["ts"]    for e in entries]
    hovers = [e["hover"] for e in entries]
    colors = [e["color"] for e in entries]

    # Track already-placed labels as (timestamp, y_position, pixel_width)
    placed: list[tuple] = []

    def clashes(ts, y, label):
        """Return True if this label would overlap any already-placed label."""
        new_width = len(label) * DAYS_PER_CHAR
        for p_ts, p_y, p_width in placed:
            if (abs((ts - p_ts).days) < (new_width + p_width) / 2
                    and abs(y - p_y) < LINE_STEP * 0.9):
                return True
        return False

    for ev in entries:
        ts = ev["ts"]
        y  = BASE_Y
        # Keep stepping until we find a y level with no overlap
        while clashes(ts, y, ev["label"]):
            y += STEP_DIR * LINE_STEP
        placed.append((ts, y, len(ev["label"]) * DAYS_PER_CHAR))

        fig.add_vline(
            x=ts.timestamp() * 1000,
            line_dash=line_dash,
            line_color=ev["color"],
            opacity=0.45,
        )
        fig.add_annotation(
            x=ts, y=y, yref="paper",
            text=ev["label"],
            showarrow=False,
            font=dict(size=9, color=ev["color"]),
            xanchor="left", yanchor=yanchor,
            bgcolor=None, borderpad=2,
        )

    fig.add_trace(go.Scatter(
        x=dates,
        y=[None] * len(dates),
        mode="markers",
        marker=dict(symbol="line-ns-open", size=14, color=colors,
                    line=dict(width=2, color=colors)),
        text=hovers,
        hovertemplate="%{text}<extra></extra>",
        showlegend=False,
        cliponaxis=True,
    ))

    if x_min is not None and x_max is not None:
        fig.update_xaxes(range=[pd.Timestamp(x_min), pd.Timestamp(x_max)])

    all_y = []
    for trace in fig.data[:-1]:
        ys = getattr(trace, "y", None)
        if ys is not None:
            all_y.extend([v for v in ys if v is not None])

    if all_y and placed:
        data_max = max(all_y)
        data_min = min(v for v in all_y if v is not None)
        if position == "top":
            lowest_label_y = min(y for _, y, _ in placed)
            label_fraction = 1.0 - lowest_label_y + LINE_STEP
            y_top = (data_max / (1.0 - label_fraction)
                     if label_fraction < 1.0 else data_max * 1.25)
            current_range = fig.layout.yaxis.range
            y_bottom = current_range[0] if current_range else min(0, data_min)
            fig.update_yaxes(range=[y_bottom, y_top])
        else:
            highest_label_y = max(y for _, y, _ in placed)
            label_fraction  = highest_label_y + LINE_STEP
            y_bottom = data_min - abs(data_min) * (
                label_fraction / (1.0 - label_fraction + 1e-9))
            current_range = fig.layout.yaxis.range
            y_top = current_range[1] if current_range else data_max * 1.05
            fig.update_yaxes(range=[y_bottom, y_top])

    return fig


def add_event_annotations(
    fig: go.Figure,
    lang: str = "es",
    x_min=None,
    x_max=None,
) -> go.Figure:
    """
    Annotate a Plotly figure with vertical dotted lines for key historical
    events (loaded from config.EVENTS).

    Args:
        fig  : Plotly figure to annotate
        lang : 'es' or 'en' — controls label language
        x_min: if provided, used as the left bound for axis range and filtering
        x_max: if provided, used as the right bound for axis range and filtering
    """
    entries = []
    for ev in EVENTS:
        ts    = pd.Timestamp(ev["date"])
        lbl   = ev.get(f"label_{lang}", ev.get("label_es", ""))
        note  = ev.get("notes", "")
        hover = f"<b>{ts.strftime('%d/%m/%Y')}</b><br>{lbl}"
        if note:
            hover += f"<br><i>{note}</i>"
        entries.append({"ts": ts, "label": lbl, "hover": hover, "color": ev["color"]})
    return _staggered_annotations(fig, entries, line_dash="dot", x_min=x_min, x_max=x_max)


def add_fare_annotations(
    fig: go.Figure,
    lang: str = "es",
    scope_filter: list | None = None,
    x_min=None,
    x_max=None,
) -> go.Figure:
    """
    Annotate a Plotly figure with vertical dashed lines for fare hike events
    (loaded from config.FARE_HIKES).

    Args:
        fig          : Plotly figure to annotate
        lang         : 'es' or 'en'
        scope_filter : if given, only draw hikes whose scope is in this list
        x_min        : if provided, used as the left bound for axis range
        x_max        : if provided, used as the right bound for axis range
    """
    scope_colors = {
        "national":   "#7C3AED",
        "amba":       "#9F67E8",
        "amba_local": "#BFA0E8",
        "interior":   "#C084FC",
    }
    entries = []
    for h in FARE_HIKES:
        if scope_filter and h["scope"] not in scope_filter:
            continue
        ts    = pd.Timestamp(h["date"])
        lbl   = h.get(f"label_{lang}", h.get("label_es", ""))
        mag   = h["magnitude"]
        note  = h.get("notes", "")
        color = scope_colors.get(h["scope"], "#7C3AED")
        mag_str   = f"+{mag}%" if mag > 0 else ("congelamiento" if lang == "es" else "freeze")
        short_lbl = mag_str
        hover = f"<b>{ts.strftime('%d/%m/%Y')}</b> · {mag_str}<br>{lbl}<br><i>Scope: {h['scope']}</i>"
        if note:
            hover += f"<br>{note}"
        entries.append({"ts": ts, "label": short_lbl, "hover": hover, "color": color})
    return _staggered_annotations(fig, entries, line_dash="dash", position="bottom",
                                   x_min=x_min, x_max=x_max)


def mode_color_map() -> dict:
    """Return {mode: hex_color} for all dashboard modes."""
    return {mode: MODE_COLORS[mode] for mode in DASHBOARD_MODES}


def hex_to_rgb(hex_color: str) -> tuple[float, float, float]:
    """Convert '#RRGGBB' to (r, g, b) floats in 0–1 range."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i+2], 16) / 255 for i in (0, 2, 4))
    return r, g, b