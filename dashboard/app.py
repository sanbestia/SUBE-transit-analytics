"""
dashboard/app.py — SUBE Transit Analytics Dashboard

Run with:
    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DASHBOARD_MODES, DB_PATH, EVENTS, FARE_HIKES, MODE_COLORS, TRANSPORT_MODES
from etl.load import get_connection

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SUBE Transit Analytics (ALPHA - IN DEVELOPMENT)",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Translations ───────────────────────────────────────────────────────────
from dashboard.strings import STRINGS, MODE_LABELS

# ── Session state ──────────────────────────────────────────────────────────
if "lang" not in st.session_state:
    st.session_state.lang = "es"


def t(key: str) -> str:
    return STRINGS[st.session_state.lang].get(key, key)


def mode_label(mode: str) -> str:
    return MODE_LABELS[st.session_state.lang].get(mode, mode)


def event_label(ev: dict) -> str:
    return ev[f"label_{st.session_state.lang}"]


# ── DB helpers ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error("Database not found. Run: python run_pipeline.py")
        st.stop()
    return get_connection()


@st.cache_data(ttl=3600)
def load_monthly() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT * FROM monthly_transactions
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


@st.cache_data(ttl=3600)
def load_daily_totals() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT fecha, year, month, day_of_week, modo,
               SUM(cantidad_usos) AS cantidad_usos
        FROM daily_transactions
        WHERE NOT is_suspicious
          AND modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        GROUP BY fecha, year, month, day_of_week, modo
        ORDER BY fecha
    """).df()


@st.cache_data(ttl=3600)
def load_modal_split() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT * FROM v_modal_split
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


@st.cache_data(ttl=3600)
def load_yoy() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT * FROM v_yoy_monthly
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


@st.cache_data(ttl=3600)
def load_heatmap() -> pd.DataFrame:
    return get_conn().execute("SELECT * FROM v_weekday_heatmap").df()


@st.cache_data(ttl=3600)
def load_amba_recovery() -> pd.DataFrame:
    return get_conn().execute("""
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


@st.cache_data(ttl=3600)
def load_top_empresas() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT nombre_empresa, modo, total_usos
        FROM top_empresas
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY total_usos DESC
        LIMIT 10
    """).df()


@st.cache_data(ttl=3600)
def load_by_provincia() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT provincia, SUM(total_usos) AS total
        FROM monthly_by_provincia
        WHERE provincia NOT IN ('JN', 'NAN', 'SN', 'SD')
          AND provincia IS NOT NULL
        GROUP BY provincia
        ORDER BY total DESC
    """).df()


# ── Chart helpers ──────────────────────────────────────────────────────────

def _staggered_annotations(fig, entries: list[dict], line_dash: str = "dot",
                            x_min=None, x_max=None,
                            position: str = "top") -> go.Figure:
    """
    Draw vertical lines with horizontal labels that never overlap each other.

    position="top"    — labels start at 0.98 and step downward  (events)
    position="bottom" — labels start at 0.02 and step upward    (fare hikes)

    The y-axis is expanded in the corresponding direction so labels never
    overlap the data.
    """
    # Estimate label width in days based on character count.
    # At font-size 9, each character is ~5.5px wide. A typical 5-year chart
    # at ~900px wide spans ~1825 days, so px-per-day ≈ 900/1825 ≈ 0.49.
    # days_per_char ≈ 5.5 / 0.49 ≈ 11 days per character.
    DAYS_PER_CHAR = 11
    LINE_STEP     = 0.05

    if position == "bottom":
        BASE_Y    = 0.02   # start near the bottom
        STEP_DIR  = +1     # step upward (increasing paper-y)
        yanchor   = "bottom"
    else:
        BASE_Y    = 0.98   # start near the top
        STEP_DIR  = -1     # step downward (decreasing paper-y)
        yanchor   = "top"

    # Infer x_min / x_max from existing data traces if not supplied
    if x_min is None or x_max is None:
        all_x = []
        for trace in fig.data:
            xs = getattr(trace, "x", None)
            if xs is not None:
                all_x.extend([v for v in xs if v is not None])
        if all_x:
            x_min = x_min or min(all_x)
            x_max = x_max or max(all_x)

    # Filter entries to only those within the chart's x range
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

    # placed: list of (ts, y_paper, label_width_days) for every committed label
    placed: list[tuple] = []

    def clashes(ts, y, label):
        """True if a label at (ts, y) overlaps any already-placed label."""
        new_width = len(label) * DAYS_PER_CHAR
        for p_ts, p_y, p_width in placed:
            x_overlap = abs((ts - p_ts).days) < (new_width + p_width) / 2
            y_overlap  = abs(y - p_y) < LINE_STEP * 0.9
            if x_overlap and y_overlap:
                return True
        return False

    for i, ev in enumerate(entries):
        ts = ev["ts"]

        y = BASE_Y
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
            x=ts,
            y=y,
            yref="paper",
            text=ev["label"],
            showarrow=False,
            font=dict(size=9, color=ev["color"]),
            xanchor="left",
            yanchor=yanchor,
            bgcolor=None,
            borderpad=2,
        )

    # Invisible scatter for rich hover — constrained to the chart's x range
    # xaxis="x" with no actual y keeps it off the data plane;
    # cliponaxis=True prevents it from extending the auto-range
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

    # Push the y-axis top up so labels never overlap the data.
    # Collect all y-values from data traces (skip the annotation scatter
    # which has y=None) and set yaxis range max to 110% of data max,
    # leaving the top 20% of paper space for labels.
    all_y = []
    for trace in fig.data[:-1]:
        ys = getattr(trace, "y", None)
        if ys is not None:
            all_y.extend([v for v in ys if v is not None])

    if all_y and placed:
        data_max = max(all_y)
        data_min = min(v for v in all_y if v is not None)

        if position == "top":
            # Push y-axis ceiling up to give labels headroom above data
            lowest_label_y = min(y for _, y, _ in placed)
            label_fraction = 1.0 - lowest_label_y + LINE_STEP
            y_top = data_max / (1.0 - label_fraction) if label_fraction < 1.0 else data_max * 1.25
            current_range = fig.layout.yaxis.range
            y_bottom = current_range[0] if current_range else min(0, data_min)
            fig.update_yaxes(range=[y_bottom, y_top])
        else:
            # Push y-axis floor down to give labels headroom below data
            highest_label_y = max(y for _, y, _ in placed)
            label_fraction = highest_label_y + LINE_STEP
            y_bottom = data_min - abs(data_min) * (label_fraction / (1.0 - label_fraction + 1e-9))
            current_range = fig.layout.yaxis.range
            y_top = current_range[1] if current_range else data_max * 1.05
            fig.update_yaxes(range=[y_bottom, y_top])

    return fig


def add_event_annotations(fig, y_ref: float = 0):
    """
    Draw vertical dotted lines for key historical events (EVENTS).
    Labels are staggered vertically to prevent overlap.
    Hover tooltip shows date, label, and notes.
    """
    lang    = st.session_state.lang
    entries = []

    for ev in EVENTS:
        ts   = pd.Timestamp(ev["date"])
        lbl  = ev.get(f"label_{lang}", ev.get("label_es", ""))
        note = ev.get("notes", "")
        hover = f"<b>{ts.strftime('%d/%m/%Y')}</b><br>{lbl}"
        if note:
            hover += f"<br><i>{note}</i>"
        entries.append({"ts": ts, "label": lbl, "hover": hover, "color": ev["color"]})

    return _staggered_annotations(fig, entries, line_dash="dot")


def add_fare_annotations(fig, y_ref: float = 0, scope_filter: list | None = None):
    """
    Draw vertical dashed lines for fare hike events (FARE_HIKES).
    Labels are staggered vertically to prevent overlap.
    Hover tooltip shows date, scope, magnitude, and notes.
    scope_filter: if given, only draw hikes whose scope is in the list.
    """
    lang = st.session_state.lang
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
        scope = h["scope"]
        mag   = h["magnitude"]
        note  = h.get("notes", "")
        color = scope_colors.get(scope, "#7C3AED")

        mag_str = f"+{mag}%" if mag > 0 else ("congelamiento" if lang == "es" else "freeze")
        # Short label for the staggered tag: just the magnitude
        short_lbl = mag_str
        hover = f"<b>{ts.strftime('%d/%m/%Y')}</b> · {mag_str}<br>{lbl}<br><i>Scope: {scope}</i>"
        if note:
            hover += f"<br>{note}"

        entries.append({"ts": ts, "label": short_lbl, "hover": hover, "color": color})

    return _staggered_annotations(fig, entries, line_dash="dash", position="bottom")


def mode_color_map() -> dict:
    return {mode: MODE_COLORS[mode] for mode in DASHBOARD_MODES}


def explainer(key: str) -> None:
    """Render a collapsible explainer box for any chart."""
    with st.expander("ℹ️ " + ("¿Cómo leer este gráfico?" if st.session_state.lang == "es" else "How to read this chart?")):
        st.markdown(t(key))


def finding(key: str) -> None:
    """Render a permanently-visible finding callout (not collapsible)."""
    st.info(t(key), icon="💡")


# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    col_es, col_en = st.columns(2)
    with col_es:
        if st.button("🇦🇷 Español", type="primary" if st.session_state.lang == "es" else "secondary"):
            st.session_state.lang = "es"
            st.rerun()
    with col_en:
        if st.button("🇬🇧 English", type="primary" if st.session_state.lang == "en" else "secondary"):
            st.session_state.lang = "en"
            st.rerun()

    st.image("dashboard/assets/sube_logo.png", width=140)
    st.title(t("sidebar_title"))
    st.caption(t("sidebar_source"))
    st.divider()

    daily = load_daily_totals()
    min_date = daily["fecha"].min()
    max_date = daily["fecha"].max()

    date_range = st.date_input(
        t("periodo"),
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    selected_modes = st.multiselect(
        t("modos"),
        options=DASHBOARD_MODES,
        default=DASHBOARD_MODES,
        format_func=mode_label,
    )

    show_events = st.toggle(t("show_events"), value=True)

    st.divider()
    if st.button(t("refresh"), width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"{t('data_until')}: **{max_date.strftime('%d/%m/%Y')}**")


# ── Date / mode filtering ──────────────────────────────────────────────────
if len(date_range) == 2:
    start_date = pd.Timestamp(date_range[0])
    end_date   = pd.Timestamp(date_range[1])
else:
    start_date = daily["fecha"].min()
    end_date   = daily["fecha"].max()

if not selected_modes:
    st.warning("Seleccioná al menos un modo." if st.session_state.lang == "es" else "Please select at least one mode.")
    st.stop()

df_daily = daily[
    (daily["fecha"] >= start_date) &
    (daily["fecha"] <= end_date) &
    (daily["modo"].isin(selected_modes))
]

monthly = load_monthly()
df_monthly = monthly[
    (monthly["month_start"] >= start_date) &
    (monthly["month_start"] <= end_date) &
    (monthly["modo"].isin(selected_modes))
]

cmap = mode_color_map()


# ── KPI cards ──────────────────────────────────────────────────────────────
st.title(t("page_title"))

total_by_day = df_daily.groupby("fecha")["cantidad_usos"].sum()

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric(t("kpi_total"), f"{df_daily['cantidad_usos'].sum()/1e9:.2f}B")
with c2:
    if not total_by_day.empty:
        peak_day = total_by_day.idxmax()
        st.metric(t("kpi_peak"), peak_day.strftime("%d/%m/%Y"),
                  f"{total_by_day.max()/1e6:.1f}M {t('kpi_trips')}")
with c3:
    if not total_by_day.empty:
        st.metric(t("kpi_avg"), f"{total_by_day.mean()/1e6:.2f}M {t('kpi_trips')}")
with c4:
    if not df_daily.empty:
        top_mode = df_daily.groupby("modo")["cantidad_usos"].sum().idxmax()
        st.metric(t("kpi_top_mode"), mode_label(top_mode))

st.caption(t("kpi_explainer"))

# ── Key findings row ───────────────────────────────────────────────────────
# Compute modal share for the most recent month in the full dataset
_all_monthly = load_monthly()
_last_month  = _all_monthly["month_start"].max()
_last_month_df = _all_monthly[_all_monthly["month_start"] == _last_month]
_last_total    = _last_month_df["total_usos"].sum()
_mode_share    = (
    _last_month_df.set_index("modo")["total_usos"] / _last_total * 100
    if _last_total > 0 else {}
)
_month_name   = _last_month.strftime("%b %Y").capitalize()
_share_sub_es = f"proporción de viajes · {_month_name}"
_share_sub_en = f"share of rides · {_month_name}"
_share_sub    = _share_sub_es if st.session_state.lang == "es" else _share_sub_en

f1, f2, f3, f4, f5 = st.columns(5)
with f1:
    _val = f"{_mode_share.get('COLECTIVO', 0):.1f}%"
    st.metric(t("finding_bus_drop"), _val, _share_sub)
with f2:
    _val = f"{_mode_share.get('TREN', 0):.1f}%"
    st.metric(t("finding_tren_drop"), _val, _share_sub)
with f3:
    _val = f"{_mode_share.get('SUBTE', 0):.1f}%"
    st.metric(t("finding_subte_drop"), _val, _share_sub)
with f4:
    # Compute cumulative compounded fare increase over the past 12 months
    # Window: first day of (current month - 12) → last day of previous month
    import datetime as _dt
    _today      = _dt.date.today()
    _win_end    = (_dt.date(_today.year, _today.month, 1) - _dt.timedelta(days=1))
    _win_start  = _dt.date(
        _win_end.year - 1 if _win_end.month < 12 else _win_end.year,
        (_win_end.month % 12) + 1, 1
    )
    _amba_scopes = {"national", "amba", "amba_local"}
    _compound = 1.0
    _n_hikes  = 0
    for _h in FARE_HIKES:
        _hdate = _dt.date.fromisoformat(_h["date"])
        if (_win_start <= _hdate <= _win_end
                and _h["scope"] in _amba_scopes
                and _h["magnitude"] > 0):
            _compound *= (1 + _h["magnitude"] / 100)
            _n_hikes += 1
    _cumulative = (_compound - 1) * 100
    _fare_sub   = (
        f"{_n_hikes} aumentos en los últimos 12 meses"
        if st.session_state.lang == "es"
        else f"{_n_hikes} hikes in the last 12 months"
    )
    st.metric(t("finding_amba_shock"), f"+{_cumulative:.0f}%", _fare_sub)
with f5:
    st.metric(t("finding_seasonal"), "Mar · Ago",
              t("finding_seasonal_sub"))

st.divider()


# ── Tabs ───────────────────────────────────────────────────────────────────
tab_ov, tab_cv, tab_ms, tab_rs, tab_an, tab_fc = st.tabs([
    t("tab_overview"), t("tab_covid"), t("tab_modal"),
    t("tab_resilience"), t("tab_analysis"), t("tab_forecast"),
])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — OVERVIEW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_ov:

    st.subheader(t("ov_series_title"))
    explainer("ov_series_explainer")

    fig = go.Figure()
    for mode in selected_modes:
        mode_df = df_daily[df_daily["modo"] == mode].sort_values("fecha")
        ma7 = mode_df["cantidad_usos"].rolling(7, min_periods=1).mean()
        fig.add_scatter(
            x=mode_df["fecha"], y=mode_df["cantidad_usos"],
            mode="lines", name=f"{mode_label(mode)} (raw)",
            line=dict(color=cmap[mode], width=1),
            opacity=0.2, showlegend=False,
        )
        fig.add_scatter(
            x=mode_df["fecha"], y=ma7,
            mode="lines", name=mode_label(mode),
            line=dict(color=cmap[mode], width=2.5),
        )

    if show_events:
        fig = add_event_annotations(fig)

    fig.update_layout(
        height=675, template="plotly_white",
        yaxis_title=t("ov_series_y"),
        legend_title=None, hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch")

    st.subheader(t("ov_split_title"))
    explainer("ov_split_explainer")

    split = load_modal_split()
    split = split[
        (split["month_start"] >= start_date) &
        (split["month_start"] <= end_date) &
        (split["modo"].isin(selected_modes))
    ].copy()
    split["modo_label"] = split["modo"].map(mode_label)

    fig2 = px.area(
        split, x="month_start", y="mode_share_pct",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        labels={"mode_share_pct": t("ov_split_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    if show_events:
        fig2 = add_event_annotations(fig2)
    fig2.update_layout(height=368, yaxis_ticksuffix="%", hovermode="x unified",
                       yaxis=dict(range=[0, 125]))
    st.plotly_chart(fig2, width="stretch")

    # Province histogram (moved from Resilience tab)
    _prov_title = ("Viajes totales por provincia (2020–presente)" if st.session_state.lang == "es"
                   else "Total trips by province (2020–present)")
    st.subheader(_prov_title)
    with st.expander("ℹ️ " + ("¿Cómo leer este gráfico?" if st.session_state.lang == "es" else "How to read this chart?")):
        st.markdown(t("rs_prov_explainer"))

    prov_df = load_by_provincia()
    fig_prov = px.bar(
        prov_df.sort_values("total"),
        x="total", y="provincia",
        orientation="h",
        labels={"total": t("ov_empresas_y"), "provincia": "Provincia"},
        template="plotly_white",
        color="total",
        color_continuous_scale="Blues",
    )
    fig_prov.update_layout(height=max(390, len(prov_df) * 22), coloraxis_showscale=False)
    st.plotly_chart(fig_prov, width="stretch")
    st.caption(t("rs_prov_caption"))

    # Heatmap (moved from Analysis tab)
    st.subheader(t("an_heatmap_title"))
    explainer("an_heatmap_explainer")

    heatmap_df = load_heatmap()
    pivot = heatmap_df.pivot(index="day_of_week", columns="month", values="avg_usos")
    pivot.index   = STRINGS[st.session_state.lang]["days"]
    pivot.columns = STRINGS[st.session_state.lang]["months"]

    fig_heat = px.imshow(
        pivot,
        color_continuous_scale="Blues",
        labels={"color": t("an_heatmap_color")},
        template="plotly_white",
        aspect="auto",
    )
    fig_heat.update_layout(height=320)
    st.plotly_chart(fig_heat, width="stretch")

    st.subheader(t("ov_empresas_title"))
    explainer("ov_empresas_explainer")

    empresas = load_top_empresas()
    empresas = empresas[empresas["modo"].isin(selected_modes)].copy()
    empresas["modo_label"]    = empresas["modo"].map(mode_label)
    empresas["empresa_short"] = empresas["nombre_empresa"].str[:35]

    fig3 = px.bar(
        empresas.sort_values("total_usos"),
        x="total_usos", y="empresa_short",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        orientation="h",
        labels={"total_usos": t("ov_empresas_y"), "empresa_short": t("ov_empresas_x"), "modo_label": ""},
        template="plotly_white",
    )
    fig3.update_layout(height=420)
    st.plotly_chart(fig3, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — COVID-19
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_cv:

    finding("cv_finding")

    # Load covid data for all charts in this tab
    covid_monthly = monthly[
        (monthly["month_start"] >= "2020-01-01") &
        (monthly["month_start"] <= "2022-07-01") &
        (monthly["modo"].isin(selected_modes))
    ].copy()
    covid_monthly["modo_label"] = covid_monthly["modo"].map(mode_label)

    st.subheader(t("cv_collapse_title"))
    explainer("cv_collapse_explainer")

    _norm_base = covid_monthly[covid_monthly["month_start"] == "2020-01-01"].set_index("modo")["total_usos"]
    _norm_df   = covid_monthly.copy()
    _norm_df["index_val"] = _norm_df.apply(
        lambda r: (r["total_usos"] / _norm_base[r["modo"]]) * 100
        if r["modo"] in _norm_base.index else float("nan"), axis=1,
    )
    _norm_df = _norm_df.dropna(subset=["index_val"])

    if not _norm_df.empty:
        _idx_label = "Índice (ene 2020 = 100)" if st.session_state.lang == "es" else "Index (Jan 2020 = 100)"
        fig_norm = px.line(
            _norm_df, x="month_start", y="index_val",
            color="modo_label",
            color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
            markers=True,
            labels={"index_val": _idx_label, "month_start": "", "modo_label": ""},
            template="plotly_white",
        )
        fig_norm.add_hline(y=100, line_dash="dash", line_color="grey", opacity=0.4,
                           annotation_text="Ene 2020 = 100" if st.session_state.lang == "es" else "Jan 2020 = 100")
        fig_norm = add_event_annotations(fig_norm)
        # Annotate each mode's April 2020 drop % directly on the chart
        _drop_map = {
            "COLECTIVO": ("−58%", "Bus −58%"),
            "TREN":      ("−87%", "Train −87%"),
            "SUBTE":     ("−92%", "Subway −92%"),
        }
        _apr2020 = pd.Timestamp("2020-04-01")
        for _mode in selected_modes:
            _apr_row = _norm_df[(_norm_df["modo"] == _mode) &
                                (_norm_df["month_start"] == _apr2020)]
            if not _apr_row.empty:
                _lbl_es, _lbl_en = _drop_map.get(_mode, ("", ""))
                _lbl = _lbl_es if st.session_state.lang == "es" else _lbl_en
                fig_norm.add_annotation(
                    x=_apr2020, y=float(_apr_row["index_val"].iloc[0]),
                    text=f"<b>{_lbl}</b>",
                    showarrow=True, arrowhead=2, arrowsize=1, arrowwidth=1.5,
                    arrowcolor=MODE_COLORS[_mode], ax=0, ay=-36,
                    font=dict(size=11, color=MODE_COLORS[_mode]),
                    bgcolor="white", borderpad=3,
                )
        fig_norm.update_layout(height=675, hovermode="x unified")
        st.plotly_chart(fig_norm, width="stretch")

    st.divider()

    # 4e — Modal recovery chart: MoM % for the 2021–2022 recovery window
    st.subheader(t("cv_subst_recovery_title"))
    explainer("cv_subst_recovery_explainer")

    _rec_modes = [m for m in selected_modes if m in ("COLECTIVO", "SUBTE", "TREN")]
    recovery_monthly = monthly[
        (monthly["month_start"] >= "2020-11-01") &
        (monthly["month_start"] <= "2022-07-01") &
        (monthly["modo"].isin(_rec_modes))
    ].copy().sort_values(["modo", "month_start"])
    recovery_monthly["modo_label"] = recovery_monthly["modo"].map(mode_label)

    # Index to 100 at Nov 2020 — fixed baseline so both modes start at the same point
    _rec_base = (recovery_monthly[recovery_monthly["month_start"] == "2020-11-01"]
                 .set_index("modo")["total_usos"])
    recovery_monthly["index_val"] = recovery_monthly.apply(
        lambda r: (r["total_usos"] / _rec_base[r["modo"]]) * 100
        if r["modo"] in _rec_base.index else float("nan"), axis=1,
    )
    recovery_monthly = recovery_monthly.dropna(subset=["index_val"])

    if not recovery_monthly.empty:
        _rec_label = ("Índice (nov 2020 = 100)" if st.session_state.lang == "es"
                      else "Index (Nov 2020 = 100)")
        fig_rec = px.line(
            recovery_monthly, x="month_start", y="index_val",
            color="modo_label",
            color_discrete_map={mode_label(m): cmap[m] for m in _rec_modes if m in cmap},
            markers=True,
            labels={"index_val": _rec_label, "month_start": "", "modo_label": ""},
            template="plotly_white",
        )
        fig_rec.add_hline(y=100, line_dash="dash", line_color="grey", opacity=0.4,
                          annotation_text="Nov 2020 = 100")
        fig_rec = add_event_annotations(fig_rec)
        fig_rec.update_layout(height=570, hovermode="x unified")
        st.plotly_chart(fig_rec, width="stretch")

    st.divider()

    st.subheader(t("cv_yoy_title"))
    explainer("cv_yoy_explainer")

    yoy = load_yoy()
    yoy_covid = yoy[
        (yoy["month_start"] >= "2020-01-01") &
        (yoy["month_start"] <= "2022-07-01") &
        (yoy["modo"].isin(selected_modes))
    ].dropna(subset=["yoy_pct_change"]).copy()
    yoy_covid["modo_label"] = yoy_covid["modo"].map(mode_label)

    fig6 = px.bar(
        yoy_covid, x="month_start", y="yoy_pct_change",
        color="modo_label", barmode="group",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        labels={"yoy_pct_change": t("cv_yoy_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    fig6.add_hline(y=0, line_color="black", line_width=1)
    fig6.update_layout(height=600, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig6, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — MODAL SUBSTITUTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_ms:

    # Monthly change — one chart per mode so shapes are directly comparable
    _ms_title = ("Variación mensual por modo" if st.session_state.lang == "es"
                 else "Monthly change by mode")
    _ms_expl  = (
        "El **SUBTE** muestra picos más pronunciados que los otros modos, lo que sugiere que "
        "tiene menor sustitución posible ante shocks discretos (lockdowns, paros, cortes de servicio): "
        "quien depende del subte no tendría una alternativa inmediata comparable. "
        "El **COLECTIVO** muestra mayor resiliencia, posiblemente porque cubre zonas sin red de subte o tren."
        if st.session_state.lang == "es" else
        "**SUBTE** shows sharper spikes than the other modes, suggesting it has fewer substitutes "
        "when discrete shocks hit (lockdowns, strikes, service disruptions): "
        "subway riders may have no comparable immediate alternative. "
        "**COLECTIVO** shows greater resilience, possibly because it serves areas without subway or rail coverage."
    )
    st.subheader(_ms_title)
    with st.expander("ℹ️ " + ("¿Cómo leer este gráfico?" if st.session_state.lang == "es"
                               else "How to read this chart?")):
        st.markdown(_ms_expl)

    _ms_full = monthly[
        (monthly["month_start"] >= start_date) &
        (monthly["month_start"] <= end_date) &
        (monthly["modo"].isin(selected_modes))
    ].copy().sort_values(["modo", "month_start"])
    _ms_full["mom_pct"]    = _ms_full.groupby("modo")["total_usos"].pct_change() * 100
    _ms_full               = _ms_full.dropna(subset=["mom_pct"])

    _mom_label   = "Variación mensual (%)" if st.session_state.lang == "es" else "Monthly change (%)"
    _active_modes = [m for m in selected_modes if m in _ms_full["modo"].unique()]

    if _active_modes:
        from plotly.subplots import make_subplots
        _n = len(_active_modes)
        _fig_ms_sub = make_subplots(
            rows=_n, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            row_titles=[mode_label(m) for m in _active_modes],
        )
        for _i, _ms_mode in enumerate(_active_modes, start=1):
            _ms_mode_df = _ms_full[_ms_full["modo"] == _ms_mode]
            _fig_ms_sub.add_trace(
                go.Scatter(
                    x=_ms_mode_df["month_start"],
                    y=_ms_mode_df["mom_pct"],
                    mode="lines+markers",
                    line=dict(color=cmap[_ms_mode], width=2),
                    marker=dict(size=4),
                    showlegend=False,
                    hovertemplate="%{x|%b %Y}<br>%{y:.1f}%<extra></extra>",
                ),
                row=_i, col=1,
            )
            _fig_ms_sub.add_hline(
                y=0, line_color="black", line_width=1, opacity=0.3,
                row=_i, col=1,
            )

        # Set y-axis range explicitly for each subplot axis — update_yaxes with
        # row/col is unreliable with shared_xaxes; direct yaxis keys are guaranteed.
        _yaxis_updates = {f"yaxis{'' if i == 1 else i}": dict(range=[-50, 75], ticksuffix="%")
                          for i in range(1, _n + 1)}
        _fig_ms_sub.update_layout(
            height=_n * 300,
            template="plotly_white",
            hovermode="x unified",
            margin=dict(t=20, b=20),
            **_yaxis_updates,
        )
        # Add event/fare annotations to the top subplot only (shared x-axis, lines span all rows)
        if show_events:
            _fig_ms_sub = add_event_annotations(_fig_ms_sub)
            _fig_ms_sub = add_fare_annotations(_fig_ms_sub)
        # Re-apply range after annotations (staggered_annotations calls update_yaxes
        # without row/col, which resets all subplot axes to its auto-computed range)
        _fig_ms_sub.update_layout(**_yaxis_updates)
        st.plotly_chart(_fig_ms_sub, width="stretch")

    st.divider()

    # Modal share — full series
    st.subheader(t("ms_share_title"))
    explainer("ms_share_explainer")

    share_df = load_modal_split()
    share_df = share_df[
        (share_df["month_start"] >= start_date) &
        (share_df["month_start"] <= end_date) &
        (share_df["modo"].isin(selected_modes))
    ].copy()
    share_df["modo_label"] = share_df["modo"].map(mode_label)

    fig_ms2 = px.area(
        share_df, x="month_start", y="mode_share_pct",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        labels={"mode_share_pct": t("ov_split_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    if show_events:
        fig_ms2 = add_event_annotations(fig_ms2)
        fig_ms2 = add_fare_annotations(fig_ms2)
    fig_ms2.update_layout(height=570, yaxis_ticksuffix="%", hovermode="x unified",
                          yaxis=dict(range=[0, 125]))
    st.plotly_chart(fig_ms2, width="stretch")

    st.divider()

    # YoY % — full series
    st.subheader(t("ms_yoy_title"))
    explainer("ms_yoy_explainer")

    yoy_all = load_yoy()
    yoy_all = yoy_all[
        (yoy_all["month_start"] >= start_date) &
        (yoy_all["month_start"] <= end_date) &
        (yoy_all["modo"].isin(selected_modes))
    ].dropna(subset=["yoy_pct_change"]).copy()
    yoy_all["modo_label"] = yoy_all["modo"].map(mode_label)

    fig_ms3 = px.bar(
        yoy_all, x="month_start", y="yoy_pct_change",
        color="modo_label", barmode="group",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        labels={"yoy_pct_change": t("cv_yoy_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    fig_ms3.add_hline(y=0, line_color="black", line_width=1)
    if show_events:
        fig_ms3 = add_event_annotations(fig_ms3)
        fig_ms3 = add_fare_annotations(fig_ms3)
    fig_ms3.update_layout(height=630, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig_ms3, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — RESILIENCE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_rs:

    lang        = st.session_state.lang
    amba_df     = load_amba_recovery()
    amba_labels = STRINGS[lang]["amba_labels"]
    amba_colors = {"SI": "#2563EB", "NO": "#F59E0B"}

    amba_plot           = amba_df.copy()
    amba_plot["region"] = amba_plot["amba"].map(amba_labels)

    finding("rs_finding")

    # Dual y-axis so AMBA and Interior use independent scales
    st.subheader(t("rs_amba_title"))
    explainer("rs_amba_explainer")

    amba_series     = amba_plot[amba_plot["amba"] == "SI"].sort_values("month_start")
    interior_series = amba_plot[amba_plot["amba"] == "NO"].sort_values("month_start")

    fig7 = go.Figure()
    fig7.add_trace(go.Scatter(
        x=amba_series["month_start"], y=amba_series["total"],
        name=amba_labels["SI"],
        line=dict(color=amba_colors["SI"], width=2),
        yaxis="y1",
    ))
    fig7.add_trace(go.Scatter(
        x=interior_series["month_start"], y=interior_series["total"],
        name=amba_labels["NO"],
        line=dict(color=amba_colors["NO"], width=2),
        yaxis="y2",
    ))
    if show_events:
        fig7 = add_event_annotations(fig7)

    fig7.update_layout(
        height=645,
        hovermode="x unified",
        template="plotly_white",
        legend=dict(orientation="h", y=-0.15),
        yaxis=dict(
            title=dict(text=f"AMBA — {t('ov_series_y')}", font=dict(color=amba_colors["SI"])),
            tickfont=dict(color=amba_colors["SI"]),
        ),
        yaxis2=dict(
            title=dict(text=f"Interior — {t('ov_series_y')}", font=dict(color=amba_colors["NO"])),
            tickfont=dict(color=amba_colors["NO"]),
            overlaying="y",
            side="right",
        ),
    )
    st.plotly_chart(fig7, width="stretch")

    st.divider()

    st.subheader(t("rs_milei_title"))
    explainer("rs_milei_explainer")

    milei_df = amba_plot[amba_plot["month_start"] >= "2023-01-01"].copy()

    # Compute 12-month rolling average on the full series, then filter to chart window
    # (needs pre-2023 data to anchor the first rolling values)
    _rolling_label = "Promedio móvil 12 meses" if lang == "es" else "12-month rolling avg"
    for _amba_key, _region_label in amba_labels.items():
        _full_region = amba_plot[amba_plot["amba"] == _amba_key].sort_values("month_start").copy()
        _full_region["rolling_12"] = _full_region["recovery_index"].rolling(12, min_periods=6).mean()
        _rolling_visible = _full_region[_full_region["month_start"] >= "2023-01-01"]
        if not _rolling_visible.empty:
            milei_df = milei_df  # reference kept; rolling added as separate trace below

    fig8 = px.line(
        milei_df, x="month_start", y="recovery_index",
        color="region",
        color_discrete_map={v: amba_colors[k] for k, v in amba_labels.items()},
        markers=True,
        labels={"recovery_index": "Índice (Ene 2020 = 100)" if lang == "es" else "Index (Jan 2020 = 100)",
                "month_start": "", "region": ""},
        template="plotly_white",
    )

    # Overlay 12-month rolling average as dotted lines
    for _amba_key, _region_label in amba_labels.items():
        _full_region = amba_plot[amba_plot["amba"] == _amba_key].sort_values("month_start").copy()
        _full_region["rolling_12"] = _full_region["recovery_index"].rolling(12, min_periods=6).mean()
        _rolling_visible = _full_region[_full_region["month_start"] >= "2023-01-01"].dropna(subset=["rolling_12"])
        if not _rolling_visible.empty:
            fig8.add_scatter(
                x=_rolling_visible["month_start"],
                y=_rolling_visible["rolling_12"],
                mode="lines",
                line=dict(color=amba_colors[_amba_key], width=2.5, dash="dot"),
                name=f"{_region_label} — {_rolling_label}",
                showlegend=True,
                hovertemplate=f"{_region_label} ({_rolling_label})<br>%{{x|%b %Y}}: %{{y:.1f}}<extra></extra>",
            )
    fig8.add_hline(y=100, line_dash="dash", line_color="grey", opacity=0.4,
                   annotation_text="Ene 2020" if lang == "es" else "Jan 2020")

    # 4d — Shade the Jan–Feb 2024 shock window and label the divergence
    _shock_label = (
        "Shock tarifario AMBA<br>+45% → +66%" if lang == "es"
        else "AMBA fare shock<br>+45% → +66%"
    )
    fig8.add_vrect(
        x0="2024-01-01", x1="2024-03-01",
        fillcolor="#EF4444", opacity=0.08,
        layer="below", line_width=0,
    )
    fig8.add_annotation(
        x=pd.Timestamp("2024-02-01"), y=0.97, yref="paper",
        text=f"<b>{_shock_label}</b>",
        showarrow=False,
        font=dict(size=10, color="#EF4444"),
        xanchor="center", yanchor="top",
        bgcolor="white", borderpad=3,
    )

    fig8 = add_fare_annotations(fig8)
    fig8.update_layout(height=600, hovermode="x unified")
    st.plotly_chart(fig8, width="stretch")

    st.divider()

    # Province histogram and heatmap moved to Overview tab




# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — ANOMALIES (STL decomposition + anomaly detection)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_an:

    finding("an_finding")

    st.divider()


    st.subheader(t("an_stl_title"))
    explainer("an_stl_explainer")

    col_l, col_r = st.columns(2)
    with col_l:
        stl_mode = st.selectbox(
            t("an_stl_mode"),
            ["ALL"] + [m for m in DASHBOARD_MODES if m in selected_modes],
            format_func=lambda m: t("an_stl_all") if m == "ALL" else mode_label(m),
        )
    with col_r:
        stl_period = st.radio(
            t("an_stl_season"),
            [7, 365],
            format_func=lambda p: t("an_stl_weekly") if p == 7 else t("an_stl_annual"),
            horizontal=True,
        )

    # Auto-run on load / whenever controls change (cached by Streamlit's widget state)
    with st.spinner(t("an_stl_running")):
        try:
            from analytics.time_series import decompose_series, detect_anomalies
            conn = get_conn()
            result = decompose_series(
                conn,
                mode=None if stl_mode == "ALL" else stl_mode,
                period=stl_period,
            )
            if result:
                anomalies = detect_anomalies(
                    result["residual"],
                    lang=st.session_state.lang,
                )

                fig11 = go.Figure()
                for key, name, color, fill in [
                    ("original", t("an_stl_observed"), "rgba(100,100,200,0.25)", True),
                    ("trend",    t("an_stl_trend"),    "#2563EB",                False),
                    ("seasonal", t("an_stl_seasonal"), "#16A34A",                False),
                    ("residual", t("an_stl_residual"), "#94a3b8",                False),
                ]:
                    s = result[key]
                    fig11.add_scatter(
                        x=s.index, y=s.values, name=name,
                        line_color=color,
                        fill="tozeroy" if fill else None,
                        opacity=0.8 if fill else 1.0,
                    )

                anom = anomalies[anomalies["is_anomaly"]]
                fig11.add_scatter(
                    x=anom["fecha"], y=anom["residual"],
                    mode="markers", name=t("an_stl_anomaly"),
                    marker=dict(color="red", size=7, symbol="x"),
                    hovertemplate="<b>%{x}</b><br>%{y:,.0f}<br>%{text}",
                    text=anom["event_label"],
                )
                fig11.update_layout(
                    height=660, template="plotly_white",
                    hovermode="x unified",
                    legend_title=t("an_stl_component"),
                )
                st.plotly_chart(fig11, width="stretch")

                if not anom.empty:
                    st.subheader(f"🚨 {len(anom)} {t('an_anom_title')}")
                    st.caption(t("an_anom_explainer"))
                    st.dataframe(
                        anom[["fecha", "z_score", "event_label"]]
                        .sort_values("z_score", key=abs, ascending=False)
                        .rename(columns={
                            "fecha":       t("an_anom_date"),
                            "z_score":     t("an_anom_z"),
                            "event_label": t("an_anom_event"),
                        }),
                        width="stretch",
                    )
        except ImportError:
            st.error("statsmodels not installed. Run: uv add statsmodels")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 5 — FORECAST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _hex_to_rgb(hex_color: str) -> tuple[float, float, float]:
    """Convert '#RRGGBB' to (r, g, b) floats in 0–1 range."""
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i+2], 16) / 255 for i in (0, 2, 4))


with tab_fc:

    explainer("fc_explainer")

    horizon = st.select_slider(
        t("fc_horizon"),
        options=[3, 6, 9, 12, 15, 18, 21, 24],
        value=12,
    )

    with st.spinner(t("fc_running")):
        try:
            from analytics.ml import forecast_ridership, forecast_summary
            conn = get_conn()
            forecasts = forecast_ridership(
                conn,
                modes=[m for m in selected_modes if m in DASHBOARD_MODES],
                horizon=horizon,
            )

            if not forecasts:
                st.warning(
                    "No forecast results. Check that the pipeline has run and data is loaded."
                )
            else:
                st.subheader(t("fc_title").format(n=horizon))

                for mode, fc in forecasts.items():
                    hist = fc[~fc["is_forecast"]]
                    pred = fc[fc["is_forecast"]]
                    r, g, b = _hex_to_rgb(cmap[mode])

                    fig = go.Figure()

                    # Confidence band
                    fig.add_scatter(
                        x=pd.concat([pred["ds"], pred["ds"].iloc[::-1]]),
                        y=pd.concat([pred["yhat_upper"], pred["yhat_lower"].iloc[::-1]]),
                        fill="toself",
                        fillcolor=f"rgba({int(r*255)},{int(g*255)},{int(b*255)},0.15)",
                        line=dict(width=0),
                        showlegend=True,
                        name=t("fc_band"),
                    )

                    # Raw actuals as faded dots — context without visual noise
                    fig.add_scatter(
                        x=hist["ds"], y=hist["actual"],
                        mode="markers",
                        marker=dict(color=cmap[mode], size=5, opacity=0.35),
                        name=t("fc_actual"),
                        showlegend=True,
                    )

                    # Fitted values (historical) — smooth line that leads into forecast
                    fig.add_scatter(
                        x=hist["ds"], y=hist["yhat"],
                        mode="lines",
                        line=dict(color=cmap[mode], width=2),
                        name=t("fc_fitted"),
                        showlegend=True,
                    )

                    # Forecast line — visually continuous from fitted
                    # Prepend the last fitted point so there's no gap
                    last_hist = hist.iloc[[-1]]
                    pred_with_join = pd.concat([last_hist, pred], ignore_index=True)
                    fig.add_scatter(
                        x=pred_with_join["ds"], y=pred_with_join["yhat"],
                        mode="lines+markers",
                        line=dict(color=cmap[mode], width=2, dash="dash"),
                        marker=dict(size=6),
                        name=t("fc_forecast"),
                    )

                    # Vertical marker at forecast start
                    fig.add_vline(
                        x=hist["ds"].max().timestamp() * 1000,
                        line_dash="dot", line_color="grey", opacity=0.6,
                        annotation_text=(
                            "→ predicción" if st.session_state.lang == "es" else "→ forecast"
                        ),
                        annotation_font_size=10,
                    )

                    if show_events:
                        fig = add_event_annotations(fig)
                        fig = add_fare_annotations(fig)

                    fig.update_layout(
                        height=480,
                        template="plotly_white",
                        title=mode_label(mode),
                        yaxis_title=t("ov_series_y"),
                        hovermode="x unified",
                        legend=dict(orientation="h", y=-0.25),
                        margin=dict(t=40, b=60),
                    )
                    st.plotly_chart(fig, width="stretch")

                # ── Summary table ──────────────────────────────────────
                st.divider()
                st.subheader(t("fc_summary_title"))

                summary = forecast_summary(forecasts)
                if not summary.empty:
                    direction_map = {
                        "up":   t("fc_direction_up"),
                        "down": t("fc_direction_down"),
                        "flat": t("fc_direction_flat"),
                    }
                    summary["mode"]          = summary["mode"].map(mode_label)
                    summary["direction"]     = summary["direction"].map(direction_map)
                    summary["last_actual"]   = summary["last_actual"].apply(lambda x: f"{x/1e6:.1f}M")
                    summary["mean_forecast"] = summary["mean_forecast"].apply(lambda x: f"{x/1e6:.1f}M")
                    summary["pct_change"]    = summary["pct_change"].apply(lambda x: f"{x:+.1f}%")

                    st.dataframe(
                        summary.rename(columns={
                            "mode":          t("fc_mode"),
                            "last_actual":   t("fc_last"),
                            "mean_forecast": t("fc_mean"),
                            "pct_change":    t("fc_change"),
                            "direction":     "",
                        }),
                        width="stretch",
                        hide_index=True,
                    )

        except ImportError as e:
            st.error(f"Missing dependency: {e}. Run: uv add prophet")


st.divider()
st.caption(
    "Fuente / Source: [datos.transporte.gob.ar](https://datos.transporte.gob.ar) · "
    "CC Attribution 4.0 · Actualización automática diaria / Daily auto-update"
)