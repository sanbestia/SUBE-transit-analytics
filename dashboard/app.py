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
    page_title="SUBE Transit Analytics",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global styles ──────────────────────────────────────────────────────────
st.markdown("""
<style>
.stTabs [data-baseweb="tab"] {
    font-size: 1.2rem !important;
    padding-top: 16px !important;
    padding-bottom: 16px !important;
    padding-left: 28px !important;
    padding-right: 28px !important;
}
.stTabs [data-baseweb="tab"] p {
    font-size: 1.2rem !important;
}
</style>
""", unsafe_allow_html=True)

# ── Translations ───────────────────────────────────────────────────────────
from dashboard.strings import STRINGS, MODE_LABELS
from analytics.causal import its_analysis as _its_analysis, build_counterfactual_df, TREATMENT_DATE as _ITS_TREATMENT
from dashboard.utils import (
    load_monthly as _load_monthly,
    load_daily_totals as _load_daily_totals,
    load_modal_split as _load_modal_split,
    load_yoy as _load_yoy,
    load_heatmap as _load_heatmap,
    load_amba_recovery as _load_amba_recovery,
    load_top_empresas as _load_top_empresas,
    load_by_provincia as _load_by_provincia,
    load_combined_monthly as _load_combined_monthly,
    add_event_annotations as _add_event_annotations,
    add_fare_annotations as _add_fare_annotations,
    mode_color_map, hex_to_rgb, compute_mom_pct, index_to_baseline,
)

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
    return _load_monthly(get_conn())


@st.cache_data(ttl=3600)
def load_combined_monthly() -> pd.DataFrame:
    return _load_combined_monthly(get_conn())


@st.cache_data(ttl=3600)
def load_daily_totals() -> pd.DataFrame:
    return _load_daily_totals(get_conn())


@st.cache_data(ttl=3600)
def load_modal_split() -> pd.DataFrame:
    return _load_modal_split(get_conn())


@st.cache_data(ttl=3600)
def load_yoy() -> pd.DataFrame:
    return _load_yoy(get_conn())


@st.cache_data(ttl=3600)
def load_heatmap() -> pd.DataFrame:
    return _load_heatmap(get_conn())


@st.cache_data(ttl=3600)
def load_amba_recovery() -> pd.DataFrame:
    return _load_amba_recovery(get_conn())


@st.cache_data(ttl=3600)
def load_top_empresas() -> pd.DataFrame:
    return _load_top_empresas(get_conn())


@st.cache_data(ttl=3600)
def load_by_provincia() -> pd.DataFrame:
    return _load_by_provincia(get_conn())


@st.cache_data(ttl=3600)
def load_its() -> pd.DataFrame:
    return _its_analysis(get_conn())


# ── Chart helpers ──────────────────────────────────────────────────────────

def add_event_annotations(fig: go.Figure, y_ref: float = 0, x_min=None, x_max=None) -> go.Figure:
    """Annotate fig with historical events (language from session state)."""
    return _add_event_annotations(fig, lang=st.session_state.lang, x_min=x_min, x_max=x_max)


def add_fare_annotations(
    fig: go.Figure, y_ref: float = 0, scope_filter: list | None = None,
    x_min=None, x_max=None,
) -> go.Figure:
    """Annotate fig with fare hike events (language from session state)."""
    return _add_fare_annotations(fig, lang=st.session_state.lang,
                                 scope_filter=scope_filter, x_min=x_min, x_max=x_max)


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
    max_date = daily["fecha"].max()

    # Sidebar goes back to 2013 (full COLECTIVO history).
    # SUBTE/TREN pre-2016 data is excluded in the chart itself.
    min_date = pd.Timestamp("2013-01-01").date()

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

# Extended series: monthly_historical + monthly_transactions
# Falls back to monthly_transactions only if historical table doesn't exist yet
combined_monthly = load_combined_monthly()

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
tab_ov, tab_an, tab_fc, tab_its, tab_cv, tab_ms, tab_rs = st.tabs([
    t("tab_overview"), t("tab_analysis"), t("tab_forecast"),
    t("tab_its"), t("tab_covid"), t("tab_modal"), t("tab_resilience"),
])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — OVERVIEW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_ov:

    st.subheader(t("ov_series_title"))
    explainer("ov_series_explainer")

    fig = go.Figure()

    # Pre-2020 monthly data — plotted as a line (same style as post-2020 MA)
    # using average daily trips (total_usos / days_in_month) to match the daily scale.
    # COLECTIVO extends to 2013; SUBTE/TREN only from 2016 (pre-2016 SUBE coverage incomplete).
    _pre2020 = combined_monthly[
        (combined_monthly["month_start"] >= start_date) &
        (combined_monthly["month_start"] < "2020-01-01") &
        (combined_monthly["modo"].isin(selected_modes)) &
        (
            (combined_monthly["modo"] == "COLECTIVO") |
            (combined_monthly["month_start"] >= "2016-01-01")
        )
    ].copy()
    _has_pre2020 = False
    if not _pre2020.empty:
        _pre2020["days_in_month"] = _pre2020["month_start"].apply(
            lambda d: (d + pd.offsets.MonthEnd(1)).day
        )
        _pre2020["avg_daily"] = (_pre2020["total_usos"] / _pre2020["days_in_month"]).round(0)
        _has_pre2020 = True
    for mode in selected_modes:
        _pre_mode = _pre2020[_pre2020["modo"] == mode].sort_values("month_start") if _has_pre2020 else pd.DataFrame()
        if _pre_mode.empty:
            continue
        fig.add_scatter(
            x=_pre_mode["month_start"],
            y=_pre_mode["avg_daily"],
            mode="lines",
            name=mode_label(mode),
            line=dict(color=cmap[mode], width=2.5),
            showlegend=False,
            hovertemplate="%{x|%b %Y}<br>%{y:,.0f}<extra></extra>",
        )

    # Post-2020 daily data — raw (faint) + 7-day MA
    for mode in selected_modes:
        mode_df = df_daily[
            (df_daily["modo"] == mode) &
            ~((df_daily["fecha"].dt.month == 1) & (df_daily["fecha"].dt.day == 1))
        ].sort_values("fecha")
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

    # Dotted gap lines — connect last pre-2020 monthly point to first daily point (Jan 2 2020)
    # per mode, and add a "missing data" bracket over the gap region.
    _gap_x1 = pd.Timestamp("2020-01-02")
    _has_gap_lines = False
    if _has_pre2020:
        for mode in selected_modes:
            _pre_mode = _pre2020[_pre2020["modo"] == mode].sort_values("month_start")
            if _pre_mode.empty:
                continue
            # Last pre-2020 point
            _last_pre = _pre_mode.iloc[-1]
            _gap_y0 = float(_last_pre["avg_daily"])
            # First daily point for this mode on Jan 2 2020
            _daily_jan2 = df_daily[
                (df_daily["modo"] == mode) &
                (df_daily["fecha"] == _gap_x1)
            ]
            if _daily_jan2.empty:
                # Fall back to first available daily point
                _daily_first = df_daily[df_daily["modo"] == mode].sort_values("fecha")
                if _daily_first.empty:
                    continue
                _gap_x1_mode = _daily_first.iloc[0]["fecha"]
                _gap_y1 = float(_daily_first.iloc[0]["cantidad_usos"])
            else:
                _gap_x1_mode = _gap_x1
                _gap_y1 = float(_daily_jan2.iloc[0]["cantidad_usos"])

            fig.add_scatter(
                x=[_last_pre["month_start"], _gap_x1_mode],
                y=[_gap_y0, _gap_y1],
                mode="lines",
                line=dict(color=cmap[mode], width=1.5, dash="dot"),
                showlegend=False,
                hoverinfo="skip",
            )
            _has_gap_lines = True

    # Missing data bracket over the Oct 2019 → Jan 2 2020 gap
    if _has_gap_lines:
        _gap_x0 = pd.Timestamp("2019-10-01")
    if show_events:
        fig = add_event_annotations(fig)

    # Compute y max across pre-2020 data for bracket positioning
    if _has_pre2020:
        _y_max_pre = float(_pre2020["avg_daily"].max())
        # Also consider post-2020 daily max in case it's higher
        _y_max_daily_pre_region = float(
            df_daily[df_daily["fecha"] < "2020-06-01"]["cantidad_usos"].max()
            if not df_daily.empty else _y_max_pre
        )
        _y_max = max(_y_max_pre, _y_max_daily_pre_region) if not pd.isna(_y_max_daily_pre_region) else _y_max_pre
        _y_bracket_main  = _y_max * 1.04   # main bracket: just above data
        _y_bracket_tick  = _y_max * 1.02
        _y_bracket_label = _y_max * 1.06
        _y_bracket_gap      = _y_max * 0.97
        _y_bracket_gap_tick = _y_max * 0.955

        # ── Main bracket: pre-2020 region ─────────────────────────────────
        _bracket_x0 = _pre2020["month_start"].min()
        _bracket_x1 = pd.Timestamp("2020-01-01")
        _bracket_label = (
            "Sin datos diarios — promedio por día calculado"
            if st.session_state.lang == "es"
            else "No daily data available — average per day shown"
        )
        fig.add_shape(type="line",
                      x0=_bracket_x0, x1=_bracket_x1,
                      y0=_y_bracket_main, y1=_y_bracket_main,
                      xref="x", yref="y",
                      line=dict(color="#888888", width=1.5))
        fig.add_shape(type="line",
                      x0=_bracket_x0, x1=_bracket_x0,
                      y0=_y_bracket_tick, y1=_y_bracket_main,
                      xref="x", yref="y",
                      line=dict(color="#888888", width=1.5))
        fig.add_shape(type="line",
                      x0=_bracket_x1, x1=_bracket_x1,
                      y0=_y_bracket_tick, y1=_y_bracket_main,
                      xref="x", yref="y",
                      line=dict(color="#888888", width=1.5))
        fig.add_annotation(
            x=_bracket_x0 + (_bracket_x1 - _bracket_x0) / 2,
            y=_y_bracket_label,
            xref="x", yref="y",
            text=_bracket_label,
            showarrow=False,
            font=dict(size=10, color="#888888"),
            xanchor="center", yanchor="bottom",
        )

    # ── Gap bracket: Oct 2019 → Jan 2 2020 ────────────────────────────────
    if _has_gap_lines:
        _gap_x0 = pd.Timestamp("2019-10-01")
        _gap_label = (
            "Sin datos disponibles"
            if st.session_state.lang == "es"
            else "No data available"
        )
        fig.add_shape(type="line",
                      x0=_gap_x0, x1=_gap_x1,
                      y0=_y_bracket_gap, y1=_y_bracket_gap,
                      xref="x", yref="y",
                      line=dict(color="#bbbbbb", width=1.2, dash="dot"))
        fig.add_shape(type="line",
                      x0=_gap_x0, x1=_gap_x0,
                      y0=_y_bracket_gap_tick, y1=_y_bracket_gap,
                      xref="x", yref="y",
                      line=dict(color="#bbbbbb", width=1.2))
        fig.add_shape(type="line",
                      x0=_gap_x1, x1=_gap_x1,
                      y0=_y_bracket_gap_tick, y1=_y_bracket_gap,
                      xref="x", yref="y",
                      line=dict(color="#bbbbbb", width=1.2))
        fig.add_annotation(
            x=_gap_x0 + (_gap_x1 - _gap_x0) / 2,
            y=_y_bracket_gap * 1.005,
            xref="x", yref="y",
            text=_gap_label,
            showarrow=False,
            font=dict(size=9, color="#bbbbbb"),
            xanchor="center", yanchor="bottom",
        )

    fig.update_layout(
        height=675, template="plotly_white",
        yaxis_title=t("ov_series_y"),
        legend_title=None, hovermode="x unified",
        xaxis=dict(range=["2016-01-01", str(max_date)]),
    )
    st.plotly_chart(fig, width="stretch")

    # Province histogram (moved from Resilience tab)
    _prov_title = ("Viajes totales por provincia" if st.session_state.lang == "es"
                   else "Total trips by province")
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

    _ms_full = combined_monthly[
        combined_monthly["modo"].isin(selected_modes)
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
            _fig_ms_sub = add_event_annotations(_fig_ms_sub, x_min="2016-01-01")
            _fig_ms_sub = add_fare_annotations(_fig_ms_sub, x_min="2016-01-01")
        # Re-apply y-axis ranges and x-axis zoom after annotations
        # (staggered_annotations calls update_xaxes/update_yaxes which resets them)
        _ms_x_range = ["2016-01-01", str(combined_monthly["month_start"].max())]
        _fig_ms_sub.update_layout(
            xaxis=dict(range=_ms_x_range),
            **_yaxis_updates,
        )
        st.plotly_chart(_fig_ms_sub, width="stretch")

    st.divider()

    # Modal share — full series
    st.subheader(t("ms_share_title"))
    explainer("ms_share_explainer")

    share_df = combined_monthly[
        (combined_monthly["month_start"] >= "2016-01-01") &
        combined_monthly["modo"].isin(selected_modes)
    ].copy()
    _share_totals = share_df.groupby("month_start")["total_usos"].sum().rename("month_total")
    share_df = share_df.join(_share_totals, on="month_start")
    share_df["mode_share_pct"] = (share_df["total_usos"] / share_df["month_total"] * 100).round(2)
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
# TAB — FARE IMPACT (ITS causal analysis)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_its:

    st.subheader(t("rs_its_title"))
    explainer("rs_its_explainer")
    finding("rs_its_finding")

    its_df = load_its()

    if not its_df.empty:
        from plotly.subplots import make_subplots

        _its_modes = [m for m in DASHBOARD_MODES if m in its_df["mode"].values]
        _n_its     = len(_its_modes)

        _fig_its = make_subplots(
            rows=_n_its, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            row_titles=[mode_label(m) for m in _its_modes],
        )

        for _i, _its_mode in enumerate(_its_modes, start=1):
            _row  = its_df[its_df["mode"] == _its_mode].iloc[0]
            _cfdf = build_counterfactual_df(_row, _ITS_TREATMENT)
            _pre  = _cfdf[_cfdf["post"] == 0]
            _post = _cfdf[_cfdf["post"] == 1]
            _col  = MODE_COLORS[_its_mode]
            _show_legend = (_i == 1)

            # Pre-treatment: faint actual
            _fig_its.add_trace(go.Scatter(
                x=_pre["ds"], y=_pre["actual"],
                mode="lines", name=t("rs_its_actual"),
                line=dict(color=_col, width=1.5),
                opacity=0.35,
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            # Pre-treatment: fitted trend
            _fig_its.add_trace(go.Scatter(
                x=_pre["ds"], y=_pre["fitted"],
                mode="lines", name="Fitted (pre)",
                line=dict(color=_col, width=1.8, dash="dot"),
                opacity=0.65,
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            # Post-treatment: actual
            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["actual"],
                mode="lines+markers", name=t("rs_its_actual"),
                line=dict(color=_col, width=2.5),
                marker=dict(size=5),
                showlegend=False,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            # Counterfactual
            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["counterfactual"],
                mode="lines", name=t("rs_its_cf"),
                line=dict(color="#9CA3AF", width=2, dash="dash"),
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            # Shaded gap
            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["counterfactual"],
                mode="none", showlegend=False, hoverinfo="skip",
            ), row=_i, col=1)
            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["actual"],
                mode="none", name=t("rs_its_gap"),
                fill="tonexty",
                fillcolor="rgba(239,68,68,0.15)",
                line=dict(width=0),
                showlegend=_show_legend,
                hoverinfo="skip",
            ), row=_i, col=1)

        # Treatment line on all subplots
        _fig_its.add_vline(
            x=_ITS_TREATMENT.timestamp() * 1000,
            line_dash="dash", line_color="#EF4444", line_width=1.5,
        )
        # Annotation on top subplot only
        _fig_its.add_annotation(
            x=_ITS_TREATMENT, y=1, yref="paper",
            text=f"<b>{t('rs_its_treatment')}</b>",
            showarrow=False, xanchor="left", yanchor="top",
            font=dict(size=10, color="#EF4444"),
            bgcolor="white", borderpad=2,
        )

        _its_yaxis_updates = {
            f"yaxis{'' if i == 1 else i}": dict(tickformat=",.0f")
            for i in range(1, _n_its + 1)
        }
        _fig_its.update_layout(
            height=_n_its * 320,
            hovermode="x unified",
            template="plotly_white",
            margin=dict(t=20, b=20),
            legend=dict(orientation="h", y=-0.06),
            **_its_yaxis_updates,
        )
        st.plotly_chart(_fig_its, width="stretch")

        # ── Plain-language metrics — one row per mode ─────────────────────
        # Pre-compute counterfactual dfs (already built for the chart above,
        # but we need them here too for gap sums)
        _its_cfdfs = {
            m: build_counterfactual_df(
                its_df[its_df["mode"] == m].iloc[0], _ITS_TREATMENT
            )
            for m in _its_modes
        }

        _header_cols = st.columns([2, 2, 2, 2])
        _header_cols[0].markdown("&nbsp;")
        _header_cols[1].markdown(f"**{t('rs_its_metric_lost')}**")
        _header_cols[2].markdown(f"**{t('rs_its_metric_now')}**")
        _header_cols[3].markdown(f"**{t('rs_its_metric_drift')}**")

        for _its_mode in _its_modes:
            _row   = its_df[its_df["mode"] == _its_mode].iloc[0]
            _cfdf  = _its_cfdfs[_its_mode]
            _post_cf = _cfdf[_cfdf["post"] == 1]

            # 1. Cumulative trips lost (positive = fewer than expected)
            _cum_gap   = float((_post_cf["counterfactual"] - _post_cf["actual"]).sum())
            _cum_label = f"{abs(_cum_gap)/1e6:.0f}M"
            _cum_delta = t("rs_its_metric_lost_sub")

            # 2. Latest month gap as %
            _latest    = _post_cf.iloc[-1]
            _gap_pct   = (_latest["counterfactual"] - _latest["actual"]) / _latest["counterfactual"] * 100
            _now_label = f"{abs(_gap_pct):.1f}% {'below' if _gap_pct > 0 else 'above'}"
            if st.session_state.lang == "es":
                _now_label = f"{abs(_gap_pct):.1f}% {'por debajo' if _gap_pct > 0 else 'por encima'}"
            _now_delta = t("rs_its_metric_now_sub")

            # 3. Post-shock drift in plain language
            _slope     = _row["beta_slope"]
            _sig_slope = _row["pvalue_slope"] < 0.05
            if not _sig_slope:
                _drift_label = t("rs_its_drift_flat")
                _drift_delta = ""
            elif _slope < 0:
                _drift_label = t("rs_its_drift_falling").format(n=f"{abs(_slope)/1e6:.2f}")
                _drift_delta = ""
            else:
                _drift_label = t("rs_its_drift_rising").format(n=f"{abs(_slope)/1e6:.2f}")
                _drift_delta = ""

            _cols = st.columns([2, 2, 2, 2])
            _cols[0].markdown(f"**{mode_label(_its_mode)}**")
            _cols[1].metric("", _cum_label, _cum_delta, delta_color="off")
            _cols[2].metric("", _now_label, _now_delta, delta_color="off")
            _cols[3].metric("", _drift_label, _drift_delta or None, delta_color="off")

        st.caption(t("rs_its_note"))


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

# hex_to_rgb imported from dashboard.utils


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
                    r, g, b = hex_to_rgb(cmap[mode])

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