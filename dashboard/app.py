"""
dashboard/app.py — SUBE Transit Analytics Dashboard

Run with:
    streamlit run dashboard/app.py
"""

import datetime as _dt
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DASHBOARD_MODES, DB_PATH, FARE_HIKES, MODE_COLORS, TRANSPORT_MODES
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
from analytics.causal import its_analysis as _its_analysis, build_counterfactual_df, TREATMENT_DATE as ITS_TREATMENT
from dashboard.utils import (
    load_monthly as _load_monthly,
    load_daily_totals as _load_daily_totals,
    load_modal_split as _load_modal_split,
    load_yoy as _load_yoy,
    load_heatmap as _load_heatmap,
    load_amba_recovery as _load_amba_recovery,
    load_amba_by_mode as _load_amba_by_mode,
    load_top_empresas as _load_top_empresas,
    load_by_provincia as _load_by_provincia,
    load_combined_monthly as _load_combined_monthly,
)

# ── Session state ──────────────────────────────────────────────────────────
if "lang" not in st.session_state:
    st.session_state.lang = "es"


def t(key: str) -> str:
    return STRINGS[st.session_state.lang].get(key, key)


def mode_label(mode: str) -> str:
    return MODE_LABELS[st.session_state.lang].get(mode, mode)


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
def load_amba_by_mode() -> pd.DataFrame:
    return _load_amba_by_mode(get_conn())


@st.cache_data(ttl=3600)
def load_top_empresas() -> pd.DataFrame:
    return _load_top_empresas(get_conn())


@st.cache_data(ttl=3600)
def load_by_provincia() -> pd.DataFrame:
    return _load_by_provincia(get_conn())


@st.cache_data(ttl=3600)
def load_its() -> pd.DataFrame:
    return _its_analysis(get_conn())


@st.cache_data(ttl=3600)
def run_stl_analysis(mode_key: str, period: int, lang: str):
    from analytics.time_series import decompose_series, detect_anomalies
    result = decompose_series(
        get_conn(),
        mode=None if mode_key == "ALL" else mode_key,
        period=period,
    )
    if not result:
        return None, None, None, None
    anomalies        = detect_anomalies(result["residual"], lang=lang)
    anom             = anomalies[anomalies["is_anomaly"]].copy()
    anom_explained   = anom[anom["event_label"].notna() & (anom["event_label"] != "")]
    anom_unexplained = anom[anom["event_label"].isna()  | (anom["event_label"] == "")]
    return result, anom, anom_explained, anom_unexplained


# ── Load data early (needed by sidebar and KPI cards) ──────────────────────
daily    = load_daily_totals()
max_date = daily["fecha"].max()

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
    st.divider()

    show_events = st.toggle(t("show_events"), value=True)

    st.divider()
    if st.button(t("refresh"), width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"{t('data_until')}: **{max_date.strftime('%d/%m/%Y')}**")
    st.divider()

    if st.session_state.lang == "es":
        st.markdown("**Fuentes de datos**")
        st.markdown("🗂️ [datos.transporte.gob.ar](https://datos.transporte.gob.ar)")
        st.markdown("**Repositorio**")
        st.markdown("💻 [GitHub — sanbestia/SUBE-transit-analytics](https://github.com/sanbestia/SUBE-transit-analytics)")
    else:
        st.markdown("**Data sources**")
        st.markdown("🗂️ [datos.transporte.gob.ar](https://datos.transporte.gob.ar)")
        st.markdown("**Repository**")
        st.markdown("💻 [GitHub — sanbestia/SUBE-transit-analytics](https://github.com/sanbestia/SUBE-transit-analytics)")


# ── Data loading ───────────────────────────────────────────────────────────
df_daily         = daily
monthly          = load_monthly()
combined_monthly = load_combined_monthly()
cmap             = {mode: MODE_COLORS[mode] for mode in DASHBOARD_MODES}


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

from dashboard.tabs.overview   import render as render_overview
from dashboard.tabs.covid      import render as render_covid
from dashboard.tabs.modal      import render as render_modal
from dashboard.tabs.resilience import render as render_resilience
from dashboard.tabs.its        import render as render_its
from dashboard.tabs.anomalies  import render as render_anomalies
from dashboard.tabs.forecast   import render as render_forecast

with tab_ov:
    render_overview(
        show_events=show_events,
        df_daily=df_daily,
        combined_monthly=combined_monthly,
        cmap=cmap,
        load_by_provincia=load_by_provincia,
        load_top_empresas=load_top_empresas,
        max_date=max_date,
    )

with tab_cv:
    render_covid(
        monthly=monthly,
        load_yoy=load_yoy,
        cmap=cmap,
    )

with tab_ms:
    render_modal(
        show_events=show_events,
        combined_monthly=combined_monthly,
        load_yoy=load_yoy,
        cmap=cmap,
    )

with tab_rs:
    render_resilience(
        show_events=show_events,
        load_amba_by_mode=load_amba_by_mode,
        cmap=cmap,
    )

with tab_its:
    render_its(
        load_its=load_its,
        build_counterfactual_df=build_counterfactual_df,
        ITS_TREATMENT=ITS_TREATMENT,
        cmap=cmap,
    )

with tab_an:
    render_anomalies(run_stl_analysis=run_stl_analysis)

with tab_fc:
    render_forecast(
        show_events=show_events,
        get_conn=get_conn,
        cmap=cmap,
    )


st.divider()
st.caption(
    "Fuente / Source: [datos.transporte.gob.ar](https://datos.transporte.gob.ar) · "
    "CC Attribution 4.0 · Actualización automática diaria / Daily auto-update"
)
