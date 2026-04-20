import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import DASHBOARD_MODES
from dashboard.strings import STRINGS
from dashboard.tabs.shared import (
    t, mode_label, explainer,
    add_event_annotations,
)


def render(show_events, df_daily, combined_monthly, cmap, load_by_provincia, load_top_empresas, max_date):

    st.subheader(t("ov_series_title"))
    explainer("ov_series_explainer")

    # ── Per-chart selectors ────────────────────────────────────────────────
    _ov_min = pd.Timestamp("2016-02-01").date()
    _ov_max = max_date
    _ov_col1, _ov_col2, _ov_col3, _ov_col4, _ov_col5 = st.columns([1, 1, 1, 2, 2])
    with _ov_col1:
        ov_col  = st.checkbox(mode_label("COLECTIVO"), value=True, key="ov_col")
    with _ov_col2:
        ov_tren = st.checkbox(mode_label("TREN"),      value=True, key="ov_tren")
    with _ov_col3:
        ov_sub  = st.checkbox(mode_label("SUBTE"),     value=True, key="ov_sub")
    with _ov_col4:
        ov_start_date = st.date_input(t("desde"), value=_ov_min, min_value=_ov_min, max_value=_ov_max, key="ov_start")
    with _ov_col5:
        ov_end_date   = st.date_input(t("hasta"), value=_ov_max, min_value=_ov_min, max_value=_ov_max, key="ov_end")
    ov_modes = [m for m, on in [("COLECTIVO", ov_col), ("TREN", ov_tren), ("SUBTE", ov_sub)] if on]
    if not ov_modes:
        ov_modes = DASHBOARD_MODES
    ov_start = pd.Timestamp(ov_start_date)
    ov_end   = pd.Timestamp(ov_end_date)

    fig = go.Figure()

    # Pre-2020 monthly data — plotted as a line (same style as post-2020 MA)
    # using average daily trips (total_usos / days_in_month) to match the daily scale.
    # COLECTIVO extends to 2013; SUBTE/TREN only from 2016 (pre-2016 SUBE coverage incomplete).
    _pre2020 = combined_monthly[
        (combined_monthly["month_start"] >= ov_start) &
        (combined_monthly["month_start"] < "2020-01-01") &
        (combined_monthly["modo"].isin(ov_modes)) &
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
    for mode in ov_modes:
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

    # Post-2020 daily data — raw (faint) + 7-day MA, filtered by local selectors
    for mode in ov_modes:
        mode_df = df_daily[
            (df_daily["modo"] == mode) &
            (df_daily["fecha"] >= ov_start) &
            (df_daily["fecha"] <= ov_end) &
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
        for mode in ov_modes:
            _pre_mode = _pre2020[_pre2020["modo"] == mode].sort_values("month_start")
            if _pre_mode.empty:
                continue
            _last_pre = _pre_mode.iloc[-1]
            _gap_y0 = float(_last_pre["avg_daily"])
            _daily_jan2 = df_daily[
                (df_daily["modo"] == mode) &
                (df_daily["fecha"] == _gap_x1)
            ]
            if _daily_jan2.empty:
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

    if _has_gap_lines:
        _gap_x0 = pd.Timestamp("2019-10-01")
    if show_events:
        fig = add_event_annotations(fig)

    # Compute y max across pre-2020 data for bracket positioning
    if _has_pre2020:
        _y_max_pre = float(_pre2020["avg_daily"].max())
        _y_max_daily_pre_region = float(
            df_daily[df_daily["fecha"] < "2020-06-01"]["cantidad_usos"].max()
            if not df_daily.empty else _y_max_pre
        )
        _y_max = max(_y_max_pre, _y_max_daily_pre_region) if not pd.isna(_y_max_daily_pre_region) else _y_max_pre
        _y_bracket_main  = _y_max * 1.04
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
        xaxis=dict(range=[str(ov_start.date()), str(ov_end.date())]),
    )
    st.plotly_chart(fig, width="stretch")

    # ── Province histogram ────────────────────────────────────────────────
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

    # ── Heatmap ───────────────────────────────────────────────────────────
    st.subheader(t("an_heatmap_title"))
    explainer("an_heatmap_explainer")

    _hm_min = pd.Timestamp("2020-01-01").date()
    _hm_col1, _hm_col2, _hm_col3, _hm_col4, _hm_col5, _hm_col6 = st.columns([1, 1, 1, 2, 2, 2])
    with _hm_col1:
        hm_col  = st.checkbox(mode_label("COLECTIVO"), value=True, key="hm_col")
    with _hm_col2:
        hm_tren = st.checkbox(mode_label("TREN"),      value=True, key="hm_tren")
    with _hm_col3:
        hm_sub  = st.checkbox(mode_label("SUBTE"),     value=True, key="hm_sub")
    with _hm_col4:
        hm_start_date = st.date_input(t("desde"), value=_hm_min, min_value=_hm_min, max_value=max_date, key="hm_start")
    with _hm_col5:
        hm_end_date   = st.date_input(t("hasta"), value=max_date, min_value=_hm_min, max_value=max_date, key="hm_end")
    with _hm_col6:
        hm_excl_lockdown = st.checkbox(t("ov_excl_lockdown"), value=True, key="hm_excl")
    hm_modes = [m for m, on in [("COLECTIVO", hm_col), ("TREN", hm_tren), ("SUBTE", hm_sub)] if on]
    if not hm_modes:
        hm_modes = DASHBOARD_MODES
    hm_start = pd.Timestamp(hm_start_date)
    hm_end   = pd.Timestamp(hm_end_date)

    _hm_daily = df_daily[
        (df_daily["modo"].isin(hm_modes)) &
        (df_daily["fecha"] >= hm_start) &
        (df_daily["fecha"] <= hm_end)
    ].copy()
    if hm_excl_lockdown:
        _hm_daily = _hm_daily[~(
            (_hm_daily["fecha"] >= pd.Timestamp("2020-03-01")) &
            (_hm_daily["fecha"] <= pd.Timestamp("2021-07-31"))
        )]
    if not _hm_daily.empty:
        _hm_agg = (
            _hm_daily.groupby(["fecha", "day_of_week", "month"])["cantidad_usos"].sum().reset_index()
        )
        pivot = (
            _hm_agg.groupby(["day_of_week", "month"])["cantidad_usos"]
            .mean()
            .reset_index()
            .pivot(index="day_of_week", columns="month", values="cantidad_usos")
        )
        pivot.index   = STRINGS[st.session_state.lang]["days"]
        pivot.columns = [STRINGS[st.session_state.lang]["months"][c - 1] for c in pivot.columns]
        fig_heat = px.imshow(
            pivot,
            color_continuous_scale="Blues",
            labels={"color": t("an_heatmap_color")},
            template="plotly_white",
            aspect="auto",
        )
        fig_heat.update_layout(height=320)
        st.plotly_chart(fig_heat, width="stretch")

    # ── Top operators ─────────────────────────────────────────────────────
    st.subheader(t("ov_empresas_title"))
    explainer("ov_empresas_explainer")

    empresas = load_top_empresas()
    empresas = empresas[empresas["modo"].isin(DASHBOARD_MODES)].copy()
    empresas["modo_label"]    = empresas["modo"].map(mode_label)
    empresas["empresa_short"] = empresas["nombre_empresa"].str[:35]

    fig3 = px.bar(
        empresas.sort_values("total_usos"),
        x="total_usos", y="empresa_short",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in DASHBOARD_MODES},
        orientation="h",
        labels={"total_usos": t("ov_empresas_y"), "empresa_short": t("ov_empresas_x"), "modo_label": ""},
        template="plotly_white",
    )
    fig3.update_layout(height=420)
    st.plotly_chart(fig3, width="stretch")
