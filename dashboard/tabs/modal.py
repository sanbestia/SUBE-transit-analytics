import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from config import DASHBOARD_MODES
from dashboard.tabs.shared import (
    t, mode_label, explainer,
    add_event_annotations, add_fare_annotations,
)


def render(show_events, combined_monthly, load_yoy, cmap):

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
        combined_monthly["modo"].isin(DASHBOARD_MODES)
    ].copy().sort_values(["modo", "month_start"])
    _ms_full["mom_pct"] = _ms_full.groupby("modo")["total_usos"].pct_change() * 100
    _ms_full            = _ms_full.dropna(subset=["mom_pct"])

    _active_modes = [m for m in DASHBOARD_MODES if m in _ms_full["modo"].unique()]

    if _active_modes:
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

        # Set y-axis range explicitly for each subplot axis
        _yaxis_updates = {f"yaxis{'' if i == 1 else i}": dict(range=[-50, 75], ticksuffix="%")
                          for i in range(1, _n + 1)}
        _fig_ms_sub.update_layout(
            height=_n * 300,
            template="plotly_white",
            hovermode="x unified",
            margin=dict(t=20, b=20),
            **_yaxis_updates,
        )
        if show_events:
            _fig_ms_sub = add_event_annotations(_fig_ms_sub, x_min="2016-01-01")
            _fig_ms_sub = add_fare_annotations(_fig_ms_sub, x_min="2016-01-01")
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
        combined_monthly["modo"].isin(DASHBOARD_MODES)
    ].copy()
    _share_totals = share_df.groupby("month_start")["total_usos"].sum().rename("month_total")
    share_df = share_df.join(_share_totals, on="month_start")
    share_df["mode_share_pct"] = (share_df["total_usos"] / share_df["month_total"] * 100).round(2)
    share_df["modo_label"] = share_df["modo"].map(mode_label)

    fig_ms2 = px.area(
        share_df, x="month_start", y="mode_share_pct",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in DASHBOARD_MODES},
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
        yoy_all["modo"].isin(DASHBOARD_MODES)
    ].dropna(subset=["yoy_pct_change"]).copy()
    yoy_all["modo_label"] = yoy_all["modo"].map(mode_label)

    fig_ms3 = px.bar(
        yoy_all, x="month_start", y="yoy_pct_change",
        color="modo_label", barmode="group",
        color_discrete_map={mode_label(m): cmap[m] for m in DASHBOARD_MODES},
        labels={"yoy_pct_change": t("cv_yoy_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    fig_ms3.add_hline(y=0, line_color="black", line_width=1)
    if show_events:
        fig_ms3 = add_event_annotations(fig_ms3)
        fig_ms3 = add_fare_annotations(fig_ms3)
    fig_ms3.update_layout(height=630, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig_ms3, width="stretch")
