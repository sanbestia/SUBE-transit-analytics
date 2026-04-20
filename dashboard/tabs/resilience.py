import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import DASHBOARD_MODES
from dashboard.strings import STRINGS
from dashboard.tabs.shared import (
    t, mode_label, explainer, finding,
    add_event_annotations, add_fare_annotations,
)


def render(show_events, load_amba_by_mode, cmap):

    lang        = st.session_state.lang
    amba_labels = STRINGS[lang]["amba_labels"]
    amba_colors = {"SI": "#2563EB", "NO": "#F59E0B"}

    finding("rs_finding")

    # ── Mode selector shared by both AMBA charts ───────────────────────────
    _rs_col1, _rs_col2, _rs_col3 = st.columns([1, 1, 1])
    with _rs_col1:
        rs_col  = st.checkbox(mode_label("COLECTIVO"), value=True, key="rs_col")
    with _rs_col2:
        rs_tren = st.checkbox(mode_label("TREN"),      value=True, key="rs_tren")
    with _rs_col3:
        rs_sub  = st.checkbox(mode_label("SUBTE"),     value=True, key="rs_sub")
    rs_modes = [m for m, on in [("COLECTIVO", rs_col), ("TREN", rs_tren), ("SUBTE", rs_sub)] if on]
    if not rs_modes:
        rs_modes = DASHBOARD_MODES

    _amba_raw = load_amba_by_mode()
    _amba_raw["month_start"] = pd.to_datetime(_amba_raw["month_start"])
    _amba_filt = _amba_raw[_amba_raw["modo"].isin(rs_modes)]
    amba_agg = (
        _amba_filt.groupby(["month_start", "amba"])["total"]
        .sum()
        .reset_index()
    )
    _jan2020 = amba_agg[amba_agg["month_start"] == "2020-01-01"].set_index("amba")["total"]
    amba_agg["recovery_index"] = amba_agg.apply(
        lambda r: round(100.0 * r["total"] / _jan2020[r["amba"]], 1)
        if r["amba"] in _jan2020.index and _jan2020[r["amba"]] > 0 else float("nan"),
        axis=1,
    )
    amba_plot           = amba_agg.copy()
    amba_plot["region"] = amba_plot["amba"].map(amba_labels)

    # Dual y-axis AMBA vs Interior chart
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

    _rolling_label = "Promedio móvil 12 meses" if lang == "es" else "12-month rolling avg"
    for _amba_key in amba_labels:
        _full_region = amba_plot[amba_plot["amba"] == _amba_key].sort_values("month_start").copy()
        _full_region["rolling_12"] = _full_region["recovery_index"].rolling(12, min_periods=6).mean()
        _full_region[_full_region["month_start"] >= "2023-01-01"]

    fig8 = px.line(
        milei_df, x="month_start", y="recovery_index",
        color="region",
        color_discrete_map={v: amba_colors[k] for k, v in amba_labels.items()},
        markers=True,
        labels={"recovery_index": "Índice (Ene 2020 = 100)" if lang == "es" else "Index (Jan 2020 = 100)",
                "month_start": "", "region": ""},
        template="plotly_white",
    )

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
