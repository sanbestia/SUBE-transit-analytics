import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import DASHBOARD_MODES, MODE_COLORS
from dashboard.tabs.shared import t, mode_label, explainer, finding, add_event_annotations


def render(monthly, load_yoy, cmap):

    finding("cv_finding")

    covid_monthly = monthly[
        (monthly["month_start"] >= "2020-01-01") &
        (monthly["month_start"] <= "2022-07-01") &
        (monthly["modo"].isin(DASHBOARD_MODES))
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
            color_discrete_map={mode_label(m): cmap[m] for m in DASHBOARD_MODES},
            markers=True,
            labels={"index_val": _idx_label, "month_start": "", "modo_label": ""},
            template="plotly_white",
        )
        fig_norm.add_hline(y=100, line_dash="dash", line_color="grey", opacity=0.4,
                           annotation_text="Ene 2020 = 100" if st.session_state.lang == "es" else "Jan 2020 = 100")
        fig_norm = add_event_annotations(fig_norm)
        _drop_map = {
            "COLECTIVO": ("−58%", "Bus −58%"),
            "TREN":      ("−87%", "Train −87%"),
            "SUBTE":     ("−92%", "Subway −92%"),
        }
        _apr2020 = pd.Timestamp("2020-04-01")
        for _mode in DASHBOARD_MODES:
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

    st.subheader(t("cv_subst_recovery_title"))
    explainer("cv_subst_recovery_explainer")

    recovery_monthly = monthly[
        (monthly["month_start"] >= "2020-04-01") &
        (monthly["month_start"] <= "2022-07-01") &
        (monthly["modo"].isin(DASHBOARD_MODES))
    ].copy().sort_values(["modo", "month_start"])
    recovery_monthly["modo_label"] = recovery_monthly["modo"].map(mode_label)

    _rec_base = (recovery_monthly[recovery_monthly["month_start"] == "2020-04-01"]
                 .set_index("modo")["total_usos"])
    recovery_monthly["index_val"] = recovery_monthly.apply(
        lambda r: (r["total_usos"] / _rec_base[r["modo"]]) * 100
        if r["modo"] in _rec_base.index else float("nan"), axis=1,
    )
    recovery_monthly = recovery_monthly.dropna(subset=["index_val"])

    if not recovery_monthly.empty:
        _rec_label = ("Índice (abr 2020 = 100)" if st.session_state.lang == "es"
                      else "Index (Apr 2020 = 100)")
        fig_rec = px.line(
            recovery_monthly, x="month_start", y="index_val",
            color="modo_label",
            color_discrete_map={mode_label(m): cmap[m] for m in DASHBOARD_MODES},
            markers=True,
            labels={"index_val": _rec_label, "month_start": "", "modo_label": ""},
            template="plotly_white",
        )
        fig_rec.add_hline(y=100, line_dash="dash", line_color="grey", opacity=0.4,
                          annotation_text="Abr 2020 = 100" if st.session_state.lang == "es" else "Apr 2020 = 100")
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
        (yoy["modo"].isin(DASHBOARD_MODES))
    ].dropna(subset=["yoy_pct_change"]).copy()
    yoy_covid["modo_label"] = yoy_covid["modo"].map(mode_label)

    fig6 = px.bar(
        yoy_covid, x="month_start", y="yoy_pct_change",
        color="modo_label", barmode="group",
        color_discrete_map={mode_label(m): cmap[m] for m in DASHBOARD_MODES},
        labels={"yoy_pct_change": t("cv_yoy_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    fig6.add_hline(y=0, line_color="black", line_width=1)
    fig6.update_layout(height=600, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig6, width="stretch")
