import plotly.graph_objects as go
import streamlit as st

from config import DASHBOARD_MODES
from dashboard.tabs.shared import t, mode_label, explainer, finding


def render(run_stl_analysis):

    finding("an_finding")

    st.divider()

    st.subheader(t("an_stl_title"))
    explainer("an_stl_explainer")

    col_l, col_r = st.columns(2)
    with col_l:
        stl_mode = st.selectbox(
            t("an_stl_mode"),
            ["ALL"] + list(DASHBOARD_MODES),
            format_func=lambda m: t("an_stl_all") if m == "ALL" else mode_label(m),
        )
    with col_r:
        stl_period = st.radio(
            t("an_stl_season"),
            [7, 365],
            format_func=lambda p: t("an_stl_weekly") if p == 7 else t("an_stl_annual"),
            horizontal=True,
        )

    try:
        with st.spinner(t("an_stl_running")):
            _stl_result, anom, _anom_explained, _anom_unexplained = run_stl_analysis(
                stl_mode, stl_period, st.session_state.lang
            )

        if _stl_result:
            fig11 = go.Figure()
            for key, name, color, fill in [
                ("original", t("an_stl_observed"), "rgba(100,100,200,0.25)", True),
                ("trend",    t("an_stl_trend"),    "#2563EB",                False),
                ("seasonal", t("an_stl_seasonal"), "#16A34A",                False),
                ("residual", t("an_stl_residual"), "#94a3b8",                False),
            ]:
                s = _stl_result[key]
                fig11.add_scatter(
                    x=s.index, y=s.values, name=name,
                    line_color=color,
                    fill="tozeroy" if fill else None,
                    opacity=0.8 if fill else 1.0,
                )
            if not _anom_explained.empty:
                fig11.add_scatter(
                    x=_anom_explained["fecha"], y=_anom_explained["residual"],
                    mode="markers", name=t("an_anom_explained"),
                    marker=dict(color="#C8A000", size=9, symbol="x"),
                    hovertemplate="<b>%{x}</b><br>%{y:,.0f}<br>%{text}",
                    text=_anom_explained["event_label"],
                )
            if not _anom_unexplained.empty:
                fig11.add_scatter(
                    x=_anom_unexplained["fecha"], y=_anom_unexplained["residual"],
                    mode="markers", name=t("an_anom_unexplained"),
                    marker=dict(color="red", size=9, symbol="x"),
                    hovertemplate="<b>%{x}</b><br>%{y:,.0f}",
                    text=_anom_unexplained["event_label"],
                )
            fig11.update_layout(
                height=660, template="plotly_white",
                hovermode="x unified",
                legend_title=t("an_stl_component"),
            )
            st.plotly_chart(fig11, width="stretch")

            if not anom.empty:
                @st.fragment
                def _anom_table_fragment(anom, _anom_unexplained):
                    st.subheader(f"🚨 {len(anom)} {t('an_anom_title')}")
                    st.caption(t("an_anom_explainer"))
                    _only_unexplained = st.checkbox(t("an_anom_only_unexplained"), value=False, key="anom_only_unexplained")
                    _anom_table = anom if not _only_unexplained else _anom_unexplained
                    st.dataframe(
                        _anom_table[["fecha", "z_score", "event_label"]]
                        .sort_values("z_score", key=abs, ascending=False)
                        .rename(columns={
                            "fecha":       t("an_anom_date"),
                            "z_score":     t("an_anom_z"),
                            "event_label": t("an_anom_event"),
                        }),
                        width="stretch",
                    )
                _anom_table_fragment(anom, _anom_unexplained)
    except ImportError:
        st.error("statsmodels not installed. Run: uv add statsmodels")
