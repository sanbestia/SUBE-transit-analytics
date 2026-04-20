"""
dashboard/tabs/anomalies.py — STL decomposition and anomaly detection tab.

Method: STL (Seasonal-Trend decomposition using LOESS) decomposes the daily
ridership series into three additive components:

    y(t) = T(t) + S(t) + R(t)

    T(t) — Trend:    slow-moving baseline (extracted via LOESS smoother)
    S(t) — Seasonal: repeating calendar pattern (period=7 weekly OR period=365 yearly)
    R(t) — Residual: what's left after removing trend and seasonality

Anomalies are flagged where the residual exceeds a z-score threshold of 3.0
standard deviations from the residual mean. Known historical events and
Argentine national holidays are cross-referenced against the anomaly dates
to distinguish "explained" spikes (lockdowns, strikes, fare hikes) from
"unexplained" ones that may warrant further investigation.

The heavy STL computation is cached inside app.py via @st.cache_data so that
switching between "explained only" / "all anomalies" view doesn't re-run
the decomposition — only the table filter changes.
"""

import plotly.graph_objects as go
import streamlit as st

from config import DASHBOARD_MODES
from dashboard.tabs.shared import t, mode_label, explainer, finding


def render(run_stl_analysis):
    """
    Render the Anomalies tab.

    Args:
        run_stl_analysis: cached callable from app.py — signature:
            run_stl_analysis(mode_key, period, lang)
            → (result_dict, anom_df, anom_explained_df, anom_unexplained_df)

            result_dict keys: "original", "trend", "seasonal", "residual"
                — each a pd.Series with a DatetimeIndex.
            anom_df: all flagged anomaly dates (|z| > 3.0).
            anom_explained: subset where event_label is non-empty
                (matched to a known event or holiday).
            anom_unexplained: subset where event_label is empty
                (no matching event found — statistically unusual and unexplained).
    """

    finding("an_finding")

    st.divider()

    st.subheader(t("an_stl_title"))
    explainer("an_stl_explainer")

    # ── Mode and period selectors ──────────────────────────────────────────
    # "ALL" aggregates across COLECTIVO + TREN + SUBTE before decomposing,
    # giving a system-wide view. Selecting a single mode shows mode-specific
    # seasonal patterns (e.g. SUBTE drops sharply on weekends and holidays).
    col_l, col_r = st.columns(2)
    with col_l:
        stl_mode = st.selectbox(
            t("an_stl_mode"),
            ["ALL"] + list(DASHBOARD_MODES),
            format_func=lambda m: t("an_stl_all") if m == "ALL" else mode_label(m),
        )
    with col_r:
        # period=7: captures the weekly cycle (Mon-Fri peaks, weekend dips).
        # period=365: captures the annual cycle (summer/winter school breaks,
        #             March-April commuting peak, January holiday dip).
        # Choosing the wrong period leaves its signal in the residual and
        # inflates the anomaly count.
        stl_period = st.radio(
            t("an_stl_season"),
            [7, 365],
            format_func=lambda p: t("an_stl_weekly") if p == 7 else t("an_stl_annual"),
            horizontal=True,
        )

    try:
        # run_stl_analysis is @st.cache_data in app.py.
        # lang is part of the cache key so that Spanish/English label sets
        # are stored separately — switching language gets the right labels
        # without re-running the decomposition.
        with st.spinner(t("an_stl_running")):
            _stl_result, anom, _anom_explained, _anom_unexplained = run_stl_analysis(
                stl_mode, stl_period, st.session_state.lang
            )

        if _stl_result:
            # ── Decomposition chart ────────────────────────────────────────
            # Overlay all four components on a single figure so the reader
            # can see at a glance how much of each spike is trend vs seasonal
            # vs residual. The "original" series is filled to zero to give it
            # visual weight as the raw data; the others are thin lines.
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
                    # fill="tozeroy" draws a shaded area between the line and y=0,
                    # used only on the "original" series to distinguish it visually.
                    fill="tozeroy" if fill else None,
                    opacity=0.8 if fill else 1.0,
                )

            # ── Anomaly markers ────────────────────────────────────────────
            # Explained anomalies (gold ×): residual spikes that match a known
            # event or holiday — e.g. the March 2020 lockdown or a general strike.
            # These are expected; they confirm the model is working correctly.
            if not _anom_explained.empty:
                fig11.add_scatter(
                    x=_anom_explained["fecha"], y=_anom_explained["residual"],
                    mode="markers", name=t("an_anom_explained"),
                    marker=dict(color="#C8A000", size=9, symbol="x"),
                    # %{text} resolves to the event_label column — the name of the
                    # matched event or holiday displayed on hover.
                    hovertemplate="<b>%{x}</b><br>%{y:,.0f}<br>%{text}",
                    text=_anom_explained["event_label"],
                )

            # Unexplained anomalies (red ×): statistically extreme residuals with
            # no matching event — these are the interesting ones worth investigating.
            # Could indicate data quality issues, unreported service disruptions,
            # or genuine demand shocks not catalogued in events.yaml.
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

            # ── Anomaly table ──────────────────────────────────────────────
            if not anom.empty:
                # @st.fragment isolates this block so that toggling the
                # "show only unexplained" checkbox only re-renders the table
                # — not the expensive STL chart above it. Without the fragment,
                # any widget interaction in this tab triggers a full page rerun.
                @st.fragment
                def _anom_table_fragment(anom, _anom_unexplained):
                    st.subheader(f"🚨 {len(anom)} {t('an_anom_title')}")
                    st.caption(t("an_anom_explainer"))

                    # Checkbox toggling between the full anomaly set (explained +
                    # unexplained) and the unexplained-only subset.
                    _only_unexplained = st.checkbox(
                        t("an_anom_only_unexplained"), value=False,
                        key="anom_only_unexplained",
                    )
                    _anom_table = anom if not _only_unexplained else _anom_unexplained

                    # Sort by absolute z-score descending so the most extreme
                    # statistical outliers appear first regardless of sign.
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
        # statsmodels is optional — STL requires it. The rest of the dashboard
        # works without it; only this tab is affected.
        st.error("statsmodels not installed. Run: uv add statsmodels")
