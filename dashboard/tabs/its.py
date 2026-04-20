"""
dashboard/tabs/its.py — Interrupted Time Series (ITS) causal analysis tab.

Method: segmented OLS regression (analytics/causal.py) estimating the causal
impact of the January 2024 SUBE fare shock on monthly ridership.

Model equation:

    y_t = β₀ + β₁·t + β₂·D_t + β₃·t_post_t + Σγ_m·Month_m + δ·COVID_t + ε_t

    β₀            — intercept: baseline ridership at t=0
    β₁            — pre-treatment trend: monthly ridership change before the shock
    β₂  (D_t)     — level shift: immediate step change in ridership at treatment date
                    (D_t = 1 for all months ≥ Jan 2024, else 0)
    β₃  (t_post)  — slope change: monthly drift in trend AFTER the shock
                    (t_post = 0 before treatment; 0 at Jan 2024, 1, 2, … after)
    γ_m (Month_m) — month-of-year fixed effects controlling for seasonality
                    (11 dummies, January is the reference category)
    δ   (COVID_t) — COVID-era effect controlling for the 2020-03 → 2021-12 collapse
    ε_t           — error term

Treatment date: January 2024
    Jan 15 2024: +45% fare hike | Feb 6 2024: +66% | cumulative Jan–Feb ≈ +141%

Standard errors:
    COLECTIVO : OLS (Ljung-Box test found no residual autocorrelation, p=0.136)
    TREN/SUBTE: HAC Newey-West with maxlags=12 (autocorrelation detected)

Counterfactual: the pre-treatment trend projected forward by setting D=0 and
t_post=0 for all months — i.e. what ridership would have been had the fare
shock never happened (holding seasonality and COVID constant).
    gap = counterfactual − actual  (positive → fewer trips than expected)

Limitation: β₂ conflates the fare price effect with the broader Dec 2023
devaluation (+118%) and real-income collapse. The implied demand elasticity
is an upper bound on the pure price elasticity of demand.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from config import DASHBOARD_MODES, MODE_COLORS
from dashboard.tabs.shared import t, mode_label, explainer, finding


def render(load_its, build_counterfactual_df, ITS_TREATMENT, cmap):
    """
    Render the Fare Impact (ITS) tab.

    Args:
        load_its             : cached callable — calls its_analysis(conn) and
                               returns a DataFrame with one row per mode.
                               Each row contains the OLS coefficients, standard
                               errors, p-values, and the raw _df / _result objects
                               needed to build the counterfactual.
        build_counterfactual_df: function(row, treatment_date) → DataFrame with:
                               ds, actual, fitted, counterfactual, gap, post
                               where post=1 for months ≥ treatment date.
        ITS_TREATMENT        : pd.Timestamp — the treatment date (Jan 2024).
        cmap                 : dict[mode_key → hex_color].
    """

    st.subheader(t("rs_its_title"))
    explainer("rs_its_explainer")
    finding("rs_its_finding")

    # its_df has one row per mode with all regression outputs.
    # Columns include: mode, beta_level, beta_slope, pvalue_level, pvalue_slope,
    # pre_mean, pct_level_change, implied_elasticity, _df, _result
    its_df = load_its()

    if not its_df.empty:
        # Only plot modes for which ITS converged (n_post ≥ 6 required).
        _its_modes = [m for m in DASHBOARD_MODES if m in its_df["mode"].values]
        _n_its     = len(_its_modes)

        # One subplot per mode on a shared x-axis so the treatment line
        # (vline below) visually spans all panels simultaneously.
        _fig_its = make_subplots(
            rows=_n_its, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            row_titles=[mode_label(m) for m in _its_modes],
        )

        for _i, _its_mode in enumerate(_its_modes, start=1):
            _row  = its_df[its_df["mode"] == _its_mode].iloc[0]

            # build_counterfactual_df sets D=0 and t_post=0 for all rows,
            # then predicts from the fitted OLS — giving the counterfactual
            # trajectory as if the fare shock had never happened.
            _cfdf = build_counterfactual_df(_row, ITS_TREATMENT)

            # Split into pre- and post-treatment segments for visual distinction.
            # pre: used to show the model was well-calibrated before the shock.
            # post: where the counterfactual and actual diverge.
            _pre  = _cfdf[_cfdf["post"] == 0]
            _post = _cfdf[_cfdf["post"] == 1]
            _col  = MODE_COLORS[_its_mode]

            # Show the legend only on the first subplot to avoid repetition —
            # all panels share the same legend entries (actual, fitted, cf, gap).
            _show_legend = (_i == 1)

            # ── Pre-treatment: faint actual ────────────────────────────────
            # The raw ridership series before January 2024, plotted faintly
            # (opacity=0.35) to provide context without competing with the
            # post-treatment data where the impact is visible.
            _fig_its.add_trace(go.Scatter(
                x=_pre["ds"], y=_pre["actual"],
                mode="lines", name=t("rs_its_actual"),
                line=dict(color=_col, width=1.5),
                opacity=0.35,
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            # ── Pre-treatment: OLS fitted line ─────────────────────────────
            # The model's in-sample fit before the treatment date — the dotted
            # line shows how well the pre-shock trend was captured. If fitted
            # tracks actual closely, the pre-treatment model is trustworthy
            # and so is the counterfactual projection.
            _fig_its.add_trace(go.Scatter(
                x=_pre["ds"], y=_pre["fitted"],
                mode="lines", name="Fitted (pre)",
                line=dict(color=_col, width=1.8, dash="dot"),
                opacity=0.65,
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            # ── Post-treatment: actual ridership ───────────────────────────
            # The observed ridership after January 2024 — plotted as a solid
            # line with markers so individual months are visible. The gap
            # between this and the counterfactual below represents the estimated
            # causal impact of the fare shock.
            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["actual"],
                mode="lines+markers", name=t("rs_its_actual"),
                line=dict(color=_col, width=2.5),
                marker=dict(size=5),
                showlegend=False,    # legend entry already added in pre-treatment trace
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            # ── Post-treatment: counterfactual ─────────────────────────────
            # The pre-treatment OLS trend projected forward (D=0, t_post=0),
            # representing the ridership that would have been observed without
            # the fare shock. The distance between this grey dashed line and
            # the actual ridership is the estimated causal impact (β₂ + β₃·t).
            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["counterfactual"],
                mode="lines", name=t("rs_its_cf"),
                line=dict(color="#9CA3AF", width=2, dash="dash"),
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            # ── Shaded gap (counterfactual − actual) ───────────────────────
            # Plotly's fill="tonexty" shades the area between two consecutive
            # traces on the same axes. The trick requires two traces plotted
            # in the right order: (1) bottom boundary (counterfactual, mode="none"
            # so it's invisible), then (2) top boundary (actual) with
            # fill="tonexty" — Plotly fills from trace (2) down to trace (1).
            # Here the counterfactual is ABOVE actual (ridership fell), so the
            # red shading covers the gap where trips were "lost".
            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["counterfactual"],
                mode="none",     # invisible — only serves as the bottom boundary for fill
                showlegend=False,
                hoverinfo="skip",
            ), row=_i, col=1)
            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["actual"],
                mode="none", name=t("rs_its_gap"),
                fill="tonexty",                        # fills from this trace down to the previous
                fillcolor="rgba(239,68,68,0.15)",      # semi-transparent red
                line=dict(width=0),
                showlegend=_show_legend,
                hoverinfo="skip",
            ), row=_i, col=1)

        # ── Treatment date line ────────────────────────────────────────────
        # A single add_vline() on a make_subplots figure spans ALL subplots
        # automatically (shared x-axis). Plotly's vline x-value for datetime
        # axes must be a Unix timestamp in milliseconds.
        _fig_its.add_vline(
            x=ITS_TREATMENT.timestamp() * 1000,
            line_dash="dash", line_color="#EF4444", line_width=1.5,
        )
        # The annotation is pinned to the top of the figure (yref="paper", y=1)
        # so it appears above the first subplot regardless of the y-axis scale.
        _fig_its.add_annotation(
            x=ITS_TREATMENT, y=1, yref="paper",
            text=f"<b>{t('rs_its_treatment')}</b>",
            showarrow=False, xanchor="left", yanchor="top",
            font=dict(size=10, color="#EF4444"),
            bgcolor="white", borderpad=2,
        )

        # Apply tickformat to every y-axis (one per subplot).
        # Plotly names them "yaxis", "yaxis2", "yaxis3", … so we build the
        # dict programmatically rather than hard-coding a fixed number of axes.
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

        # ── Plain-language metrics ─────────────────────────────────────────
        # The chart shows the shape of the impact; this table translates the
        # regression coefficients into three actionable numbers per mode:
        #
        #   Trips lost     : ∑(counterfactual − actual) over all post months
        #                    — the cumulative ridership deficit since Jan 2024.
        #   Latest gap     : (cf − actual) / cf at the most recent month
        #                    — how far below the counterfactual ridership is RIGHT NOW.
        #   Drift          : β₃ (pvalue_slope) — whether ridership is still falling
        #                    month-over-month post-shock, flat, or recovering.
        #                    Only reported if statistically significant (p < 0.05).

        # Pre-compute counterfactual DataFrames for the metrics below.
        # These were already built for the chart but are not stored on _cfdf;
        # we rebuild them here (fast — no re-fitting, just matrix multiply).
        _its_cfdfs = {
            m: build_counterfactual_df(
                its_df[its_df["mode"] == m].iloc[0], ITS_TREATMENT
            )
            for m in _its_modes
        }

        # Header row — aligned to the four metric columns.
        _header_cols = st.columns([2, 2, 2, 2])
        _header_cols[0].markdown("&nbsp;")
        _header_cols[1].markdown(f"**{t('rs_its_metric_lost')}**")
        _header_cols[2].markdown(f"**{t('rs_its_metric_now')}**")
        _header_cols[3].markdown(f"**{t('rs_its_metric_drift')}**")

        for _its_mode in _its_modes:
            _row     = its_df[its_df["mode"] == _its_mode].iloc[0]
            _cfdf    = _its_cfdfs[_its_mode]
            _post_cf = _cfdf[_cfdf["post"] == 1]

            # ── Metric 1: cumulative trips lost ────────────────────────────
            # Sum of the gap (counterfactual − actual) over all post-treatment
            # months. A positive value means fewer trips were taken than the
            # model predicts would have occurred without the shock.
            _cum_gap   = float((_post_cf["counterfactual"] - _post_cf["actual"]).sum())
            _cum_label = f"{abs(_cum_gap)/1e6:.0f}M"
            _cum_delta = t("rs_its_metric_lost_sub")

            # ── Metric 2: latest-month gap as % ───────────────────────────
            # At the most recent observed month:
            #   gap% = (counterfactual − actual) / counterfactual × 100
            # A positive gap% means ridership is below counterfactual (lost trips).
            # A negative gap% (recovery) would mean ridership exceeded the projection.
            _latest    = _post_cf.iloc[-1]
            _gap_pct   = (
                (_latest["counterfactual"] - _latest["actual"])
                / _latest["counterfactual"] * 100
            )
            _now_label = f"{abs(_gap_pct):.1f}% {'below' if _gap_pct > 0 else 'above'}"
            if st.session_state.lang == "es":
                _now_label = f"{abs(_gap_pct):.1f}% {'por debajo' if _gap_pct > 0 else 'por encima'}"
            _now_delta = t("rs_its_metric_now_sub")

            # ── Metric 3: post-shock drift (β₃) ───────────────────────────
            # beta_slope is the OLS coefficient on t_post: the estimated monthly
            # change in ridership AFTER the shock, above/below the pre-shock trend.
            # A negative β₃ means ridership continues to fall month-over-month
            # relative to the counterfactual (accelerating impact).
            # A positive β₃ means some recovery is underway.
            # If pvalue_slope ≥ 0.05 we report "flat" — the trend change is not
            # statistically distinguishable from zero at the 5% level.
            _slope     = _row["beta_slope"]
            _sig_slope = _row["pvalue_slope"] < 0.05
            if not _sig_slope:
                _drift_label = t("rs_its_drift_flat")
                _drift_delta = ""
            elif _slope < 0:
                # Falling: ridership losing abs(β₃)/1e6 million trips per month
                _drift_label = t("rs_its_drift_falling").format(n=f"{abs(_slope)/1e6:.2f}")
                _drift_delta = ""
            else:
                # Rising: recovering abs(β₃)/1e6 million trips per month
                _drift_label = t("rs_its_drift_rising").format(n=f"{abs(_slope)/1e6:.2f}")
                _drift_delta = ""

            _cols = st.columns([2, 2, 2, 2])
            _cols[0].markdown(f"**{mode_label(_its_mode)}**")
            _cols[1].metric("", _cum_label, _cum_delta, delta_color="off")
            _cols[2].metric("", _now_label, _now_delta, delta_color="off")
            _cols[3].metric("", _drift_label, _drift_delta or None, delta_color="off")

        # Caption flags the key limitation: β₂ is an upper bound on the pure
        # fare elasticity because it conflates price with the Dec 2023 macro shock.
        st.caption(t("rs_its_note"))
