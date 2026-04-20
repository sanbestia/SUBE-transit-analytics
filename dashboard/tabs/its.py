import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from config import DASHBOARD_MODES, MODE_COLORS
from dashboard.tabs.shared import t, mode_label, explainer, finding


def render(load_its, build_counterfactual_df, ITS_TREATMENT, cmap):

    st.subheader(t("rs_its_title"))
    explainer("rs_its_explainer")
    finding("rs_its_finding")

    its_df = load_its()

    if not its_df.empty:
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
            _cfdf = build_counterfactual_df(_row, ITS_TREATMENT)
            _pre  = _cfdf[_cfdf["post"] == 0]
            _post = _cfdf[_cfdf["post"] == 1]
            _col  = MODE_COLORS[_its_mode]
            _show_legend = (_i == 1)

            _fig_its.add_trace(go.Scatter(
                x=_pre["ds"], y=_pre["actual"],
                mode="lines", name=t("rs_its_actual"),
                line=dict(color=_col, width=1.5),
                opacity=0.35,
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            _fig_its.add_trace(go.Scatter(
                x=_pre["ds"], y=_pre["fitted"],
                mode="lines", name="Fitted (pre)",
                line=dict(color=_col, width=1.8, dash="dot"),
                opacity=0.65,
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["actual"],
                mode="lines+markers", name=t("rs_its_actual"),
                line=dict(color=_col, width=2.5),
                marker=dict(size=5),
                showlegend=False,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

            _fig_its.add_trace(go.Scatter(
                x=_post["ds"], y=_post["counterfactual"],
                mode="lines", name=t("rs_its_cf"),
                line=dict(color="#9CA3AF", width=2, dash="dash"),
                showlegend=_show_legend,
                hovertemplate="%{x|%b %Y}: %{y:,.0f}<extra></extra>",
            ), row=_i, col=1)

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

        _fig_its.add_vline(
            x=ITS_TREATMENT.timestamp() * 1000,
            line_dash="dash", line_color="#EF4444", line_width=1.5,
        )
        _fig_its.add_annotation(
            x=ITS_TREATMENT, y=1, yref="paper",
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
        _its_cfdfs = {
            m: build_counterfactual_df(
                its_df[its_df["mode"] == m].iloc[0], ITS_TREATMENT
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

            _cum_gap   = float((_post_cf["counterfactual"] - _post_cf["actual"]).sum())
            _cum_label = f"{abs(_cum_gap)/1e6:.0f}M"
            _cum_delta = t("rs_its_metric_lost_sub")

            _latest    = _post_cf.iloc[-1]
            _gap_pct   = (_latest["counterfactual"] - _latest["actual"]) / _latest["counterfactual"] * 100
            _now_label = f"{abs(_gap_pct):.1f}% {'below' if _gap_pct > 0 else 'above'}"
            if st.session_state.lang == "es":
                _now_label = f"{abs(_gap_pct):.1f}% {'por debajo' if _gap_pct > 0 else 'por encima'}"
            _now_delta = t("rs_its_metric_now_sub")

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
