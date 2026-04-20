"""
dashboard/tabs/forecast.py — Prophet demand forecast tab.

Model: Meta's Prophet (Taylor & Letham, 2018) — a decomposable time series
model that fits an additive (or multiplicative) combination of:

    y(t) = g(t) + s(t) + h(t) + r(t) + ε_t

    g(t) — piecewise linear trend with explicit changepoints at every
            fare hike and macro event date
    s(t) — Fourier-series yearly seasonality (Argentine ridership has a
            strong March–April peak and January/July school-break dip)
    h(t) — Argentine public holiday effects (from Prophet's built-in country
            holiday model — 134 holidays registered for 2020–2026)
    r(t) — four external regressors (described below)
    ε_t  — noise

External regressors added to Prophet:
    covid_impact       : binary 1 for 2020-03 → 2021-12. Lets Prophet fit
                         the COVID collapse explicitly instead of absorbing it
                         into the trend or seasonality. Without it, the post-2021
                         trend would underestimate the true recovery.
    fare_pressure      : cumulative sum of all fare hike magnitudes (%) up to
                         each month. Encodes accumulated fare burden as a
                         monotonically increasing series. Standardized before
                         fitting so the coefficient is in standard-deviation units.
    macro_shock        : binary 1 from Dec 2023 onward, marking the Milei
                         regime change (+118% devaluation and subsidy cuts).
                         Captures purchasing-power collapse independently of
                         the fare level already in fare_pressure.
    recovery_momentum  : log(1 + months_since_Jan_2022) for dates ≥ Jan 2022,
                         0 before. Captures the decelerating post-COVID recovery:
                         fast initial rebound slowing toward a new equilibrium.
                         The log shape prevents the trend from treating the
                         deceleration as a structural downward shift.

Outputs:
    yhat        — point forecast (posterior median)
    yhat_lower  — lower bound of the 80% prediction interval
    yhat_upper  — upper bound of the 80% prediction interval

    The CI widens with horizon because changepoint_prior_scale > 0 introduces
    uncertainty in the trend direction that compounds over time. Modes with
    higher changepoint_prior_scale (SUBTE=0.20) show wider intervals than
    more stable modes (COLECTIVO=0.05).

Training windows:
    COLECTIVO : 2013-01 → present  (SUBE fully integrated from the start)
    TREN      : 2016-01 → present  (SUBE integration mature; pre-2016 undercounts)
    SUBTE     : 2016-01 → present  (same as TREN)

Forecast outputs are clipped to floor = 50% of historical minimum to prevent
the linear trend from extrapolating to zero or negative values.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import DASHBOARD_MODES
from dashboard.utils import hex_to_rgb
from dashboard.tabs.shared import (
    t, mode_label, explainer,
    add_event_annotations, add_fare_annotations,
)


def render(show_events, get_conn, cmap):
    """
    Render the Forecast tab.

    Args:
        show_events : bool — sidebar toggle controlling event/fare annotations.
        get_conn    : cached callable from app.py — returns the live DuckDB
                      connection. Passed here (rather than the connection object
                      itself) so that forecast_ridership() can call it inside
                      the @st.cache_data context and Prophet can be imported
                      lazily (it's slow to import).
        cmap        : dict[mode_key → hex_color] for consistent mode colours.
    """

    explainer("fc_explainer")

    # ── Horizon selector ───────────────────────────────────────────────────
    # Users can pick 3–24 months ahead. Longer horizons produce wider CIs
    # because trend uncertainty accumulates with each month beyond the last
    # observed data point. The slider triggers a full Prophet re-fit because
    # make_future_dataframe() depends on the horizon.
    horizon = st.select_slider(
        t("fc_horizon"),
        options=[3, 6, 9, 12, 15, 18, 21, 24],
        value=12,
    )

    with st.spinner(t("fc_running")):
        try:
            # Lazy import — Prophet and cmdstanpy take ~2s to import and
            # compile the Stan model. Keeping the import inside the function
            # avoids delaying the rest of the dashboard on every page load.
            from analytics.ml import forecast_ridership, forecast_summary
            conn = get_conn()

            # forecast_ridership() fits one Prophet model per mode and returns
            # a dict[mode → DataFrame]. Each DataFrame has columns:
            #   ds, yhat, yhat_lower, yhat_upper, trend, additive_terms,
            #   is_forecast (True for future rows), actual (NaN for future rows)
            forecasts = forecast_ridership(
                conn,
                modes=list(DASHBOARD_MODES),
                horizon=horizon,
            )

            if not forecasts:
                st.warning(
                    "No forecast results. Check that the pipeline has run and data is loaded."
                )
            else:
                st.subheader(t("fc_title").format(n=horizon))

                for mode, fc in forecasts.items():
                    # Split the DataFrame into historical and forecast rows.
                    # hist: rows up to and including the last observed month —
                    #       yhat here is the in-sample fitted value, not a prediction.
                    # pred: future rows where yhat is the actual forecast.
                    hist = fc[~fc["is_forecast"]]
                    pred = fc[fc["is_forecast"]]

                    # Convert the mode's hex color to 0-1 RGB floats so we can
                    # build an rgba() string for the confidence band fill.
                    r, g, b = hex_to_rgb(cmap[mode])

                    fig = go.Figure()

                    # ── Confidence band ────────────────────────────────────
                    # Plotly's "toself" fill draws a closed polygon between
                    # two traces. The trick is to concatenate the upper bound
                    # forward with the lower bound reversed — that traces the
                    # outline of the shaded region in one go.
                    # Alpha=0.15 keeps the band subtle so it doesn't obscure
                    # the forecast line.
                    fig.add_scatter(
                        x=pd.concat([pred["ds"], pred["ds"].iloc[::-1]]),
                        y=pd.concat([pred["yhat_upper"], pred["yhat_lower"].iloc[::-1]]),
                        fill="toself",
                        fillcolor=f"rgba({int(r*255)},{int(g*255)},{int(b*255)},0.15)",
                        line=dict(width=0),
                        showlegend=True,
                        name=t("fc_band"),    # "80% confidence interval"
                    )

                    # ── Raw actuals (faint dots) ───────────────────────────
                    # Plotted as semi-transparent markers rather than a line to
                    # avoid visual competition with the smooth fitted line.
                    # The dots give the reader a feel for month-to-month noise
                    # without dominating the chart.
                    fig.add_scatter(
                        x=hist["ds"], y=hist["actual"],
                        mode="markers",
                        marker=dict(color=cmap[mode], size=5, opacity=0.35),
                        name=t("fc_actual"),
                        showlegend=True,
                    )

                    # ── Fitted line (historical) ───────────────────────────
                    # Prophet's in-sample fitted values — the model's best
                    # reconstruction of the historical series using all
                    # components (trend + seasonality + regressors).
                    # Plotted as a solid line that leads continuously into
                    # the dashed forecast line, signalling the model boundary.
                    fig.add_scatter(
                        x=hist["ds"], y=hist["yhat"],
                        mode="lines",
                        line=dict(color=cmap[mode], width=2),
                        name=t("fc_fitted"),
                        showlegend=True,
                    )

                    # ── Forecast line ──────────────────────────────────────
                    # Dashed to visually distinguish prediction from fitted history.
                    # Prepend the last historical point so the dashed line
                    # starts exactly where the solid fitted line ends — without
                    # this, there is a one-month visual gap between the two lines.
                    last_hist = hist.iloc[[-1]]
                    pred_with_join = pd.concat([last_hist, pred], ignore_index=True)
                    fig.add_scatter(
                        x=pred_with_join["ds"], y=pred_with_join["yhat"],
                        mode="lines+markers",
                        line=dict(color=cmap[mode], width=2, dash="dash"),
                        marker=dict(size=6),
                        name=t("fc_forecast"),
                    )

                    # ── Forecast-start marker ──────────────────────────────
                    # Vertical dotted line at the last observed date so the
                    # reader can immediately identify where history ends and
                    # prediction begins. Plotly expects Unix ms for x-axis
                    # timestamps when the axis is a datetime axis.
                    fig.add_vline(
                        x=hist["ds"].max().timestamp() * 1000,
                        line_dash="dot", line_color="grey", opacity=0.6,
                        annotation_text=(
                            "→ predicción" if st.session_state.lang == "es" else "→ forecast"
                        ),
                        annotation_font_size=10,
                    )

                    # Event and fare hike annotations — only when the sidebar
                    # toggle is on, as they can crowd the chart on long horizons.
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

                # ── Summary table ──────────────────────────────────────────
                # forecast_summary() compares the rolling mean of the last
                # 6 actuals (to smooth out seasonal position) against the
                # mean yhat over the forecast window.
                # direction: "up" / "down" if mean change exceeds ±5%; else "flat".
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
                    # Format raw counts as human-readable millions.
                    summary["last_actual"]   = summary["last_actual"].apply(lambda x: f"{x/1e6:.1f}M")
                    summary["mean_forecast"] = summary["mean_forecast"].apply(lambda x: f"{x/1e6:.1f}M")
                    # Leading "+" for positive changes makes the direction explicit.
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
            # prophet / cmdstanpy are optional heavy dependencies.
            # If missing, the tab shows a helpful install message rather than crashing.
            st.error(f"Missing dependency: {e}. Run: uv add prophet")
