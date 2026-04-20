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

    explainer("fc_explainer")

    horizon = st.select_slider(
        t("fc_horizon"),
        options=[3, 6, 9, 12, 15, 18, 21, 24],
        value=12,
    )

    with st.spinner(t("fc_running")):
        try:
            from analytics.ml import forecast_ridership, forecast_summary
            conn = get_conn()
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
                    hist = fc[~fc["is_forecast"]]
                    pred = fc[fc["is_forecast"]]
                    r, g, b = hex_to_rgb(cmap[mode])

                    fig = go.Figure()

                    fig.add_scatter(
                        x=pd.concat([pred["ds"], pred["ds"].iloc[::-1]]),
                        y=pd.concat([pred["yhat_upper"], pred["yhat_lower"].iloc[::-1]]),
                        fill="toself",
                        fillcolor=f"rgba({int(r*255)},{int(g*255)},{int(b*255)},0.15)",
                        line=dict(width=0),
                        showlegend=True,
                        name=t("fc_band"),
                    )

                    fig.add_scatter(
                        x=hist["ds"], y=hist["actual"],
                        mode="markers",
                        marker=dict(color=cmap[mode], size=5, opacity=0.35),
                        name=t("fc_actual"),
                        showlegend=True,
                    )

                    fig.add_scatter(
                        x=hist["ds"], y=hist["yhat"],
                        mode="lines",
                        line=dict(color=cmap[mode], width=2),
                        name=t("fc_fitted"),
                        showlegend=True,
                    )

                    last_hist = hist.iloc[[-1]]
                    pred_with_join = pd.concat([last_hist, pred], ignore_index=True)
                    fig.add_scatter(
                        x=pred_with_join["ds"], y=pred_with_join["yhat"],
                        mode="lines+markers",
                        line=dict(color=cmap[mode], width=2, dash="dash"),
                        marker=dict(size=6),
                        name=t("fc_forecast"),
                    )

                    fig.add_vline(
                        x=hist["ds"].max().timestamp() * 1000,
                        line_dash="dot", line_color="grey", opacity=0.6,
                        annotation_text=(
                            "→ predicción" if st.session_state.lang == "es" else "→ forecast"
                        ),
                        annotation_font_size=10,
                    )

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

                # ── Summary table ──────────────────────────────────────
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
                    summary["last_actual"]   = summary["last_actual"].apply(lambda x: f"{x/1e6:.1f}M")
                    summary["mean_forecast"] = summary["mean_forecast"].apply(lambda x: f"{x/1e6:.1f}M")
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
            st.error(f"Missing dependency: {e}. Run: uv add prophet")
