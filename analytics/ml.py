"""
analytics/ml.py — Machine learning and forecasting for SUBE ridership.

Models:
  - forecast_ridership()  : Prophet forecast per mode, 6 months ahead
                            with Argentina holidays + fare hike regressors
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DASHBOARD_MODES, EVENTS, FARE_HIKES


# ── Constants ─────────────────────────────────────────────────────────────────

FORECAST_MONTHS = 6

# Macro shock events that get their own binary regressor.
# These are discrete structural breaks that fare_pressure doesn't capture.
_MACRO_SHOCK_COLORS = {"gray", "purple"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_fare_pressure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a cumulative fare pressure index for each row in df.

    For each month in df['ds'], sum the magnitudes of all fare hikes
    that have already taken effect by that date. The result is a
    monotonically increasing number that reflects accumulated fare burden.

    Freeze events (magnitude=0) contribute nothing to the index but their
    dates are still used as explicit Prophet changepoints — the freeze itself
    is a structural break in the trend even if the price didn't move.
    """
    hike_dates = [
        (pd.Timestamp(h["date"]), h["magnitude"])
        for h in FARE_HIKES
    ]

    def cumulative_pressure(ds: pd.Timestamp) -> float:
        return sum(mag for date, mag in hike_dates if date <= ds)

    df = df.copy()
    df["fare_pressure"] = df["ds"].apply(cumulative_pressure)
    return df


def _build_macro_shock(df: pd.DataFrame) -> pd.DataFrame:
    """
    Binary regressor: 1 for any month on or after the Dec 2023 Milei
    devaluation (the largest discrete macro shock in the data window).

    We use a single threshold rather than one column per event to avoid
    multicollinearity — the devaluations and fare hikes are correlated in
    time. fare_pressure already captures the fare side; this captures the
    broader purchasing-power collapse.
    """
    shock_dates = [
        pd.Timestamp(e["date"])
        for e in EVENTS
        if e.get("color") in _MACRO_SHOCK_COLORS
    ]
    if not shock_dates:
        df["macro_shock"] = 0
        return df

    threshold = min(shock_dates)
    df = df.copy()
    df["macro_shock"] = (df["ds"] >= threshold).astype(int)
    return df


def _all_changepoints(last_date: pd.Timestamp) -> list[pd.Timestamp]:
    """
    Collect all structural break dates before last_date:
      - Every fare hike / freeze date from FARE_HIKES
      - Every macro shock event from EVENTS (gray and purple)
    Deduplicates and sorts.
    """
    dates = set()

    for h in FARE_HIKES:
        d = pd.Timestamp(h["date"])
        if d < last_date:
            dates.add(d)

    for e in EVENTS:
        if e.get("color") in _MACRO_SHOCK_COLORS:
            d = pd.Timestamp(e["date"])
            if d < last_date:
                dates.add(d)

    return sorted(dates)


def _load_monthly_mode(conn, mode: str) -> pd.DataFrame:
    """
    Return a Prophet-ready DataFrame (ds, y) for a single mode.
    Unions monthly_historical (pre-2020) with monthly_transactions (post-2020).
    Falls back to monthly_transactions only if monthly_historical doesn't exist.
    """
    try:
        df = conn.execute("""
            SELECT month_start AS ds, total_usos AS y
            FROM monthly_historical
            WHERE modo = ?

            UNION ALL

            SELECT month_start AS ds, total_usos AS y
            FROM monthly_transactions
            WHERE modo = ?

            ORDER BY ds
        """, [mode, mode]).df()
    except Exception:
        df = conn.execute("""
            SELECT month_start AS ds, total_usos AS y
            FROM monthly_transactions
            WHERE modo = ?
            ORDER BY ds
        """, [mode]).df()
    df["ds"] = pd.to_datetime(df["ds"])
    return df


def _build_covid_impact(df: pd.DataFrame) -> pd.DataFrame:
    """
    Binary regressor marking the COVID disruption period (2020-03 to 2021-12).
    Lets Prophet fit the COVID collapse explicitly rather than absorbing it
    into the trend or seasonality components.
    Required when training on the full history including pre-2020 data.
    """
    df = df.copy()
    df["covid_impact"] = (
        (df["ds"] >= pd.Timestamp("2020-03-01")) &
        (df["ds"] <= pd.Timestamp("2021-12-01"))
    ).astype(int)
    return df


def _make_future(model, horizon: int, use_covid_regressor: bool = False) -> pd.DataFrame:
    """
    Build the future DataFrame for Prophet, including all regressors.
    covid_impact is always included — it will be 0 for all forecast dates
    (COVID period ended 2021-12, so no future rows will be flagged).
    """
    future = model.make_future_dataframe(periods=horizon, freq="MS")
    future = _build_fare_pressure(future)
    future = _build_macro_shock(future)
    future = _build_covid_impact(future)
    return future


# ── Main forecasting function ──────────────────────────────────────────────────

def forecast_ridership(
    conn,
    modes: list[str] | None = None,
    horizon: int = FORECAST_MONTHS,
) -> dict[str, pd.DataFrame]:
    """
    Fit a Prophet model per mode and forecast `horizon` months ahead.

    Args:
        conn    : DuckDB connection
        modes   : list of modes to forecast; defaults to DASHBOARD_MODES
        horizon : number of months to forecast (default 6)

    Returns:
        dict keyed by mode, each value a DataFrame with columns:
            ds, yhat, yhat_lower, yhat_upper,
            trend, additive_terms (seasonality),
            is_forecast (bool — True for future rows)
    """
    try:
        from prophet import Prophet
    except ImportError:
        logger.error("prophet not installed. Run: uv add prophet")
        return {}

    if modes is None:
        modes = DASHBOARD_MODES

    results = {}

    for mode in modes:
        logger.info(f"Fitting Prophet for {mode} …")

        df = _load_monthly_mode(conn, mode)
        if len(df) < 24:
            logger.warning(f"Skipping {mode}: only {len(df)} months of data (need ≥ 24)")
            continue

        # Training windows now that pre-2020 historical data is available:
        # - COLECTIVO: from 2013-01 (fully SUBE-integrated, reliable full history)
        # - SUBTE/TREN: from 2016-01 (SUBE integration mature; pre-2016 undercounts)
        # The COVID collapse (2020-03 → 2021-12) is handled via a covid_impact
        # binary regressor — Prophet learns the shock explicitly rather than
        # absorbing it into the trend or seasonality.
        if mode == "COLECTIVO":
            train_start = pd.Timestamp("2013-01-01")
        else:
            train_start = pd.Timestamp("2016-01-01")

        df = df[df["ds"] >= train_start].copy()
        logger.info(f"  {mode}: training from {train_start.strftime('%b %Y')} ({len(df)} months)")

        df = _build_fare_pressure(df)
        df = _build_macro_shock(df)
        df = _build_covid_impact(df)

        use_covid_regressor = True

        last_date = df["ds"].max()

        # Only use changepoints that fall within the training window
        known_changepoints = [
            cp for cp in _all_changepoints(last_date)
            if cp >= df["ds"].min()
        ]

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            seasonality_mode="multiplicative",
            interval_width=0.80,
            changepoint_prior_scale=0.03,   # tighter — we're providing explicit changepoints
            changepoint_range=0.95,
            changepoints=known_changepoints if known_changepoints else None,
        )
        model.add_country_holidays(country_name="AR")
        model.add_regressor("fare_pressure", standardize=True)
        model.add_regressor("macro_shock",   standardize=False)
        model.add_regressor("covid_impact",  standardize=False)

        # Suppress Stan/cmdstanpy output
        import logging
        logging.getLogger("prophet").setLevel(logging.WARNING)
        logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

        model.fit(df)

        # ── Forecast ───────────────────────────────────────────────────────
        future   = _make_future(model, horizon)
        forecast = model.predict(future)

        # Tag which rows are the actual forecast vs historical fitted values
        forecast["is_forecast"] = forecast["ds"] > last_date

        # Keep only the columns the dashboard needs
        cols = ["ds", "yhat", "yhat_lower", "yhat_upper",
                "trend", "additive_terms", "is_forecast"]
        cols = [c for c in cols if c in forecast.columns]
        forecast = forecast[cols].copy()

        # Merge in the actuals so the dashboard can plot both on the same figure
        forecast = forecast.merge(
            df[["ds", "y"]].rename(columns={"y": "actual"}),
            on="ds", how="left",
        )

        results[mode] = forecast
        logger.info(
            f"  {mode}: fitted {len(df)} months, "
            f"forecast through {forecast['ds'].max().strftime('%b %Y')}"
        )

    return results


# ── Forecast summary ───────────────────────────────────────────────────────────

def forecast_summary(forecasts: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Return a one-row-per-mode summary of the forecast.
    Compares the mean of the last 6 actuals vs the mean of the forecast
    period — smoothing out seasonal position effects.
    """
    rows = []
    for mode, fc in forecasts.items():
        actual_rows   = fc[~fc["is_forecast"]].dropna(subset=["actual"])
        forecast_rows = fc[fc["is_forecast"]]

        if actual_rows.empty or forecast_rows.empty:
            continue

        # Use rolling mean of last N months to avoid seasonal last-point bias
        n = min(6, len(actual_rows))
        baseline      = actual_rows["actual"].iloc[-n:].mean()
        mean_forecast = forecast_rows["yhat"].mean()
        pct_change    = (mean_forecast - baseline) / baseline * 100

        direction = "flat"
        if pct_change >  5:  direction = "up"
        if pct_change < -5:  direction = "down"

        rows.append({
            "mode":          mode,
            "last_actual":   actual_rows["actual"].iloc[-1],
            "mean_forecast": mean_forecast,
            "pct_change":    round(pct_change, 1),
            "direction":     direction,
        })

    return pd.DataFrame(rows)