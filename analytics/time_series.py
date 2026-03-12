"""
analytics/time_series.py — Statistical analysis on SUBE ridership data.

Functions:
  - decompose_series()   : STL decomposition (trend + seasonality + residuals)
  - detect_anomalies()   : Flag statistical outliers in residuals
  - compute_recovery()   : Post-COVID recovery index vs 2019/pre-COVID baseline
  - rolling_stats()      : 7-day and 30-day rolling averages
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import EVENTS


def _get_total_daily(conn) -> pd.DataFrame:
    """Fetch total daily ridership (all modes combined) from DuckDB."""
    return conn.execute("""
        SELECT fecha, total_usos
        FROM v_total_daily
        ORDER BY fecha
    """).df()


def rolling_stats(conn) -> pd.DataFrame:
    """
    Add 7-day and 30-day rolling averages to the daily totals.
    Useful for smoothing weekend dips in the chart.
    """
    df = _get_total_daily(conn)
    df = df.set_index("fecha").sort_index()
    df["ma_7d"]  = df["total_usos"].rolling(7,  min_periods=1).mean()
    df["ma_30d"] = df["total_usos"].rolling(30, min_periods=1).mean()
    return df.reset_index()


def decompose_series(conn, mode: str | None = None, period: int = 365) -> dict:
    """
    STL (Seasonal-Trend decomposition using LOESS) on daily ridership.

    Args:
        conn   : DuckDB connection
        mode   : filter to a single transport mode, or None for all modes combined
        period : seasonality period in days (365 for yearly, 7 for weekly)

    Returns dict with keys: original, trend, seasonal, residual (all pd.Series)
    """
    try:
        from statsmodels.tsa.seasonal import STL
    except ImportError:
        logger.error("statsmodels not installed. Run: pip install statsmodels")
        return {}

    if mode:
        df = conn.execute("""
            SELECT fecha, SUM(cantidad_usos) AS total_usos
            FROM daily_transactions
            WHERE NOT is_suspicious AND modo = ?
            GROUP BY fecha
            ORDER BY fecha
        """, [mode]).df()
    else:
        df = _get_total_daily(conn)

    series = df.set_index("fecha")["total_usos"].asfreq("D").fillna(method="ffill")

    # Need at least 2 full cycles for STL
    if len(series) < 2 * period:
        logger.warning(
            f"Series too short ({len(series)} days) for period={period}. "
            "Falling back to period=7 (weekly)."
        )
        period = 7

    logger.info(f"Running STL decomposition (period={period}, n={len(series)}) …")
    stl    = STL(series, period=period, robust=True)
    result = stl.fit()

    return {
        "original":  series,
        "trend":     pd.Series(result.trend,    index=series.index),
        "seasonal":  pd.Series(result.seasonal, index=series.index),
        "residual":  pd.Series(result.resid,    index=series.index),
    }


def detect_anomalies(residuals: pd.Series, z_threshold: float = 3.0) -> pd.DataFrame:
    """
    Flag dates where the STL residual exceeds `z_threshold` standard deviations.
    Annotates known events from config.EVENTS where dates align.

    Returns a DataFrame with columns: fecha, residual, z_score, is_anomaly, event_label
    """
    mean = residuals.mean()
    std  = residuals.std()

    df = pd.DataFrame({
        "fecha":      residuals.index,
        "residual":   residuals.values,
        "z_score":    ((residuals - mean) / std).values,
    })
    df["is_anomaly"] = df["z_score"].abs() > z_threshold

    # Annotate with known events
    event_map = {
        pd.Timestamp(e["date"]): e["label"]
        for e in EVENTS
    }
    df["event_label"] = df["fecha"].map(event_map).fillna("")

    anomaly_count = df["is_anomaly"].sum()
    logger.info(f"Detected {anomaly_count} anomalies (|z| > {z_threshold})")
    return df


def compute_recovery_index(conn, baseline_years: list[int] = [2022, 2023]) -> pd.DataFrame:
    """
    Compute a recovery index: monthly ridership relative to the average of
    baseline years (post-ASPO "normal").

    Returns a DataFrame with month_start, modo, total_usos, baseline_avg, recovery_index.
    """
    df = conn.execute("""
        SELECT month_start, year, month, modo, total_usos
        FROM monthly_transactions
        ORDER BY month_start, modo
    """).df()

    # Compute per-mode, per-calendar-month baseline average
    baseline = (
        df[df["year"].isin(baseline_years)]
        .groupby(["month", "modo"])["total_usos"]
        .mean()
        .rename("baseline_avg")
        .reset_index()
    )

    df = df.merge(baseline, on=["month", "modo"], how="left")
    df["recovery_index"] = (df["total_usos"] / df["baseline_avg"] * 100).round(2)

    return df


def modal_statistics(conn) -> pd.DataFrame:
    """
    Per-mode summary statistics for the dashboard KPI cards.
    """
    return conn.execute("""
        SELECT
            modo,
            SUM(total_usos)                             AS total_all_time,
            AVG(avg_daily_usos)                         AS avg_daily,
            MAX(total_usos)                             AS peak_monthly,
            MIN(CASE WHEN total_usos > 0 THEN total_usos END) AS min_monthly
        FROM monthly_transactions
        GROUP BY modo
        ORDER BY total_all_time DESC
    """).df()
