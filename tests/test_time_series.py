"""
tests/test_time_series.py — Tests for analytics/time_series.py

Uses in-memory DuckDB with synthetic data — no real CSV files needed.
"""

import datetime

import duckdb
import numpy as np
import pandas as pd
import pytest

from analytics.time_series import (
    compute_recovery_index,
    detect_anomalies,
    modal_statistics,
    rolling_stats,
)


# ── Fixtures ───────────────────────────────────────────────────────────────

def _make_daily_df(n_days: int = 60, start: str = "2022-01-01") -> pd.DataFrame:
    """Generate synthetic daily ridership data for testing."""
    dates = pd.date_range(start, periods=n_days, freq="D")
    rows = []
    for date in dates:
        for modo, base in [("COLECTIVO", 50000), ("TREN", 20000), ("SUBTE", 15000)]:
            rows.append({
                "fecha":         date,
                "year":          date.year,
                "month":         date.month,
                "day_of_week":   date.dayofweek,
                "modo":          modo,
                "cantidad_usos": base + np.random.randint(-1000, 1000),
                "is_suspicious": False,
                "source_file":   "synthetic.csv",
                "nombre_empresa": "EMPRESA TEST",
                "linea":         "LINEA 1",
                "amba":          "SI",
                "jurisdiccion":  "NACIONAL",
                "provincia":     "BUENOS AIRES",
                "municipio":     "CABA",
                "dato_preliminar": "N",
            })
    return pd.DataFrame(rows)


def _make_monthly_df(years=(2022, 2023, 2024)) -> pd.DataFrame:
    """Synthetic monthly rollup for recovery index / modal stats tests."""
    rows = []
    for year in years:
        for month in range(1, 13):
            for modo, base in [("COLECTIVO", 1_500_000), ("TREN", 600_000)]:
                month_start = pd.Timestamp(year=year, month=month, day=1)
                rows.append({
                    "month_start":    month_start,
                    "year":           year,
                    "month":          month,
                    "modo":           modo,
                    "total_usos":     base + (year - 2022) * 50_000,
                    "avg_daily_usos": (base + (year - 2022) * 50_000) / 30,
                    "days_with_data": 30,
                    "suspicious_days": 0,
                })
    return pd.DataFrame(rows)


@pytest.fixture
def conn_with_data():
    """In-memory DuckDB with all views needed by analytics functions."""
    np.random.seed(42)
    daily  = _make_daily_df(n_days=400, start="2022-01-01")
    monthly = _make_monthly_df()

    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE daily_transactions AS SELECT * FROM daily
    """)
    conn.execute("""
        CREATE TABLE monthly_transactions AS SELECT * FROM monthly
    """)
    conn.execute("""
        CREATE VIEW v_total_daily AS
        SELECT fecha, year, month, day_of_week, SUM(cantidad_usos) AS total_usos
        FROM daily_transactions
        WHERE NOT is_suspicious
        GROUP BY fecha, year, month, day_of_week
        ORDER BY fecha
    """)

    return conn


# ── rolling_stats ──────────────────────────────────────────────────────────

class TestRollingStats:
    def test_returns_dataframe(self, conn_with_data):
        result = rolling_stats(conn_with_data)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, conn_with_data):
        result = rolling_stats(conn_with_data)
        assert "fecha" in result.columns
        assert "total_usos" in result.columns
        assert "ma_7d" in result.columns
        assert "ma_30d" in result.columns

    def test_ma7_is_never_null(self, conn_with_data):
        result = rolling_stats(conn_with_data)
        assert result["ma_7d"].isna().sum() == 0

    def test_ma30_is_never_null(self, conn_with_data):
        result = rolling_stats(conn_with_data)
        assert result["ma_30d"].isna().sum() == 0

    def test_ma7_smooths_values(self, conn_with_data):
        result = rolling_stats(conn_with_data)
        # MA should have lower variance than raw
        assert result["ma_7d"].std() <= result["total_usos"].std()

    def test_sorted_by_fecha(self, conn_with_data):
        result = rolling_stats(conn_with_data)
        assert result["fecha"].is_monotonic_increasing

    def test_row_count_matches_input(self, conn_with_data):
        n_days = conn_with_data.execute(
            "SELECT COUNT(DISTINCT fecha) FROM daily_transactions"
        ).fetchone()[0]
        result = rolling_stats(conn_with_data)
        assert len(result) == n_days


# ── detect_anomalies ───────────────────────────────────────────────────────

class TestDetectAnomalies:
    def _normal_residuals(self, n=100, seed=42) -> pd.Series:
        np.random.seed(seed)
        idx = pd.date_range("2022-01-01", periods=n, freq="D")
        return pd.Series(np.random.normal(0, 1, n), index=idx)

    def _residuals_with_spike(self, n=100, spike_pos=50, spike_val=10.0) -> pd.Series:
        np.random.seed(42)
        idx = pd.date_range("2022-01-01", periods=n, freq="D")
        data = np.random.normal(0, 1, n)
        data[spike_pos] = spike_val
        return pd.Series(data, index=idx)

    def test_returns_dataframe(self):
        result = detect_anomalies(self._normal_residuals())
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self):
        result = detect_anomalies(self._normal_residuals())
        for col in ("fecha", "residual", "z_score", "is_anomaly", "event_label"):
            assert col in result.columns

    def test_row_count_matches_input(self):
        residuals = self._normal_residuals(n=100)
        result = detect_anomalies(residuals, lang="es")
        assert len(result) == 100

    def test_detects_obvious_spike(self):
        residuals = self._residuals_with_spike(spike_val=20.0)
        result = detect_anomalies(residuals, z_threshold=3.0)
        assert result["is_anomaly"].any()

    def test_normal_data_has_few_anomalies(self):
        residuals = self._normal_residuals(n=500)
        result = detect_anomalies(residuals, z_threshold=3.0)
        # With normally distributed data, ~0.3% should exceed 3σ
        anomaly_rate = result["is_anomaly"].mean()
        assert anomaly_rate < 0.05

    def test_higher_threshold_fewer_anomalies(self):
        residuals = self._residuals_with_spike(spike_val=5.0, n=200)
        low  = detect_anomalies(residuals, z_threshold=2.0)["is_anomaly"].sum()
        high = detect_anomalies(residuals, z_threshold=4.0)["is_anomaly"].sum()
        assert low >= high

    def test_z_scores_computed_correctly(self):
        idx = pd.date_range("2022-01-01", periods=5, freq="D")
        residuals = pd.Series([0.0, 1.0, 2.0, 3.0, 4.0], index=idx)
        result = detect_anomalies(residuals, lang="es")
        mean = residuals.mean()
        std  = residuals.std()
        expected_z = (residuals.iloc[0] - mean) / std
        assert abs(result["z_score"].iloc[0] - expected_z) < 1e-6

    def test_known_event_dates_annotated(self):
        from config import EVENTS
        event_date = pd.Timestamp(EVENTS[0]["date"])
        # Build residuals that include a known event date
        idx = pd.date_range(event_date - pd.Timedelta(days=5),
                            event_date + pd.Timedelta(days=5), freq="D")
        residuals = pd.Series(np.ones(len(idx)) * 10, index=idx)
        result = detect_anomalies(residuals, lang="es")
        row = result[result["fecha"] == event_date]
        if not row.empty:
            assert row["event_label"].iloc[0] == EVENTS[0]["label_es"]

    def test_non_event_dates_have_empty_label(self):
        idx = pd.date_range("2000-01-01", periods=10, freq="D")
        residuals = pd.Series(np.zeros(10), index=idx)
        result = detect_anomalies(residuals, lang="es")
        assert (result["event_label"] == "").all()


# ── compute_recovery_index ─────────────────────────────────────────────────

class TestComputeRecoveryIndex:
    def test_returns_dataframe(self, conn_with_data):
        result = compute_recovery_index(conn_with_data, baseline_years=[2022, 2023])
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, conn_with_data):
        result = compute_recovery_index(conn_with_data, baseline_years=[2022, 2023])
        for col in ("month_start", "modo", "total_usos", "baseline_avg", "recovery_index"):
            assert col in result.columns

    def test_baseline_year_recovery_near_100(self, conn_with_data):
        result = compute_recovery_index(conn_with_data, baseline_years=[2022])
        baseline_rows = result[result["year"] == 2022]
        # A year used as its own baseline should average ~100
        avg_index = baseline_rows["recovery_index"].mean()
        assert 90 <= avg_index <= 110

    def test_recovery_index_is_positive(self, conn_with_data):
        result = compute_recovery_index(conn_with_data, baseline_years=[2022])
        valid = result["recovery_index"].dropna()
        assert (valid > 0).all()

    def test_covers_all_modes(self, conn_with_data):
        result = compute_recovery_index(conn_with_data, baseline_years=[2022])
        assert "COLECTIVO" in result["modo"].values
        assert "TREN" in result["modo"].values

    def test_no_baseline_data_gives_null_index(self, conn_with_data):
        # Use a year not in the data as baseline
        result = compute_recovery_index(conn_with_data, baseline_years=[1990])
        # All recovery indices should be NaN since baseline_avg will be NaN
        assert result["recovery_index"].isna().all()


# ── modal_statistics ───────────────────────────────────────────────────────

class TestModalStatistics:
    def test_returns_dataframe(self, conn_with_data):
        result = modal_statistics(conn_with_data)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, conn_with_data):
        result = modal_statistics(conn_with_data)
        for col in ("modo", "total_all_time", "avg_daily", "peak_monthly", "min_monthly"):
            assert col in result.columns

    def test_one_row_per_mode(self, conn_with_data):
        result = modal_statistics(conn_with_data)
        assert result["modo"].nunique() == len(result)

    def test_sorted_by_total_descending(self, conn_with_data):
        result = modal_statistics(conn_with_data)
        assert result["total_all_time"].is_monotonic_decreasing

    def test_totals_are_positive(self, conn_with_data):
        result = modal_statistics(conn_with_data)
        assert (result["total_all_time"] > 0).all()

    def test_peak_gte_min(self, conn_with_data):
        result = modal_statistics(conn_with_data)
        assert (result["peak_monthly"] >= result["min_monthly"]).all()

    def test_colectivo_has_highest_ridership(self, conn_with_data):
        result = modal_statistics(conn_with_data)
        assert result.iloc[0]["modo"] == "COLECTIVO"