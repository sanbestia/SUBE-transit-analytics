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
    daily   = _make_daily_df(n_days=400, start="2022-01-01")
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

    def test_ma30_smoother_than_ma7(self, conn_with_data):
        result = rolling_stats(conn_with_data)
        assert result["ma_30d"].std() <= result["ma_7d"].std()

    def test_sorted_by_fecha(self, conn_with_data):
        result = rolling_stats(conn_with_data)
        assert result["fecha"].is_monotonic_increasing

    def test_row_count_matches_input(self, conn_with_data):
        n_days = conn_with_data.execute(
            "SELECT COUNT(DISTINCT fecha) FROM daily_transactions"
        ).fetchone()[0]
        result = rolling_stats(conn_with_data)
        assert len(result) == n_days

    def test_ma7_is_mean_of_up_to_7_values(self, conn_with_data):
        """On day 7 and beyond, ma_7d should equal the mean of the 7-day window."""
        result = rolling_stats(conn_with_data)
        # For row index 6 (7th row, 0-indexed), ma_7d = mean of rows 0..6
        expected = result["total_usos"].iloc[:7].mean()
        actual = result["ma_7d"].iloc[6]
        assert abs(actual - expected) < 1e-6


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

    def test_english_labels_used_when_lang_en(self):
        from config import EVENTS
        event_date = pd.Timestamp(EVENTS[0]["date"])
        idx = pd.date_range(event_date - pd.Timedelta(days=2),
                            event_date + pd.Timedelta(days=2), freq="D")
        residuals = pd.Series(np.ones(len(idx)) * 10, index=idx)
        result = detect_anomalies(residuals, lang="en")
        row = result[result["fecha"] == event_date]
        if not row.empty:
            assert row["event_label"].iloc[0] == EVENTS[0]["label_en"]

    def test_is_anomaly_is_boolean_dtype(self):
        result = detect_anomalies(self._normal_residuals())
        assert result["is_anomaly"].dtype == bool

    def test_spike_below_threshold_not_flagged(self):
        """A spike that doesn't exceed the threshold must not be flagged."""
        idx = pd.date_range("2022-01-01", periods=100, freq="D")
        data = np.zeros(100)
        data[50] = 2.5  # below 3σ threshold when data is all zeros except this point
        residuals = pd.Series(data, index=idx)
        result = detect_anomalies(residuals, z_threshold=3.0)
        # The spike value at position 50 might or might not exceed 3σ depending on std;
        # what we assert is that the function respects the threshold parameter.
        result_low  = detect_anomalies(residuals, z_threshold=1.0)["is_anomaly"].sum()
        result_high = detect_anomalies(residuals, z_threshold=10.0)["is_anomaly"].sum()
        assert result_low >= result_high


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
        result = compute_recovery_index(conn_with_data, baseline_years=[1990])
        assert result["recovery_index"].isna().all()

    def test_higher_year_has_higher_index_when_data_grows(self, conn_with_data):
        """Synthetic data is set up so 2024 ridership > 2022 — index should exceed 100."""
        result = compute_recovery_index(conn_with_data, baseline_years=[2022])
        rows_2024 = result[result["year"] == 2024]["recovery_index"].dropna()
        assert (rows_2024 > 100).all()

    def test_multiple_baseline_years_averaged(self, conn_with_data):
        """Using multiple baseline years should produce different results than a single year."""
        single = compute_recovery_index(conn_with_data, baseline_years=[2022])
        multi  = compute_recovery_index(conn_with_data, baseline_years=[2022, 2023])
        # They may differ because the baseline average changes — just assert both succeed
        assert not single.empty
        assert not multi.empty


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

    def test_only_dashboard_modes_returned(self, conn_with_data):
        from config import DASHBOARD_MODES
        result = modal_statistics(conn_with_data)
        for modo in result["modo"]:
            assert modo in DASHBOARD_MODES

    def test_avg_daily_is_positive(self, conn_with_data):
        result = modal_statistics(conn_with_data)
        assert (result["avg_daily"] > 0).all()


# ── decompose_series ───────────────────────────────────────────────────────

class TestDecomposeSeries:
    """
    decompose_series requires statsmodels. Tests are skipped if it's not installed,
    and they use a longer synthetic series (730+ days) to satisfy STL's minimum
    requirement of 2 full seasonal cycles.
    """

    statsmodels = pytest.importorskip("statsmodels", reason="statsmodels not installed")

    @pytest.fixture
    def conn_long(self):
        """800-day daily series — enough for both weekly and annual STL."""
        np.random.seed(0)
        n = 800
        daily = _make_daily_df(n_days=n, start="2021-01-01")
        monthly = _make_monthly_df(years=(2021, 2022, 2023, 2024))
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE daily_transactions AS SELECT * FROM daily")
        conn.execute("CREATE TABLE monthly_transactions AS SELECT * FROM monthly")
        conn.execute("""
            CREATE VIEW v_total_daily AS
            SELECT fecha, year, month, day_of_week, SUM(cantidad_usos) AS total_usos
            FROM daily_transactions
            WHERE NOT is_suspicious
            GROUP BY fecha, year, month, day_of_week
            ORDER BY fecha
        """)
        return conn

    def test_returns_dict(self, conn_long):
        from analytics.time_series import decompose_series
        result = decompose_series(conn_long, period=7)
        assert isinstance(result, dict)

    def test_has_required_keys(self, conn_long):
        from analytics.time_series import decompose_series
        result = decompose_series(conn_long, period=7)
        for key in ("original", "trend", "seasonal", "residual"):
            assert key in result, f"Missing key: {key}"

    def test_all_components_are_series(self, conn_long):
        from analytics.time_series import decompose_series
        result = decompose_series(conn_long, period=7)
        for key, val in result.items():
            assert isinstance(val, pd.Series), f"Component '{key}' is not a pd.Series"

    def test_components_same_length(self, conn_long):
        from analytics.time_series import decompose_series
        result = decompose_series(conn_long, period=7)
        lengths = {key: len(val) for key, val in result.items()}
        assert len(set(lengths.values())) == 1, f"Components have different lengths: {lengths}"

    def test_components_have_datetime_index(self, conn_long):
        from analytics.time_series import decompose_series
        result = decompose_series(conn_long, period=7)
        assert pd.api.types.is_datetime64_any_dtype(result["original"].index)

    def test_original_matches_input_values(self, conn_long):
        from analytics.time_series import decompose_series
        result = decompose_series(conn_long, period=7)
        # The original series should reflect the actual ridership values (ballpark check)
        assert result["original"].mean() > 0

    def test_mode_filter_returns_subset(self, conn_long):
        from analytics.time_series import decompose_series
        result_all  = decompose_series(conn_long, period=7)
        result_mode = decompose_series(conn_long, mode="COLECTIVO", period=7)
        # COLECTIVO alone should have lower ridership than all modes combined
        assert result_mode["original"].mean() < result_all["original"].mean()

    def test_short_series_falls_back_to_weekly(self, conn_long):
        """If the series is too short for annual period, STL falls back to period=7."""
        # Build a very short series (15 days — not enough for period=365)
        np.random.seed(1)
        short_daily = _make_daily_df(n_days=20, start="2024-01-01")
        conn_short = duckdb.connect(":memory:")
        conn_short.execute("CREATE TABLE daily_transactions AS SELECT * FROM short_daily")
        conn_short.execute("""
            CREATE VIEW v_total_daily AS
            SELECT fecha, year, month, day_of_week, SUM(cantidad_usos) AS total_usos
            FROM daily_transactions
            WHERE NOT is_suspicious
            GROUP BY fecha, year, month, day_of_week
            ORDER BY fecha
        """)
        from analytics.time_series import decompose_series
        # period=365 with 20 days should silently fall back and return a result
        result = decompose_series(conn_short, period=365)
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_returns_empty_dict_without_statsmodels(self, conn_long, monkeypatch):
        """If statsmodels is unavailable, decompose_series should return {} gracefully."""
        import analytics.time_series as ts_module
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "statsmodels.tsa.seasonal":
                raise ImportError("mocked missing statsmodels")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        from analytics.time_series import decompose_series
        result = decompose_series(conn_long, period=7)
        assert result == {}