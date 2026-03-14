"""
tests/test_ml.py — Tests for analytics/ml.py

Pure helper functions (_build_fare_pressure, _build_macro_shock,
_all_changepoints, _build_covid_impact, forecast_summary) are fully tested
without any ML dependencies.

forecast_ridership is tested at the smoke-test level and only runs if
Prophet is installed (skipped otherwise).
"""

import duckdb
import pandas as pd
import numpy as np
import pytest

from analytics.ml import (
    _build_covid_impact,
    _build_fare_pressure,
    _build_macro_shock,
    _all_changepoints,
    forecast_summary,
)


# ── Helpers ────────────────────────────────────────────────────────────────

def _date_range_df(start: str, periods: int, freq: str = "MS") -> pd.DataFrame:
    """Build a minimal DataFrame with a 'ds' column for regressor tests."""
    return pd.DataFrame({"ds": pd.date_range(start, periods=periods, freq=freq)})


# ── _build_fare_pressure ───────────────────────────────────────────────────

class TestBuildFarePressure:
    def test_returns_dataframe(self):
        df = _date_range_df("2022-01-01", 12)
        result = _build_fare_pressure(df)
        assert isinstance(result, pd.DataFrame)

    def test_adds_fare_pressure_column(self):
        df = _date_range_df("2022-01-01", 12)
        result = _build_fare_pressure(df)
        assert "fare_pressure" in result.columns

    def test_does_not_mutate_input(self):
        df = _date_range_df("2022-01-01", 12)
        original_cols = list(df.columns)
        _build_fare_pressure(df)
        assert list(df.columns) == original_cols

    def test_pressure_zero_before_any_hike(self):
        # 2020 is before any fare hike in fare_hikes.yaml (first hike: 2022-03-01)
        df = _date_range_df("2020-01-01", 6)
        result = _build_fare_pressure(df)
        assert (result["fare_pressure"] == 0).all()

    def test_pressure_increases_after_hike(self):
        # Rows before and after the 2022-03-01 hike (magnitude=30)
        df = pd.DataFrame({"ds": [
            pd.Timestamp("2022-02-01"),  # before
            pd.Timestamp("2022-03-01"),  # on the hike date
            pd.Timestamp("2022-04-01"),  # after
        ]})
        result = _build_fare_pressure(df)
        assert result["fare_pressure"].iloc[0] == 0
        assert result["fare_pressure"].iloc[1] > 0   # hike applies on its date
        assert result["fare_pressure"].iloc[2] >= result["fare_pressure"].iloc[1]

    def test_pressure_is_monotonically_non_decreasing(self):
        """Cumulative fare pressure can only stay flat or increase over time."""
        df = _date_range_df("2021-01-01", 60)
        result = _build_fare_pressure(df)
        diffs = result["fare_pressure"].diff().dropna()
        assert (diffs >= 0).all()

    def test_pressure_is_numeric(self):
        df = _date_range_df("2022-01-01", 12)
        result = _build_fare_pressure(df)
        assert pd.api.types.is_numeric_dtype(result["fare_pressure"])

    def test_freeze_entry_does_not_add_pressure(self):
        """A fare hike with magnitude=0 (freeze) contributes nothing to the index."""
        # 2023-08-01 is a freeze (magnitude=0)
        before_freeze = pd.Timestamp("2023-07-01")
        on_freeze     = pd.Timestamp("2023-08-01")
        df = pd.DataFrame({"ds": [before_freeze, on_freeze]})
        result = _build_fare_pressure(df)
        # Pressure on the freeze date should equal pressure before it (no change)
        assert result["fare_pressure"].iloc[1] == result["fare_pressure"].iloc[0]


# ── _build_macro_shock ─────────────────────────────────────────────────────

class TestBuildMacroShock:
    def test_returns_dataframe(self):
        df = _date_range_df("2022-01-01", 12)
        result = _build_macro_shock(df)
        assert isinstance(result, pd.DataFrame)

    def test_adds_macro_shock_column(self):
        df = _date_range_df("2022-01-01", 12)
        result = _build_macro_shock(df)
        assert "macro_shock" in result.columns

    def test_does_not_mutate_input(self):
        df = _date_range_df("2022-01-01", 12)
        _build_macro_shock(df)
        assert "macro_shock" not in df.columns

    def test_macro_shock_is_binary(self):
        df = _date_range_df("2020-01-01", 72)  # spans pre- and post-shock periods
        result = _build_macro_shock(df)
        unique_values = set(result["macro_shock"].unique())
        assert unique_values <= {0, 1}

    def test_pre_shock_rows_are_zero(self):
        # All dates in 2020 should be 0 (shock threshold is Dec 2023 at earliest)
        df = _date_range_df("2020-01-01", 12)
        result = _build_macro_shock(df)
        assert (result["macro_shock"] == 0).all()

    def test_post_shock_rows_are_one(self):
        # Dates in 2024 are after the Dec 2023 Milei devaluation
        df = _date_range_df("2024-01-01", 12)
        result = _build_macro_shock(df)
        assert (result["macro_shock"] == 1).all()

    def test_transition_is_a_step_function(self):
        """Once macro_shock switches to 1, it must not go back to 0."""
        df = _date_range_df("2023-01-01", 24)
        result = _build_macro_shock(df)
        values = result["macro_shock"].tolist()
        # Find first 1
        try:
            first_one = values.index(1)
        except ValueError:
            return  # all zeros — acceptable if shock date is after this range
        assert all(v == 1 for v in values[first_one:])


# ── _all_changepoints ──────────────────────────────────────────────────────

class TestAllChangepoints:
    def test_returns_list(self):
        last_date = pd.Timestamp("2025-01-01")
        result = _all_changepoints(last_date)
        assert isinstance(result, list)

    def test_returns_timestamps(self):
        last_date = pd.Timestamp("2025-01-01")
        result = _all_changepoints(last_date)
        for cp in result:
            assert isinstance(cp, pd.Timestamp)

    def test_all_changepoints_before_last_date(self):
        last_date = pd.Timestamp("2025-01-01")
        result = _all_changepoints(last_date)
        for cp in result:
            assert cp < last_date

    def test_sorted_ascending(self):
        last_date = pd.Timestamp("2026-01-01")
        result = _all_changepoints(last_date)
        assert result == sorted(result)

    def test_no_duplicates(self):
        last_date = pd.Timestamp("2026-01-01")
        result = _all_changepoints(last_date)
        assert len(result) == len(set(result))

    def test_empty_when_last_date_before_all_events(self):
        # Set last_date to before any known events (2022-03-01 is the first fare hike)
        last_date = pd.Timestamp("2021-01-01")
        result = _all_changepoints(last_date)
        assert result == []

    def test_includes_fare_hike_dates(self):
        from config import FARE_HIKES
        last_date = pd.Timestamp("2026-12-31")
        result = _all_changepoints(last_date)
        # At least the 2022-03-01 hike should appear
        first_hike = pd.Timestamp(FARE_HIKES[0]["date"])
        assert first_hike in result

    def test_includes_macro_shock_dates(self):
        from config import EVENTS
        from analytics.ml import _MACRO_SHOCK_COLORS
        macro_events = [e for e in EVENTS if e.get("color") in _MACRO_SHOCK_COLORS]
        if not macro_events:
            pytest.skip("No macro shock events defined in config")
        shock_date = pd.Timestamp(macro_events[0]["date"])
        last_date = shock_date + pd.Timedelta(days=1)
        result = _all_changepoints(last_date)
        assert shock_date in result


# ── _build_covid_impact ────────────────────────────────────────────────────

class TestBuildCovidImpact:
    def test_returns_dataframe(self):
        df = _date_range_df("2019-01-01", 36)
        result = _build_covid_impact(df)
        assert isinstance(result, pd.DataFrame)

    def test_adds_covid_impact_column(self):
        df = _date_range_df("2019-01-01", 36)
        result = _build_covid_impact(df)
        assert "covid_impact" in result.columns

    def test_does_not_mutate_input(self):
        df = _date_range_df("2019-01-01", 36)
        _build_covid_impact(df)
        assert "covid_impact" not in df.columns

    def test_is_binary(self):
        df = _date_range_df("2019-01-01", 48)
        result = _build_covid_impact(df)
        unique_values = set(result["covid_impact"].unique())
        assert unique_values <= {0, 1}

    def test_pre_covid_is_zero(self):
        df = _date_range_df("2019-01-01", 14)  # Jan 2019 – Feb 2020
        result = _build_covid_impact(df)
        assert (result["covid_impact"] == 0).all()

    def test_during_covid_is_one(self):
        # COVID window: 2020-03-01 to 2021-12-01
        df = pd.DataFrame({"ds": [
            pd.Timestamp("2020-06-01"),
            pd.Timestamp("2021-06-01"),
        ]})
        result = _build_covid_impact(df)
        assert (result["covid_impact"] == 1).all()

    def test_post_covid_is_zero(self):
        df = _date_range_df("2022-01-01", 12)
        result = _build_covid_impact(df)
        assert (result["covid_impact"] == 0).all()

    def test_boundary_start_is_one(self):
        df = pd.DataFrame({"ds": [pd.Timestamp("2020-03-01")]})
        result = _build_covid_impact(df)
        assert result["covid_impact"].iloc[0] == 1

    def test_boundary_end_is_one(self):
        df = pd.DataFrame({"ds": [pd.Timestamp("2021-12-01")]})
        result = _build_covid_impact(df)
        assert result["covid_impact"].iloc[0] == 1


# ── forecast_summary ───────────────────────────────────────────────────────

class TestForecastSummary:
    def _make_forecast(self, mode: str, n_actual: int = 24, n_forecast: int = 6,
                       actual_base: float = 1_000_000,
                       forecast_base: float = 1_100_000) -> pd.DataFrame:
        """Build a minimal forecast DataFrame mimicking forecast_ridership output."""
        anchor = pd.Timestamp("2022-01-01")
        if n_actual > 0:
            hist_dates = pd.date_range(anchor, periods=n_actual, freq="MS")
            pred_start = hist_dates[-1] + pd.DateOffset(months=1)
        else:
            hist_dates = pd.DatetimeIndex([])
            pred_start = anchor

        pred_dates = pd.date_range(pred_start, periods=n_forecast, freq="MS")
        hist = pd.DataFrame({
            "ds":          hist_dates,
            "yhat":        [actual_base] * n_actual,
            "actual":      [actual_base] * n_actual,
            "is_forecast": [False] * n_actual,
        })
        pred = pd.DataFrame({
            "ds":          pred_dates,
            "yhat":        [forecast_base] * n_forecast,
            "actual":      [np.nan] * n_forecast,
            "is_forecast": [True] * n_forecast,
        })
        fc = pd.concat([hist, pred], ignore_index=True)
        fc["is_forecast"] = fc["is_forecast"].astype(bool)
        return fc

    def test_returns_dataframe(self):
        forecasts = {"COLECTIVO": self._make_forecast("COLECTIVO")}
        result = forecast_summary(forecasts)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self):
        forecasts = {"COLECTIVO": self._make_forecast("COLECTIVO")}
        result = forecast_summary(forecasts)
        for col in ("mode", "last_actual", "mean_forecast", "pct_change", "direction"):
            assert col in result.columns

    def test_one_row_per_mode(self):
        forecasts = {
            "COLECTIVO": self._make_forecast("COLECTIVO"),
            "TREN":      self._make_forecast("TREN"),
        }
        result = forecast_summary(forecasts)
        assert len(result) == 2
        assert set(result["mode"]) == {"COLECTIVO", "TREN"}

    def test_direction_up_when_forecast_higher(self):
        fc = self._make_forecast("COLECTIVO", actual_base=1_000_000, forecast_base=1_200_000)
        result = forecast_summary({"COLECTIVO": fc})
        assert result["direction"].iloc[0] == "up"

    def test_direction_down_when_forecast_lower(self):
        fc = self._make_forecast("COLECTIVO", actual_base=1_000_000, forecast_base=800_000)
        result = forecast_summary({"COLECTIVO": fc})
        assert result["direction"].iloc[0] == "down"

    def test_direction_flat_when_similar(self):
        fc = self._make_forecast("COLECTIVO", actual_base=1_000_000, forecast_base=1_030_000)
        result = forecast_summary({"COLECTIVO": fc})
        assert result["direction"].iloc[0] == "flat"

    def test_pct_change_is_numeric(self):
        forecasts = {"COLECTIVO": self._make_forecast("COLECTIVO")}
        result = forecast_summary(forecasts)
        assert pd.api.types.is_numeric_dtype(result["pct_change"])

    def test_pct_change_sign_matches_direction(self):
        fc_up   = self._make_forecast("COLECTIVO", actual_base=1_000_000, forecast_base=1_200_000)
        fc_down = self._make_forecast("TREN",      actual_base=1_000_000, forecast_base=800_000)
        result = forecast_summary({"COLECTIVO": fc_up, "TREN": fc_down})
        up_row   = result[result["mode"] == "COLECTIVO"].iloc[0]
        down_row = result[result["mode"] == "TREN"].iloc[0]
        assert up_row["pct_change"] > 0
        assert down_row["pct_change"] < 0

    def test_empty_forecasts_returns_empty_df(self):
        result = forecast_summary({})
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_mode_with_no_actuals_skipped(self):
        """A mode whose forecast has no actual rows should be excluded gracefully."""
        fc = self._make_forecast("COLECTIVO", n_actual=0, n_forecast=6)
        # When n_actual=0, actual_rows will be empty — should be skipped
        result = forecast_summary({"COLECTIVO": fc})
        assert len(result) == 0

    def test_last_actual_is_last_observed_value(self):
        fc = self._make_forecast("COLECTIVO", actual_base=999_000)
        result = forecast_summary({"COLECTIVO": fc})
        assert result["last_actual"].iloc[0] == 999_000

    def test_mean_forecast_is_average_of_future_rows(self):
        fc = self._make_forecast("COLECTIVO", actual_base=1_000_000, forecast_base=1_200_000)
        result = forecast_summary({"COLECTIVO": fc})
        assert abs(result["mean_forecast"].iloc[0] - 1_200_000) < 1


# ── forecast_ridership (smoke test, Prophet optional) ─────────────────────

class TestForecastRidership:
    prophet = pytest.importorskip("prophet", reason="prophet not installed")

    @pytest.fixture
    def conn_for_forecast(self):
        """In-memory DuckDB with ≥24 months of monthly data per mode."""
        rows = []
        for year in range(2022, 2025):
            for month in range(1, 13):
                for modo, base in [("COLECTIVO", 1_500_000), ("TREN", 600_000), ("SUBTE", 900_000)]:
                    rows.append({
                        "month_start":    pd.Timestamp(year=year, month=month, day=1),
                        "year":           year,
                        "month":          month,
                        "modo":           modo,
                        "total_usos":     base + np.random.randint(-50_000, 50_000),
                        "avg_daily_usos": base / 30,
                        "days_with_data": 28,
                        "suspicious_days": 0,
                    })
        df = pd.DataFrame(rows)
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE monthly_transactions AS SELECT * FROM df")
        return conn

    def test_returns_dict(self, conn_for_forecast):
        from analytics.ml import forecast_ridership
        result = forecast_ridership(conn_for_forecast, modes=["COLECTIVO"], horizon=3)
        assert isinstance(result, dict)

    def test_returns_result_for_requested_mode(self, conn_for_forecast):
        from analytics.ml import forecast_ridership
        result = forecast_ridership(conn_for_forecast, modes=["COLECTIVO"], horizon=3)
        assert "COLECTIVO" in result

    def test_forecast_df_has_required_columns(self, conn_for_forecast):
        from analytics.ml import forecast_ridership
        result = forecast_ridership(conn_for_forecast, modes=["COLECTIVO"], horizon=3)
        fc = result["COLECTIVO"]
        for col in ("ds", "yhat", "yhat_lower", "yhat_upper", "is_forecast"):
            assert col in fc.columns

    def test_horizon_controls_future_rows(self, conn_for_forecast):
        from analytics.ml import forecast_ridership
        result = forecast_ridership(conn_for_forecast, modes=["COLECTIVO"], horizon=4)
        fc = result["COLECTIVO"]
        n_future = fc["is_forecast"].sum()
        assert n_future == 4

    def test_skips_mode_with_insufficient_data(self):
        """Modes with < 24 months of data should be skipped, not crash."""
        rows = []
        for month in range(1, 13):  # only 12 months
            rows.append({
                "month_start":    pd.Timestamp(year=2024, month=month, day=1),
                "year":           2024,
                "month":          month,
                "modo":           "COLECTIVO",
                "total_usos":     1_000_000,
                "avg_daily_usos": 33_333,
                "days_with_data": 28,
                "suspicious_days": 0,
            })
        df = pd.DataFrame(rows)
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE monthly_transactions AS SELECT * FROM df")

        from analytics.ml import forecast_ridership
        result = forecast_ridership(conn, modes=["COLECTIVO"], horizon=3)
        assert "COLECTIVO" not in result