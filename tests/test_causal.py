"""
tests/test_causal.py — Tests for analytics/causal.py

Pure helpers (_cumulative_hike_pct, _build_its_features) are fully unit-tested
without any ML dependencies.

its_analysis and build_counterfactual_df require statsmodels and are run as
smoke tests (skipped if statsmodels is not installed).
"""

import duckdb
import numpy as np
import pandas as pd
import pytest

from analytics.causal import (
    TREATMENT_DATE,
    WB_ELASTICITY,
    _build_its_features,
    _cumulative_hike_pct,
    build_counterfactual_df,
)


# ── Helpers ────────────────────────────────────────────────────────────────

def _monthly_df(start: str, periods: int, base: float = 1_000_000) -> pd.DataFrame:
    """Build a minimal (ds, y) DataFrame for ITS feature tests."""
    return pd.DataFrame({
        "ds": pd.date_range(start, periods=periods, freq="MS"),
        "y":  [base + i * 1000 for i in range(periods)],
    })


def _in_memory_conn(n_months: int = 72, base: float = 1_000_000) -> duckdb.DuckDBPyConnection:
    """
    In-memory DuckDB with monthly_transactions covering n_months ending after
    TREATMENT_DATE (so each mode has ≥6 post-treatment rows).
    """
    start = pd.Timestamp("2018-01-01")
    rows = []
    for i in range(n_months):
        ds = start + pd.DateOffset(months=i)
        for modo, b in [("COLECTIVO", base), ("TREN", base * 0.4), ("SUBTE", base * 0.6)]:
            rows.append({
                "month_start":     ds,
                "year":            ds.year,
                "month":           ds.month,
                "modo":            modo,
                "total_usos":      int(b + np.random.randint(-20_000, 20_000)),
                "avg_daily_usos":  b / 30,
                "days_with_data":  28,
                "suspicious_days": 0,
            })
    df = pd.DataFrame(rows)
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE monthly_transactions AS SELECT * FROM df")
    return conn


# ── _cumulative_hike_pct ───────────────────────────────────────────────────

class TestCumulativeHikePct:
    def test_returns_float(self):
        result = _cumulative_hike_pct(
            pd.Timestamp("2023-12-31"),
            pd.Timestamp("2024-02-28"),
        )
        assert isinstance(result, float)

    def test_positive_for_jan_feb_2024_shock(self):
        """The Jan–Feb 2024 fare shock must produce a positive cumulative hike."""
        result = _cumulative_hike_pct(
            pd.Timestamp("2023-12-31"),
            pd.Timestamp("2024-02-28"),
        )
        assert result > 0

    def test_jan_feb_2024_exceeds_100_pct(self):
        """Jan +45% then Feb +66% compounds to >100%."""
        result = _cumulative_hike_pct(
            pd.Timestamp("2023-12-31"),
            pd.Timestamp("2024-02-28"),
        )
        assert result > 100.0

    def test_zero_when_no_hikes_in_window(self):
        """A window with no hikes should return 0."""
        # 2015-01 to 2016-03-31 is before the first known fare hike (2016-04-01)
        result = _cumulative_hike_pct(
            pd.Timestamp("2015-01-01"),
            pd.Timestamp("2016-03-31"),
        )
        assert result == pytest.approx(0.0)

    def test_from_date_is_exclusive(self):
        """A hike exactly on from_date must NOT be counted."""
        from config import FARE_HIKES
        first_hike = min(FARE_HIKES, key=lambda h: h["date"])
        hike_date = pd.Timestamp(first_hike["date"])
        # from_date == hike_date → hike should be excluded (strict >)
        result = _cumulative_hike_pct(hike_date, hike_date + pd.Timedelta(days=1))
        assert result == pytest.approx(0.0)

    def test_to_date_is_inclusive(self):
        """A hike exactly on to_date must be counted."""
        from config import FARE_HIKES
        first_hike = min(FARE_HIKES, key=lambda h: h["date"])
        hike_date = pd.Timestamp(first_hike["date"])
        if first_hike["magnitude"] == 0:
            pytest.skip("First hike has magnitude 0 — cannot test inclusion")
        result = _cumulative_hike_pct(hike_date - pd.Timedelta(days=1), hike_date)
        assert result > 0.0

    def test_compounding_is_multiplicative(self):
        """Two successive +10% hikes should compound to +21%, not +20%."""
        from unittest.mock import patch
        fake_hikes = [
            {"date": "2024-01-15", "magnitude": 10, "scope": "national"},
            {"date": "2024-02-15", "magnitude": 10, "scope": "national"},
        ]
        with patch("analytics.causal.FARE_HIKES", fake_hikes):
            result = _cumulative_hike_pct(
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-03-01"),
                scopes={"national"},
            )
        assert result == pytest.approx(21.0, abs=0.01)

    def test_scope_filter_excludes_other_scopes(self):
        """Hikes outside the requested scopes must not be counted."""
        from unittest.mock import patch
        fake_hikes = [
            {"date": "2024-01-15", "magnitude": 50, "scope": "interior"},
        ]
        with patch("analytics.causal.FARE_HIKES", fake_hikes):
            result = _cumulative_hike_pct(
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-02-01"),
                scopes={"national"},
            )
        assert result == pytest.approx(0.0)


# ── _build_its_features ────────────────────────────────────────────────────

class TestBuildItsFeatures:
    TREATMENT = pd.Timestamp("2024-01-01")

    def test_returns_dataframe(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        assert isinstance(result, pd.DataFrame)

    def test_does_not_mutate_input(self):
        df = _monthly_df("2022-01-01", 36)
        original_cols = set(df.columns)
        _build_its_features(df, self.TREATMENT)
        assert set(df.columns) == original_cols

    def test_adds_t_column(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        assert "t" in result.columns

    def test_t_starts_at_zero(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        assert result["t"].iloc[0] == 0

    def test_t_is_contiguous_integers(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        expected = list(range(len(df)))
        assert list(result["t"]) == expected

    def test_adds_D_column(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        assert "D" in result.columns

    def test_D_is_binary(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        assert set(result["D"].unique()).issubset({0, 1})

    def test_D_zero_before_treatment(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        pre = result[result["ds"] < self.TREATMENT]
        assert (pre["D"] == 0).all()

    def test_D_one_at_and_after_treatment(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        post = result[result["ds"] >= self.TREATMENT]
        assert (post["D"] == 1).all()

    def test_adds_t_post_column(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        assert "t_post" in result.columns

    def test_t_post_zero_before_treatment(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        pre = result[result["ds"] < self.TREATMENT]
        assert (pre["t_post"] == 0).all()

    def test_t_post_zero_at_treatment_month(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        at = result[result["ds"] == self.TREATMENT]
        if len(at):
            assert at["t_post"].iloc[0] == 0

    def test_t_post_increments_after_treatment(self):
        df = _monthly_df("2022-01-01", 36)
        result = _build_its_features(df, self.TREATMENT)
        post = result[result["ds"] > self.TREATMENT]
        if len(post) >= 2:
            diffs = post["t_post"].diff().dropna()
            assert (diffs == 1).all()

    def test_adds_covid_column(self):
        df = _monthly_df("2019-01-01", 72)
        result = _build_its_features(df, self.TREATMENT)
        assert "covid" in result.columns

    def test_covid_is_binary(self):
        df = _monthly_df("2019-01-01", 72)
        result = _build_its_features(df, self.TREATMENT)
        assert set(result["covid"].unique()).issubset({0, 1})

    def test_covid_one_during_window(self):
        df = pd.DataFrame({"ds": [
            pd.Timestamp("2020-06-01"),
            pd.Timestamp("2021-06-01"),
        ], "y": [1_000_000, 1_000_000]})
        result = _build_its_features(df, self.TREATMENT)
        assert (result["covid"] == 1).all()

    def test_covid_zero_outside_window(self):
        df = pd.DataFrame({"ds": [
            pd.Timestamp("2019-01-01"),
            pd.Timestamp("2022-06-01"),
        ], "y": [1_000_000, 1_000_000]})
        result = _build_its_features(df, self.TREATMENT)
        assert (result["covid"] == 0).all()

    def test_month_dummies_present(self):
        df = _monthly_df("2022-01-01", 24)
        result = _build_its_features(df, self.TREATMENT)
        dummy_cols = [c for c in result.columns if c.startswith("m_")]
        assert len(dummy_cols) > 0

    def test_january_reference_month_dropped(self):
        """m_1 must be dropped — January is the reference category."""
        df = _monthly_df("2022-01-01", 24)
        result = _build_its_features(df, self.TREATMENT)
        assert "m_1" not in result.columns

    def test_sorted_by_ds(self):
        """Output must be sorted chronologically regardless of input order."""
        df = _monthly_df("2022-01-01", 12).sample(frac=1, random_state=42)
        result = _build_its_features(df, self.TREATMENT)
        assert list(result["ds"]) == sorted(result["ds"])


# ── constants ──────────────────────────────────────────────────────────────

class TestConstants:
    def test_treatment_date_is_jan_2024(self):
        assert TREATMENT_DATE == pd.Timestamp("2024-01-01")

    def test_wb_elasticity_is_negative(self):
        assert WB_ELASTICITY < 0

    def test_wb_elasticity_value(self):
        assert WB_ELASTICITY == pytest.approx(-0.12)


# ── its_analysis (smoke test, statsmodels optional) ────────────────────────

statsmodels = pytest.importorskip("statsmodels", reason="statsmodels not installed")


class TestItsAnalysis:
    from analytics.causal import its_analysis

    @pytest.fixture
    def conn(self):
        return _in_memory_conn(n_months=84)  # 7 years — plenty of pre/post data

    def test_returns_dataframe(self, conn):
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["COLECTIVO"])
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, conn):
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["COLECTIVO"])
        for col in (
            "mode", "n_obs", "n_post",
            "beta_level", "pvalue_level",
            "beta_slope", "pvalue_slope",
            "pre_mean", "pct_level_change",
            "implied_elasticity", "wb_elasticity",
            "r2", "se_type",
        ):
            assert col in result.columns, f"Missing column: {col}"

    def test_one_row_per_mode(self, conn):
        from analytics.causal import its_analysis
        modes = ["COLECTIVO", "TREN"]
        result = its_analysis(conn, modes=modes)
        assert len(result) == len(modes)
        assert set(result["mode"]) == set(modes)

    def test_r2_between_zero_and_one(self, conn):
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["COLECTIVO"])
        r2 = result["r2"].iloc[0]
        assert 0.0 <= r2 <= 1.0

    def test_pvalues_between_zero_and_one(self, conn):
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["COLECTIVO"])
        for col in ("pvalue_level", "pvalue_slope"):
            val = result[col].iloc[0]
            assert 0.0 <= val <= 1.0, f"{col} = {val} out of range"

    def test_n_post_correct(self, conn):
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["COLECTIVO"])
        # With 84 months from 2018-01, treatment is Jan 2024 — 12 post months
        n_post = result["n_post"].iloc[0]
        assert n_post >= 6

    def test_skips_mode_with_too_few_post_months(self):
        """Mode with <6 post-treatment months should be skipped."""
        # Only 3 months total — 0 post-treatment rows
        conn = _in_memory_conn(n_months=3)
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["COLECTIVO"])
        assert len(result) == 0

    def test_df_and_result_cols_present(self, conn):
        """_df and _result columns are needed for counterfactual plotting."""
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["COLECTIVO"])
        assert "_df" in result.columns
        assert "_result" in result.columns

    def test_se_type_hac_for_tren_subte(self, conn):
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["TREN", "SUBTE"])
        for _, row in result.iterrows():
            assert row["se_type"] == "HAC-12"

    def test_se_type_ols_for_colectivo(self, conn):
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=["COLECTIVO"])
        assert result["se_type"].iloc[0] == "OLS"

    def test_empty_result_when_no_modes(self, conn):
        from analytics.causal import its_analysis
        result = its_analysis(conn, modes=[])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# ── build_counterfactual_df (statsmodels required) ─────────────────────────

class TestBuildCounterfactualDf:
    @pytest.fixture
    def its_row(self):
        conn = _in_memory_conn(n_months=84)
        from analytics.causal import its_analysis
        results = its_analysis(conn, modes=["COLECTIVO"])
        assert not results.empty, "its_analysis returned no rows — fixture broken"
        return results.iloc[0]

    def test_returns_dataframe(self, its_row):
        result = build_counterfactual_df(its_row)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, its_row):
        result = build_counterfactual_df(its_row)
        for col in ("ds", "actual", "fitted", "counterfactual", "gap", "post"):
            assert col in result.columns, f"Missing column: {col}"

    def test_gap_equals_actual_minus_counterfactual(self, its_row):
        result = build_counterfactual_df(its_row)
        computed_gap = result["actual"] - result["counterfactual"]
        pd.testing.assert_series_equal(
            result["gap"].reset_index(drop=True),
            computed_gap.reset_index(drop=True),
            check_names=False,
        )

    def test_post_column_is_binary(self, its_row):
        result = build_counterfactual_df(its_row)
        assert set(result["post"].unique()).issubset({0, 1})

    def test_ds_is_datetime(self, its_row):
        result = build_counterfactual_df(its_row)
        assert pd.api.types.is_datetime64_any_dtype(result["ds"])

    def test_counterfactual_differs_from_fitted_in_post_period(self, its_row):
        """
        In the post period, the counterfactual (D=0, t_post=0) must differ
        from the fitted values (which include the treatment terms) unless
        both coefficients happen to be zero.
        """
        result = build_counterfactual_df(its_row)
        post = result[result["post"] == 1]
        if len(post) == 0:
            pytest.skip("No post-treatment rows in fixture data")
        # If both betas are non-zero the series should not be identical
        if its_row["beta_level"] != 0 or its_row["beta_slope"] != 0:
            assert not np.allclose(post["fitted"].values, post["counterfactual"].values)
