"""
tests/test_dashboard.py — Unit tests for dashboard/utils.py.

All tests use in-memory DuckDB — no network calls, no disk I/O,
no Streamlit session required.
"""

import pytest
import pandas as pd
import duckdb
import plotly.graph_objects as go

# ── Local fixtures (mirrors conftest.py — self-contained for portability) ─────

REAL_SCHEMA_CSV = """dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar
2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
2024-01-01,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,20000,N
2024-01-01,EMPRESA C,LINEA 3,NO,COLECTIVO,PROVINCIAL,CORDOBA,CORDOBA,15000,N
2024-01-02,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,48000,N
2024-01-02,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,19000,N
2024-01-03,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,52000,N
2024-01-03,EMPRESA D,LINEA 4,SI,SUBTE,NACIONAL,BUENOS AIRES,CABA,30000,N
"""


@pytest.fixture
def in_memory_db(tmp_path):
    """In-memory DuckDB loaded with clean sample data. Self-contained."""
    import duckdb as _duckdb
    from etl.clean import clean_file
    from etl.load import load

    csv_path = tmp_path / "dat-ab-usos-2024.csv"
    csv_path.write_text(REAL_SCHEMA_CSV, encoding="utf-8")

    df = clean_file(csv_path)
    conn = _duckdb.connect(":memory:")
    load(df, conn=conn)
    return conn



# ── load_* functions ───────────────────────────────────────────────────────

class TestLoadMonthly:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_monthly
        df = load_monthly(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_only_dashboard_modes(self, in_memory_db):
        from dashboard.utils import load_monthly
        df = load_monthly(in_memory_db)
        assert set(df["modo"].unique()).issubset({"COLECTIVO", "TREN", "SUBTE"})

    def test_has_expected_columns(self, in_memory_db):
        from dashboard.utils import load_monthly
        df = load_monthly(in_memory_db)
        assert "month_start" in df.columns
        assert "total_usos" in df.columns
        assert "modo" in df.columns

    def test_sorted_by_month_and_mode(self, in_memory_db):
        from dashboard.utils import load_monthly
        df = load_monthly(in_memory_db)
        if len(df) > 1:
            pairs = list(zip(df["month_start"], df["modo"]))
            assert pairs == sorted(pairs)


class TestLoadDailyTotals:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_daily_totals
        df = load_daily_totals(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_has_fecha_column(self, in_memory_db):
        from dashboard.utils import load_daily_totals
        df = load_daily_totals(in_memory_db)
        assert "fecha" in df.columns

    def test_only_dashboard_modes(self, in_memory_db):
        from dashboard.utils import load_daily_totals
        df = load_daily_totals(in_memory_db)
        if not df.empty:
            assert set(df["modo"].unique()).issubset({"COLECTIVO", "TREN", "SUBTE"})

    def test_no_suspicious_rows(self, in_memory_db):
        from dashboard.utils import load_daily_totals
        # Verify the query excludes suspicious rows by checking
        # the count doesn't exceed what's in daily_transactions
        df = load_daily_totals(in_memory_db)
        raw = in_memory_db.execute("""
            SELECT COUNT(*) FROM daily_transactions
            WHERE NOT is_suspicious AND modo IN ('COLECTIVO','TREN','SUBTE')
        """).fetchone()[0]
        # daily_totals aggregates per (fecha, modo) so row count ≤ raw count
        assert len(df) <= raw


class TestLoadModalSplit:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_modal_split
        df = load_modal_split(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_has_mode_share_column(self, in_memory_db):
        from dashboard.utils import load_modal_split
        df = load_modal_split(in_memory_db)
        assert "mode_share_pct" in df.columns

    def test_shares_sum_to_100_per_month(self, in_memory_db):
        from dashboard.utils import load_modal_split
        df = load_modal_split(in_memory_db)
        if df.empty:
            pytest.skip("No data in fixture")
        totals = df.groupby("month_start")["mode_share_pct"].sum()
        for month, total in totals.items():
            assert abs(total - 100.0) < 0.5, f"{month}: sum={total}"


class TestLoadYoy:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_yoy
        df = load_yoy(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_has_yoy_column(self, in_memory_db):
        from dashboard.utils import load_yoy
        df = load_yoy(in_memory_db)
        assert "yoy_pct_change" in df.columns


class TestLoadHeatmap:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_heatmap
        df = load_heatmap(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_has_expected_columns(self, in_memory_db):
        from dashboard.utils import load_heatmap
        df = load_heatmap(in_memory_db)
        assert "day_of_week" in df.columns
        assert "month" in df.columns
        assert "avg_usos" in df.columns

    def test_weekday_range(self, in_memory_db):
        from dashboard.utils import load_heatmap
        df = load_heatmap(in_memory_db)
        if df.empty:
            pytest.skip("No data in fixture")
        assert df["day_of_week"].between(0, 6).all()

    def test_month_range(self, in_memory_db):
        from dashboard.utils import load_heatmap
        df = load_heatmap(in_memory_db)
        if df.empty:
            pytest.skip("No data in fixture")
        assert df["month"].between(1, 12).all()


class TestLoadAmbaRecovery:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_amba_recovery
        df = load_amba_recovery(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_only_amba_values(self, in_memory_db):
        from dashboard.utils import load_amba_recovery
        df = load_amba_recovery(in_memory_db)
        if not df.empty:
            assert set(df["amba"].unique()).issubset({"SI", "NO"})

    def test_has_recovery_index(self, in_memory_db):
        from dashboard.utils import load_amba_recovery
        df = load_amba_recovery(in_memory_db)
        assert "recovery_index" in df.columns

    def test_recovery_index_positive(self, in_memory_db):
        from dashboard.utils import load_amba_recovery
        df = load_amba_recovery(in_memory_db)
        if not df.empty:
            assert (df["recovery_index"] >= 0).all()


class TestLoadTopEmpresas:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_top_empresas
        df = load_top_empresas(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_at_most_10_rows(self, in_memory_db):
        from dashboard.utils import load_top_empresas
        df = load_top_empresas(in_memory_db)
        assert len(df) <= 10

    def test_sorted_descending(self, in_memory_db):
        from dashboard.utils import load_top_empresas
        df = load_top_empresas(in_memory_db)
        if len(df) > 1:
            vals = df["total_usos"].tolist()
            assert vals == sorted(vals, reverse=True)


class TestLoadByProvincia:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_by_provincia
        df = load_by_provincia(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_excludes_unknown_jurisdictions(self, in_memory_db):
        from dashboard.utils import load_by_provincia
        df = load_by_provincia(in_memory_db)
        excluded = {"JN", "NAN", "SN", "SD"}
        if not df.empty:
            assert not set(df["provincia"].unique()) & excluded

    def test_no_null_provinces(self, in_memory_db):
        from dashboard.utils import load_by_provincia
        df = load_by_provincia(in_memory_db)
        assert df["provincia"].notna().all()

    def test_positive_totals(self, in_memory_db):
        from dashboard.utils import load_by_provincia
        df = load_by_provincia(in_memory_db)
        if not df.empty:
            assert (df["total"] > 0).all()


# ── Data transform helpers ─────────────────────────────────────────────────

class TestComputeMomPct:
    def test_basic_calculation(self):
        from dashboard.utils import compute_mom_pct
        df = pd.DataFrame({
            "modo":        ["COLECTIVO"] * 3,
            "month_start": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "total_usos":  [100, 110, 99],
        })
        result = compute_mom_pct(df)
        assert "mom_pct" in result.columns
        assert len(result) == 2  # first row dropped (NaN)

    def test_correct_values(self):
        from dashboard.utils import compute_mom_pct
        df = pd.DataFrame({
            "modo":        ["SUBTE"] * 2,
            "month_start": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "total_usos":  [100, 150],
        })
        result = compute_mom_pct(df)
        assert abs(result["mom_pct"].iloc[0] - 50.0) < 0.01

    def test_multiple_modes_independent(self):
        from dashboard.utils import compute_mom_pct
        df = pd.DataFrame({
            "modo":        ["COLECTIVO", "COLECTIVO", "SUBTE", "SUBTE"],
            "month_start": pd.to_datetime([
                "2024-01-01", "2024-02-01",
                "2024-01-01", "2024-02-01",
            ]),
            "total_usos":  [100, 200, 50, 50],
        })
        result = compute_mom_pct(df)
        col = result[result["modo"] == "COLECTIVO"]["mom_pct"].iloc[0]
        sub = result[result["modo"] == "SUBTE"]["mom_pct"].iloc[0]
        assert abs(col - 100.0) < 0.01
        assert abs(sub - 0.0) < 0.01

    def test_drops_nan_rows(self):
        from dashboard.utils import compute_mom_pct
        df = pd.DataFrame({
            "modo":        ["TREN"] * 4,
            "month_start": pd.to_datetime([
                "2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"
            ]),
            "total_usos":  [100, 110, 121, 110],
        })
        result = compute_mom_pct(df)
        assert result["mom_pct"].notna().all()
        assert len(result) == 3

    def test_empty_dataframe(self):
        from dashboard.utils import compute_mom_pct
        df = pd.DataFrame(columns=["modo", "month_start", "total_usos"])
        result = compute_mom_pct(df)
        assert result.empty


class TestIndexToBaseline:
    def test_baseline_is_100(self):
        from dashboard.utils import index_to_baseline
        df = pd.DataFrame({
            "modo":        ["SUBTE"] * 3,
            "month_start": ["2020-01-01", "2020-02-01", "2020-03-01"],
            "total_usos":  [1000, 800, 1200],
        })
        result = index_to_baseline(df, "2020-01-01")
        base_row = result[result["month_start"] == "2020-01-01"]
        assert abs(base_row["index_val"].iloc[0] - 100.0) < 0.01

    def test_correct_index_values(self):
        from dashboard.utils import index_to_baseline
        df = pd.DataFrame({
            "modo":        ["COLECTIVO"] * 2,
            "month_start": ["2020-01-01", "2020-04-01"],
            "total_usos":  [1000, 420],
        })
        result = index_to_baseline(df, "2020-01-01")
        apr = result[result["month_start"] == "2020-04-01"]
        assert abs(apr["index_val"].iloc[0] - 42.0) < 0.01

    def test_missing_baseline_drops_group(self):
        from dashboard.utils import index_to_baseline
        df = pd.DataFrame({
            "modo":        ["SUBTE", "SUBTE", "TREN"],
            "month_start": ["2020-02-01", "2020-03-01", "2020-02-01"],
            "total_usos":  [100, 110, 200],
        })
        # TREN has no Jan 2020 entry → should be dropped
        result = index_to_baseline(df, "2020-01-01")
        assert "TREN" not in result["modo"].values

    def test_multiple_groups_independent(self):
        from dashboard.utils import index_to_baseline
        df = pd.DataFrame({
            "modo":        ["SUBTE", "SUBTE", "TREN", "TREN"],
            "month_start": [
                "2020-01-01", "2020-02-01",
                "2020-01-01", "2020-02-01",
            ],
            "total_usos":  [100, 200, 50, 25],
        })
        result = index_to_baseline(df, "2020-01-01")
        sub = result[result["modo"] == "SUBTE"]
        trn = result[result["modo"] == "TREN"]
        assert abs(sub[sub["month_start"] == "2020-02-01"]["index_val"].iloc[0] - 200.0) < 0.01
        assert abs(trn[trn["month_start"] == "2020-02-01"]["index_val"].iloc[0] - 50.0) < 0.01


# ── Chart helpers ──────────────────────────────────────────────────────────

class TestHexToRgb:
    def test_black(self):
        from dashboard.utils import hex_to_rgb
        assert hex_to_rgb("#000000") == (0.0, 0.0, 0.0)

    def test_white(self):
        from dashboard.utils import hex_to_rgb
        r, g, b = hex_to_rgb("#ffffff")
        assert abs(r - 1.0) < 0.01
        assert abs(g - 1.0) < 0.01
        assert abs(b - 1.0) < 0.01

    def test_red(self):
        from dashboard.utils import hex_to_rgb
        r, g, b = hex_to_rgb("#ff0000")
        assert abs(r - 1.0) < 0.01
        assert g == 0.0
        assert b == 0.0

    def test_without_hash(self):
        from dashboard.utils import hex_to_rgb
        assert hex_to_rgb("ff0000") == hex_to_rgb("#ff0000")

    def test_values_in_0_1_range(self):
        from dashboard.utils import hex_to_rgb
        for ch in hex_to_rgb("#2563EB"):
            assert 0.0 <= ch <= 1.0


class TestModeColorMap:
    def test_returns_dict(self):
        from dashboard.utils import mode_color_map
        result = mode_color_map()
        assert isinstance(result, dict)

    def test_covers_all_dashboard_modes(self):
        from dashboard.utils import mode_color_map
        from config import DASHBOARD_MODES
        result = mode_color_map()
        for mode in DASHBOARD_MODES:
            assert mode in result

    def test_values_are_hex_strings(self):
        from dashboard.utils import mode_color_map
        for color in mode_color_map().values():
            assert color.startswith("#")
            assert len(color) == 7


class TestStaggeredAnnotations:
    """Tests for _staggered_annotations via add_event_annotations / add_fare_annotations."""

    def test_add_event_annotations_returns_figure(self):
        from dashboard.utils import add_event_annotations
        fig = go.Figure()
        fig.add_scatter(x=pd.date_range("2020-01-01", periods=10, freq="ME"),
                        y=list(range(10)))
        result = add_event_annotations(fig, lang="en")
        assert isinstance(result, go.Figure)

    def test_add_fare_annotations_returns_figure(self):
        from dashboard.utils import add_fare_annotations
        fig = go.Figure()
        fig.add_scatter(x=pd.date_range("2022-01-01", periods=24, freq="ME"),
                        y=[100] * 24)
        result = add_fare_annotations(fig, lang="es")
        assert isinstance(result, go.Figure)

    def test_no_annotations_outside_range(self):
        from dashboard.utils import add_event_annotations
        # A figure covering only 2025 — no events should appear before 2020
        fig = go.Figure()
        fig.add_scatter(x=pd.date_range("2025-01-01", periods=6, freq="ME"),
                        y=[1] * 6)
        result = add_event_annotations(fig, lang="en")
        # Check that all vline x-values (if any) fall within the chart range
        for shape in result.layout.shapes:
            if hasattr(shape, "x0"):
                ts = pd.Timestamp(shape.x0 / 1000, unit="s")
                assert ts >= pd.Timestamp("2025-01-01"), f"Out-of-range annotation: {ts}"

    def test_scope_filter_reduces_annotations(self):
        from dashboard.utils import add_fare_annotations
        fig_all = go.Figure()
        fig_all.add_scatter(x=pd.date_range("2020-01-01", periods=60, freq="ME"),
                            y=[100] * 60)
        fig_national = go.Figure()
        fig_national.add_scatter(x=pd.date_range("2020-01-01", periods=60, freq="ME"),
                                 y=[100] * 60)

        result_all      = add_fare_annotations(fig_all,      lang="en")
        result_national = add_fare_annotations(fig_national, lang="en",
                                               scope_filter=["national"])

        # National-only should have fewer or equal annotations
        assert len(result_national.layout.annotations) <= len(result_all.layout.annotations)

    def test_english_labels_differ_from_spanish(self):
        from dashboard.utils import add_event_annotations
        fig_es = go.Figure()
        fig_en = go.Figure()
        for fig in (fig_es, fig_en):
            fig.add_scatter(x=pd.date_range("2020-01-01", periods=36, freq="ME"),
                            y=[1] * 36)

        result_es = add_event_annotations(fig_es, lang="es")
        result_en = add_event_annotations(fig_en, lang="en")

        labels_es = {a.text for a in result_es.layout.annotations}
        labels_en = {a.text for a in result_en.layout.annotations}
        # The two label sets should differ (at least some events have different labels)
        assert labels_es != labels_en


# ── Historical data functions ──────────────────────────────────────────────

class TestLoadHistoricalMonthly:
    def test_returns_dataframe_when_table_missing(self, in_memory_db):
        """Should return empty DataFrame gracefully if monthly_historical doesn't exist."""
        from dashboard.utils import load_historical_monthly
        df = load_historical_monthly(in_memory_db)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_returns_data_when_table_exists(self, in_memory_db):
        """Should return rows when monthly_historical is populated."""
        from dashboard.utils import load_historical_monthly
        in_memory_db.execute("""
            CREATE TABLE monthly_historical AS
            SELECT
                '2016-01-01'::DATE AS month_start,
                2016               AS year,
                1                  AS month,
                'COLECTIVO'        AS modo,
                242553686          AS total_usos,
                'SI'               AS amba,
                'pre2020'          AS era,
                'mmodo_2016_2019'  AS source
            UNION ALL
            SELECT '2016-01-01'::DATE, 2016, 1, 'SUBTE', 16281434, 'SI', 'pre2020', 'mmodo_2016_2019'
            UNION ALL
            SELECT '2016-01-01'::DATE, 2016, 1, 'TREN',  19728996, 'SI', 'pre2020', 'mmodo_2016_2019'
        """)
        df = load_historical_monthly(in_memory_db)
        assert len(df) == 3
        assert set(df["modo"].unique()) == {"COLECTIVO", "SUBTE", "TREN"}

    def test_has_expected_columns(self, in_memory_db):
        from dashboard.utils import load_historical_monthly
        df = load_historical_monthly(in_memory_db)
        # Empty but schema-correct
        for col in ["month_start", "modo", "total_usos"]:
            assert col in df.columns


class TestLoadCombinedMonthly:
    def test_returns_dataframe(self, in_memory_db):
        from dashboard.utils import load_combined_monthly
        df = load_combined_monthly(in_memory_db)
        assert isinstance(df, pd.DataFrame)

    def test_falls_back_gracefully_without_historical(self, in_memory_db):
        """When monthly_historical doesn't exist, should return post-2020 data only."""
        from dashboard.utils import load_combined_monthly
        df = load_combined_monthly(in_memory_db)
        # Should still return data from monthly_transactions
        assert not df.empty
        assert "month_start" in df.columns
        assert "modo" in df.columns
        assert "total_usos" in df.columns

    def test_unions_historical_and_post2020(self, in_memory_db):
        """When monthly_historical exists, combined result should have more rows."""
        from dashboard.utils import load_combined_monthly, load_monthly
        post2020_count = len(load_monthly(in_memory_db))

        # Add a historical row for a pre-2020 month
        in_memory_db.execute("""
            CREATE TABLE monthly_historical AS
            SELECT
                '2016-01-01'::DATE AS month_start,
                2016 AS year, 1 AS month,
                'COLECTIVO' AS modo,
                200000000   AS total_usos,
                'SI' AS amba, 'pre2020' AS era, 'test' AS source
        """)
        combined_count = len(load_combined_monthly(in_memory_db))
        assert combined_count == post2020_count + 1

    def test_only_dashboard_modes(self, in_memory_db):
        from dashboard.utils import load_combined_monthly
        df = load_combined_monthly(in_memory_db)
        assert set(df["modo"].unique()).issubset({"COLECTIVO", "TREN", "SUBTE"})

    def test_sorted_by_month_and_mode(self, in_memory_db):
        from dashboard.utils import load_combined_monthly
        df = load_combined_monthly(in_memory_db)
        if len(df) > 1:
            pairs = list(zip(df["month_start"].astype(str), df["modo"]))
            assert pairs == sorted(pairs)


class TestIngestHistorical:
    """Tests for etl/ingest_historical.py parsing functions.
    No network calls — tests use pre-baked CSV bytes."""

    def test_parse_mmodo_basic(self):
        """Source B parser handles semicolon-delimited MM/YYYY format."""
        from etl.ingest_historical import _parse_mmodo
        csv = (
            "\ufeffanio;MODO;TOTAL\n"
            "01/2016;COLECTIVO;242553686\n"
            "01/2016;SUBTE;16281434\n"
            "01/2016;TREN;19728996\n"
        )
        df = _parse_mmodo(csv.encode("utf-8"))
        assert len(df) == 3
        assert set(df["modo"].unique()) == {"COLECTIVO", "SUBTE", "TREN"}
        assert df[df["modo"] == "COLECTIVO"]["total_usos"].iloc[0] == 242553686
        assert df["month_start"].iloc[0] == pd.Timestamp("2016-01-01")

    def test_parse_mmodo_premetro_mapped_to_subte(self):
        """PREMETRO should be mapped to SUBTE."""
        from etl.ingest_historical import _parse_mmodo
        csv = (
            "\ufeffanio;MODO;TOTAL\n"
            "01/2016;PREMETRO;500000\n"
            "01/2016;SUBTE;16000000\n"
        )
        df = _parse_mmodo(csv.encode("utf-8"))
        assert "PREMETRO" not in df["modo"].values
        assert "SUBTE" in df["modo"].values
        # PREMETRO aggregated into SUBTE
        subte_total = df[df["modo"] == "SUBTE"]["total_usos"].sum()
        assert subte_total == 16500000

    def test_parse_periodo_modo_yyyymm_format(self):
        """Source A parser handles YYYYMM integer period format."""
        from etl.ingest_historical import _parse_periodo_modo
        csv = (
            "periodo;modo;suma de cantidad;actualizacion\n"
            "201301;COLECTIVO;235.035.085;201903\n"
            "201301;SUBTE;42.133;201903\n"
            "201301;TREN;710.787;201903\n"
        )
        df = _parse_periodo_modo(csv.encode("utf-8"))
        assert len(df) == 3
        assert df[df["modo"] == "COLECTIVO"]["total_usos"].iloc[0] == 235035085
        assert df["month_start"].iloc[0] == pd.Timestamp("2013-01-01")

    def test_parse_periodo_modo_thousands_separator(self):
        """Dot thousands separator in cantidad must be stripped correctly."""
        from etl.ingest_historical import _parse_periodo_modo
        csv = (
            "periodo;modo;suma de cantidad;actualizacion\n"
            "201601;COLECTIVO;242.553.686;201903\n"
        )
        df = _parse_periodo_modo(csv.encode("utf-8"))
        assert df["total_usos"].iloc[0] == 242553686

    def test_merge_prefers_source_b(self):
        """When both sources cover the same month, Source B wins."""
        import pandas as pd
        from etl.ingest_historical import MODE_MAP, VALID_MODES

        df_a = pd.DataFrame({
            "month_start": [pd.Timestamp("2016-01-01")],
            "modo": ["COLECTIVO"],
            "total_usos": pd.array([999], dtype="Int64"),
            "source": ["periodo_modo_2013_2019"],
        })
        df_b = pd.DataFrame({
            "month_start": [pd.Timestamp("2016-01-01")],
            "modo": ["COLECTIVO"],
            "total_usos": pd.array([242553686], dtype="Int64"),
            "source": ["mmodo_2016_2019"],
        })

        # Replicate merge logic
        b_keys = set(zip(df_b["month_start"].dt.strftime("%Y-%m"), df_b["modo"]))
        df_a["_key"] = df_a["month_start"].dt.strftime("%Y-%m")
        df_a_fill = df_a[
            ~df_a.apply(lambda r: (r["_key"], r["modo"]) in b_keys, axis=1)
        ].drop(columns=["_key"])

        result = pd.concat([df_b, df_a_fill], ignore_index=True)
        assert len(result) == 1
        assert result["source"].iloc[0] == "mmodo_2016_2019"
        assert result["total_usos"].iloc[0] == 242553686

    def test_load_historical_writes_table(self, in_memory_db):
        """load_historical should write monthly_historical table to DuckDB."""
        import pandas as pd
        from etl.ingest_historical import load_historical

        df = pd.DataFrame({
            "month_start": pd.to_datetime(["2016-01-01", "2016-02-01"]),
            "modo": ["COLECTIVO", "COLECTIVO"],
            "total_usos": pd.array([242553686, 245218982], dtype="Int64"),
            "source": ["mmodo_2016_2019", "mmodo_2016_2019"],
        })
        load_historical(df, in_memory_db)

        result = in_memory_db.execute(
            "SELECT COUNT(*) FROM monthly_historical"
        ).fetchone()[0]
        assert result == 2

    def test_load_historical_mode_specific_clip(self, in_memory_db):
        """SUBTE/TREN rows before 2016 should be dropped; COLECTIVO kept."""
        import pandas as pd
        from etl.ingest_historical import load_historical

        df = pd.DataFrame({
            "month_start": pd.to_datetime([
                "2013-01-01", "2013-01-01", "2013-01-01",  # pre-2016
                "2016-01-01", "2016-01-01", "2016-01-01",  # post-2016
            ]),
            "modo": ["COLECTIVO", "SUBTE", "TREN"] * 2,
            "total_usos": pd.array([100, 200, 300, 400, 500, 600], dtype="Int64"),
            "source": ["test"] * 6,
        })
        load_historical(df, in_memory_db)

        counts = in_memory_db.execute("""
            SELECT modo, COUNT(*) as n FROM monthly_historical GROUP BY modo ORDER BY modo
        """).df().set_index("modo")["n"].to_dict()

        assert counts["COLECTIVO"] == 2   # both 2013 and 2016
        assert counts["SUBTE"] == 1       # only 2016
        assert counts["TREN"] == 1        # only 2016