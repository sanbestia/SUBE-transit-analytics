"""
tests/test_load.py — Tests for etl/load.py

Uses in-memory DuckDB — no files written to disk.
"""

import pandas as pd
import duckdb
import pytest

from etl.load import _create_views, get_connection, load, query
from etl.clean import clean_file


# ── Helpers ────────────────────────────────────────────────────────────────

REAL_SCHEMA_CSV = """dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar
2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
2024-01-01,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,20000,N
2024-01-01,EMPRESA C,LINEA 3,NO,COLECTIVO,PROVINCIAL,CORDOBA,CORDOBA,15000,N
2024-01-02,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,48000,N
2024-01-02,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,19000,N
2024-01-03,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,52000,N
2024-01-03,EMPRESA D,LINEA 4,SI,SUBTE,NACIONAL,BUENOS AIRES,CABA,30000,N
2022-06-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,45000,N
2022-06-01,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,18000,N
2023-06-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,47000,N
2023-06-01,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,19500,N
"""


@pytest.fixture
def sample_df(tmp_path):
    path = tmp_path / "dat-ab-usos-2024.csv"
    path.write_text(REAL_SCHEMA_CSV)
    return clean_file(path)


@pytest.fixture
def loaded_conn(sample_df):
    conn = duckdb.connect(":memory:")
    load(sample_df, conn=conn)
    return conn


# ── load() ─────────────────────────────────────────────────────────────────

class TestLoad:
    def test_creates_daily_transactions_table(self, loaded_conn):
        tables = loaded_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "daily_transactions" in table_names

    def test_creates_monthly_transactions_table(self, loaded_conn):
        tables = loaded_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "monthly_transactions" in table_names

    def test_creates_monthly_by_provincia_table(self, loaded_conn):
        tables = loaded_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "monthly_by_provincia" in table_names

    def test_creates_top_empresas_table(self, loaded_conn):
        tables = loaded_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "top_empresas" in table_names

    def test_row_count_matches_input(self, sample_df, loaded_conn):
        n = loaded_conn.execute("SELECT COUNT(*) FROM daily_transactions").fetchone()[0]
        assert n == len(sample_df)

    def test_daily_transactions_has_enrichment_columns(self, loaded_conn):
        cols = [c[0] for c in loaded_conn.execute(
            "DESCRIBE daily_transactions"
        ).fetchall()]
        for col in ("nombre_empresa", "linea", "amba", "provincia", "municipio"):
            assert col in cols

    def test_null_enrichment_cols_coalesced_to_unknown(self, tmp_path):
        # Simple schema has no enrichment columns — they should default to UNKNOWN
        simple_csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-01,COLECTIVO,50000\n"
        path = tmp_path / "simple.csv"
        path.write_text(simple_csv)
        df = clean_file(path)

        conn = duckdb.connect(":memory:")
        load(df, conn=conn)
        row = conn.execute(
            "SELECT nombre_empresa FROM daily_transactions LIMIT 1"
        ).fetchone()
        assert row[0] == "UNKNOWN"

    def test_load_is_idempotent(self, sample_df):
        conn = duckdb.connect(":memory:")
        load(sample_df, conn=conn)
        load(sample_df, conn=conn)  # second load should replace, not append
        n = conn.execute("SELECT COUNT(*) FROM daily_transactions").fetchone()[0]
        assert n == len(sample_df)

    def test_monthly_aggregation_sums_correctly(self, loaded_conn):
        # Jan 2024 COLECTIVO:
        #   2024-01-01 EMPRESA A: 50000
        #   2024-01-01 EMPRESA C: 15000
        #   2024-01-02 EMPRESA A: 48000
        #   2024-01-03 EMPRESA A: 52000
        # Total = 165000 (none are suspicious)
        result = loaded_conn.execute("""
            SELECT SUM(total_usos)
            FROM monthly_transactions
            WHERE year = 2024 AND month = 1 AND modo = 'COLECTIVO'
        """).fetchone()[0]
        assert result == 165000

    def test_monthly_excludes_suspicious_rows(self, tmp_path):
        # Insert a weekday zero row — should be flagged suspicious and excluded from monthly
        csv = """FECHA,MODO,CANTIDAD_USOS
2024-01-02,COLECTIVO,0
2024-01-03,COLECTIVO,50000
"""
        path = tmp_path / "test.csv"
        path.write_text(csv)
        df = clean_file(path)
        conn = duckdb.connect(":memory:")
        load(df, conn=conn)
        result = conn.execute("""
            SELECT SUM(total_usos) FROM monthly_transactions
            WHERE year = 2024 AND month = 1 AND modo = 'COLECTIVO'
        """).fetchone()[0]
        # Only the 50000 row (Jan 3, Thursday) should be included
        assert result == 50000

    def test_daily_transactions_fecha_type(self, loaded_conn):
        dtype = loaded_conn.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name='daily_transactions' AND column_name='fecha'"
        ).fetchone()[0]
        assert dtype in ("DATE", "date")

    # ── monthly_by_provincia values ────────────────────────────────────────

    def test_monthly_by_provincia_amba_grouping(self, loaded_conn):
        # EMPRESA C / CORDOBA has amba='NO'; EMPRESA A/B/D have amba='SI'
        result = loaded_conn.execute("""
            SELECT amba, SUM(total_usos)
            FROM monthly_by_provincia
            WHERE month_start = '2024-01-01'
            GROUP BY amba
            ORDER BY amba
        """).fetchall()
        amba_map = {row[0]: row[1] for row in result}
        assert "SI" in amba_map
        assert "NO" in amba_map
        # CORDOBA COLECTIVO: 15000
        assert amba_map["NO"] == 15000

    def test_monthly_by_provincia_provincia_present(self, loaded_conn):
        provincias = {row[0] for row in loaded_conn.execute(
            "SELECT DISTINCT provincia FROM monthly_by_provincia"
        ).fetchall()}
        assert "BUENOS AIRES" in provincias
        assert "CORDOBA" in provincias

    # ── top_empresas values ────────────────────────────────────────────────

    def test_top_empresas_empresa_a_has_most_trips(self, loaded_conn):
        result = loaded_conn.execute(
            "SELECT nombre_empresa FROM top_empresas ORDER BY total_usos DESC LIMIT 1"
        ).fetchone()[0]
        # EMPRESA A appears in 2022, 2023, and 2024 — highest cumulative total
        assert result == "EMPRESA A"

    def test_top_empresas_total_usos_positive(self, loaded_conn):
        min_total = loaded_conn.execute(
            "SELECT MIN(total_usos) FROM top_empresas"
        ).fetchone()[0]
        assert min_total > 0

    def test_top_empresas_excludes_unknown(self, loaded_conn):
        unknown_count = loaded_conn.execute(
            "SELECT COUNT(*) FROM top_empresas WHERE nombre_empresa = 'UNKNOWN'"
        ).fetchone()[0]
        assert unknown_count == 0


# ── _create_views() ────────────────────────────────────────────────────────

class TestCreateViews:
    def test_v_total_daily_exists(self, loaded_conn):
        result = loaded_conn.execute("SELECT COUNT(*) FROM v_total_daily").fetchone()[0]
        assert result > 0

    def test_v_yoy_monthly_exists(self, loaded_conn):
        loaded_conn.execute("SELECT * FROM v_yoy_monthly LIMIT 1")

    def test_v_modal_split_exists(self, loaded_conn):
        loaded_conn.execute("SELECT * FROM v_modal_split LIMIT 1")

    def test_v_weekday_heatmap_exists(self, loaded_conn):
        loaded_conn.execute("SELECT * FROM v_weekday_heatmap LIMIT 1")

    def test_v_amba_vs_interior_exists(self, loaded_conn):
        loaded_conn.execute("SELECT * FROM v_amba_vs_interior LIMIT 1")

    def test_v_total_daily_aggregates_all_modes(self, loaded_conn):
        # On 2024-01-01: COLECTIVO(50000+15000) + TREN(20000) = 85000
        # SUBTE is not present on 2024-01-01 in the fixture
        result = loaded_conn.execute("""
            SELECT total_usos FROM v_total_daily
            WHERE fecha = '2024-01-01'
        """).fetchone()[0]
        assert result == 85000

    def test_v_modal_split_sums_to_100(self, loaded_conn):
        result = loaded_conn.execute("""
            SELECT ROUND(SUM(mode_share_pct), 0)
            FROM v_modal_split
            WHERE month_start = '2024-01-01'
        """).fetchone()[0]
        assert result == 100.0

    def test_v_yoy_monthly_has_pct_change(self, loaded_conn):
        # We have 2022, 2023, 2024 data — YoY should be non-null for 2023 and 2024
        result = loaded_conn.execute("""
            SELECT COUNT(*) FROM v_yoy_monthly
            WHERE yoy_pct_change IS NOT NULL
        """).fetchone()[0]
        assert result > 0

    def test_v_weekday_heatmap_covers_all_days(self, loaded_conn):
        days = loaded_conn.execute(
            "SELECT DISTINCT day_of_week FROM v_weekday_heatmap ORDER BY 1"
        ).fetchall()
        day_values = [d[0] for d in days]
        assert all(d in range(7) for d in day_values)

    def test_v_amba_vs_interior_only_si_no(self, loaded_conn):
        """v_amba_vs_interior should contain only rows where amba is 'SI' or 'NO'."""
        amba_values = {row[0] for row in loaded_conn.execute(
            "SELECT DISTINCT amba FROM v_amba_vs_interior"
        ).fetchall()}
        assert amba_values <= {"SI", "NO"}

    def test_v_amba_vs_interior_share_sums_to_100_per_period_mode(self, loaded_conn):
        """Within each (month_start, modo) group, share_pct values should sum to ~100."""
        result = loaded_conn.execute("""
            SELECT month_start, modo, ROUND(SUM(share_pct), 0) AS total_share
            FROM v_amba_vs_interior
            GROUP BY month_start, modo
        """).fetchall()
        for row in result:
            assert row[2] == 100.0, (
                f"share_pct for {row[0]} / {row[1]} sums to {row[2]}, expected 100"
            )

    def test_v_total_daily_excludes_suspicious(self, tmp_path):
        csv = """FECHA,MODO,CANTIDAD_USOS
2024-01-02,COLECTIVO,0
2024-01-03,COLECTIVO,50000
"""
        path = tmp_path / "test.csv"
        path.write_text(csv)
        df = clean_file(path)
        conn = duckdb.connect(":memory:")
        load(df, conn=conn)
        # Jan 2 is suspicious (weekday zero), should be excluded from v_total_daily
        result = conn.execute(
            "SELECT total_usos FROM v_total_daily WHERE fecha = '2024-01-02'"
        ).fetchone()
        assert result is None  # row excluded entirely


# ── query() ────────────────────────────────────────────────────────────────

class TestQuery:
    def test_returns_dataframe(self, loaded_conn):
        result = query(
            "SELECT COUNT(*) AS n FROM daily_transactions",
            conn=loaded_conn,
        )
        assert isinstance(result, pd.DataFrame)

    def test_query_returns_correct_result(self, loaded_conn):
        result = query(
            "SELECT modo, SUM(cantidad_usos) AS total FROM daily_transactions GROUP BY modo ORDER BY modo",
            conn=loaded_conn,
        )
        assert "modo" in result.columns
        assert "total" in result.columns
        assert len(result) > 0

    def test_invalid_sql_raises(self, loaded_conn):
        with pytest.raises(Exception):
            query("SELECT * FROM nonexistent_table_xyz", conn=loaded_conn)

    def test_query_result_has_correct_columns(self, loaded_conn):
        result = query(
            "SELECT fecha, total_usos FROM v_total_daily LIMIT 3",
            conn=loaded_conn,
        )
        assert list(result.columns) == ["fecha", "total_usos"]

    def test_query_row_count(self, loaded_conn):
        result = query(
            "SELECT * FROM daily_transactions LIMIT 5",
            conn=loaded_conn,
        )
        assert len(result) <= 5