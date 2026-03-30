"""
etl/load.py — Load clean DataFrames into DuckDB and build analytics views.

Schema:
  daily_transactions   -> one row per (fecha, modo, nombre_empresa, linea)
  monthly_transactions -> pre-aggregated monthly rollup by mode
"""

from pathlib import Path

import duckdb
import pandas as pd
from loguru import logger

from config import DATA_PROC_DIR, DB_PATH, TABLE_CLEAN, TABLE_MONTHLY


def get_connection() -> duckdb.DuckDBPyConnection:
    DATA_PROC_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def load(df: pd.DataFrame, conn: duckdb.DuckDBPyConnection | None = None) -> None:
    """
    Write the clean DataFrame into DuckDB and rebuild derived tables.
    Safe to call multiple times — uses CREATE OR REPLACE.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        logger.info("Loading daily_transactions into DuckDB ...")

        # ── Core fact table ───────────────────────────────────────────────
        # Add missing enrichment columns with defaults before loading into DuckDB
        # (simple-schema CSVs won't have these columns)
        enrichment_defaults = {
            'nombre_empresa': 'UNKNOWN', 'linea': 'UNKNOWN', 'amba': 'UNKNOWN',
            'jurisdiccion': 'UNKNOWN', 'provincia': 'UNKNOWN',
            'municipio': 'UNKNOWN', 'dato_preliminar': 'N',
        }
        for col, default in enrichment_defaults.items():
            if col not in df.columns:
                df = df.copy()
                df[col] = default

        conn.execute(f"""
            CREATE OR REPLACE TABLE {TABLE_CLEAN} AS
            SELECT
                fecha::DATE                          AS fecha,
                year::INTEGER                        AS year,
                month::INTEGER                       AS month,
                day_of_week::INTEGER                 AS day_of_week,
                modo::VARCHAR                        AS modo,
                cantidad_usos::BIGINT                AS cantidad_usos,
                is_suspicious::BOOLEAN               AS is_suspicious,
                source_file::VARCHAR                 AS source_file,
                COALESCE(nombre_empresa, 'UNKNOWN')::VARCHAR  AS nombre_empresa,
                COALESCE(linea, 'UNKNOWN')::VARCHAR           AS linea,
                COALESCE(amba, 'UNKNOWN')::VARCHAR            AS amba,
                COALESCE(jurisdiccion, 'UNKNOWN')::VARCHAR    AS jurisdiccion,
                COALESCE(provincia, 'UNKNOWN')::VARCHAR       AS provincia,
                COALESCE(municipio, 'UNKNOWN')::VARCHAR       AS municipio,
                COALESCE(dato_preliminar, 'N')::VARCHAR       AS dato_preliminar
            FROM df
        """)

        n = conn.execute(f"SELECT COUNT(*) FROM {TABLE_CLEAN}").fetchone()[0]
        logger.success(f"  {n:,} rows in {TABLE_CLEAN}")

        # ── Monthly rollup by mode ─────────────────────────────────────────
        logger.info("Building monthly_transactions ...")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {TABLE_MONTHLY} AS
            SELECT
                DATE_TRUNC('month', fecha)::DATE     AS month_start,
                year,
                month,
                modo,
                SUM(cantidad_usos)                   AS total_usos,
                AVG(cantidad_usos)                   AS avg_daily_usos,
                COUNT(DISTINCT fecha)                AS days_with_data,
                SUM(is_suspicious::INT)              AS suspicious_days
            FROM {TABLE_CLEAN}
            WHERE NOT is_suspicious
            GROUP BY 1, 2, 3, 4
            ORDER BY 1, 4
        """)

        n2 = conn.execute(f"SELECT COUNT(*) FROM {TABLE_MONTHLY}").fetchone()[0]
        logger.success(f"  {n2:,} rows in {TABLE_MONTHLY}")

        # ── Monthly rollup by province ─────────────────────────────────────
        logger.info("Building monthly_by_provincia ...")
        conn.execute("""
            CREATE OR REPLACE TABLE monthly_by_provincia AS
            SELECT
                DATE_TRUNC('month', fecha)::DATE     AS month_start,
                year,
                month,
                modo,
                provincia,
                amba,
                SUM(cantidad_usos)                   AS total_usos,
                COUNT(DISTINCT fecha)                AS days_with_data
            FROM daily_transactions
            WHERE NOT is_suspicious
            GROUP BY 1, 2, 3, 4, 5, 6
            ORDER BY 1, 4, 5
        """)
        logger.success("  monthly_by_provincia built")

        # ── Top empresas ──────────────────────────────────────────────────
        logger.info("Building top_empresas ...")
        conn.execute("""
            CREATE OR REPLACE TABLE top_empresas AS
            SELECT
                nombre_empresa,
                modo,
                SUM(cantidad_usos)                   AS total_usos,
                COUNT(DISTINCT fecha)                AS active_days,
                COUNT(DISTINCT linea)                AS num_lineas
            FROM daily_transactions
            WHERE NOT is_suspicious
              AND nombre_empresa != 'UNKNOWN'
            GROUP BY 1, 2
            ORDER BY total_usos DESC
        """)
        logger.success("  top_empresas built")

        # ── Convenience views ─────────────────────────────────────────────
        _create_views(conn)

    finally:
        if own_conn:
            conn.close()


def _create_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Create SQL views for common dashboard queries."""

    # All modes summed into a single daily total — used by the STL decomposition
    # and the daily ridership chart's 7-day moving average.
    conn.execute("""
        CREATE OR REPLACE VIEW v_total_daily AS
        SELECT
            fecha,
            year,
            month,
            day_of_week,
            SUM(cantidad_usos)  AS total_usos
        FROM daily_transactions
        WHERE NOT is_suspicious
        GROUP BY 1, 2, 3, 4
        ORDER BY 1
    """)

    # YoY % change per mode: LAG() looks back exactly 12 months (same month,
    # previous year) within each mode partition — removes seasonal effects so
    # you can compare e.g. Jan 2024 vs Jan 2023 without winter/summer bias.
    conn.execute("""
        CREATE OR REPLACE VIEW v_yoy_monthly AS
        SELECT
            month_start,
            year,
            month,
            modo,
            total_usos,
            LAG(total_usos) OVER (
                PARTITION BY month, modo ORDER BY year
            )                           AS prev_year_usos,
            ROUND(
                100.0 * (total_usos - LAG(total_usos) OVER (
                    PARTITION BY month, modo ORDER BY year
                )) / NULLIF(LAG(total_usos) OVER (
                    PARTITION BY month, modo ORDER BY year
                ), 0),
            2)                          AS yoy_pct_change
        FROM monthly_transactions
        ORDER BY month_start, modo
    """)

    # Each mode's share of total monthly ridership.
    # SUM() OVER (PARTITION BY month_start) gives the all-modes total for
    # that month, so dividing gives the % share of each individual mode.
    conn.execute("""
        CREATE OR REPLACE VIEW v_modal_split AS
        SELECT
            month_start,
            year,
            month,
            modo,
            total_usos,
            ROUND(
                100.0 * total_usos / SUM(total_usos) OVER (PARTITION BY month_start),
            2)                          AS mode_share_pct
        FROM monthly_transactions
        ORDER BY month_start, modo
    """)

    # Average daily trips for every (weekday, calendar month) combination,
    # pooled across all years. Used for the heatmap chart — darker = more trips.
    conn.execute("""
        CREATE OR REPLACE VIEW v_weekday_heatmap AS
        SELECT
            day_of_week,
            month,
            SUM(cantidad_usos)              AS total_usos,
            AVG(cantidad_usos)              AS avg_usos,
            COUNT(DISTINCT fecha)           AS num_days
        FROM daily_transactions
        WHERE NOT is_suspicious
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)

    # AMBA vs Interior monthly totals, with each region's share within its mode.
    # amba='SI' = Buenos Aires metro area; amba='NO' = all other provinces.
    # share_pct = region's trips / total trips for that mode in that month.
    conn.execute("""
        CREATE OR REPLACE VIEW v_amba_vs_interior AS
        SELECT
            month_start,
            year,
            month,
            modo,
            amba,
            total_usos,
            ROUND(
                100.0 * total_usos / SUM(total_usos) OVER (PARTITION BY month_start, modo),
            2)                          AS share_pct
        FROM monthly_by_provincia
        WHERE amba IN ('SI', 'NO')
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY month_start, modo, amba
    """)

    logger.success("  Views created: v_total_daily, v_yoy_monthly, v_modal_split, v_weekday_heatmap, v_amba_vs_interior")


def query(sql: str, conn: duckdb.DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Convenience wrapper: run a SQL query and return a DataFrame."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        return conn.execute(sql).df()
    finally:
        if own_conn:
            conn.close()


if __name__ == "__main__":
    from etl.clean import clean_all
    df = clean_all()
    load(df)
    result = query("SELECT modo, SUM(cantidad_usos) as total FROM daily_transactions GROUP BY 1 ORDER BY 2 DESC")
    print(result)