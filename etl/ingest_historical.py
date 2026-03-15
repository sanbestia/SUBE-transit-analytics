"""
etl/ingest_historical.py — Download and normalise pre-2020 SUBE historical data.

Two source datasets from datos.transporte.gob.ar, together covering 2013-01 → 2019-12:

  SOURCE A — "Operaciones por Periodo-Modo" (transporte.gob.ar)
    Coverage : 2013-01 → 2019-06
    Schema   : periodo (MM/YYYY or YYYY-MM), modo, tipo_operacion, cantidad
    Filter   : tipo_operacion == 'uso' (excludes check-in, venta de pasaje, etc.)
    URL      : HISTORICAL_URL_PERIODO_MODO

  SOURCE B — "Operaciones por mes en RMBA — detalle por modo" (transporte.gob.ar)
    Coverage : 2016-01 → 2019-12  (confirmed from live sample)
    Schema   : anio (MM/YYYY), MODO, TOTAL  (semicolon-delimited, BOM-encoded)
    Note     : Already filtered to 'uso' — all rows are trip counts.
    URL      : HISTORICAL_URL_MMODO

Strategy:
  - Source B is cleaner and its schema is confirmed.  Use it as the primary source
    for 2016-07 → 2019-12 (the overlap and the gap months).
  - Use Source A for 2013-01 → 2016-06 (the period not covered by Source B).
  - Where both sources cover the same month, prefer Source B.
  - Merge into a single DataFrame with columns:
      month_start (date), modo (str), total_usos (int), source (str)
  - PREMETRO is mapped to SUBTE (it is the Buenos Aires pre-metro tram, operated
    by the subway company as part of the underground network).

Usage:
    from etl.ingest_historical import download_historical, load_historical
    df = download_historical()          # fetches and normalises both CSVs
    load_historical(df, conn)           # writes monthly_historical to DuckDB
"""

import io
import hashlib
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW_DIR

# ── Source URLs ────────────────────────────────────────────────────────────

HISTORICAL_URL_PERIODO_MODO = (
    "https://datos.transporte.gob.ar/dataset/"
    "6cdf3d04-a5cb-45ca-9b64-dd9c520eb4f8/resource/"
    "29ec1015-0c53-48e4-ae0b-99ff514741bf/download/"
    "operaciones-de-viaje-por-periodo-modo.csv"
)

HISTORICAL_URL_MMODO = (
    "https://datos.transporte.gob.ar/dataset/"
    "880e663e-eb1d-47ec-aab9-23f3df6c49e8/resource/"
    "158e2d42-88bd-4d84-ab2d-7f9da94f96b9/download/"
    "cancelaciones_mes_mmodo.csv"
)

# Cache file names inside data/raw/
_CACHE_PERIODO_MODO = "historical_periodo_modo.csv"
_CACHE_MMODO        = "historical_mmodo.csv"

# Modes to keep (after PREMETRO → SUBTE mapping)
VALID_MODES = {"COLECTIVO", "TREN", "SUBTE"}

# PREMETRO is the Buenos Aires pre-metro tram — part of the SUBTE network,
# operated by EMOVA under the subway concession.  Map it to SUBTE so the
# historical series is consistent with post-2020 data.
MODE_MAP = {
    "COLECTIVO": "COLECTIVO",
    "TREN":      "TREN",
    "SUBTE":     "SUBTE",
    "PREMETRO":  "SUBTE",
}


# ── Download helpers ───────────────────────────────────────────────────────

def _download_csv(url: str, cache_name: str, force: bool = False) -> bytes:
    """
    Download a CSV, caching it locally under data/raw/.
    Uses a hash sidecar (.sha256) to skip re-download if content hasn't changed.
    Set force=True to bypass the cache.
    """
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = DATA_RAW_DIR / cache_name
    hash_path  = cache_path.with_suffix(".sha256")

    if not force and cache_path.exists():
        logger.info(f"  Using cached {cache_name}")
        return cache_path.read_bytes()

    logger.info(f"  Downloading {url} ...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    content = resp.content

    # Write cache + hash
    cache_path.write_bytes(content)
    hash_path.write_text(hashlib.sha256(content).hexdigest())
    logger.success(f"  Saved {cache_name} ({len(content):,} bytes)")
    return content


# ── Source B parser: cancelaciones_mes_mmodo.csv ──────────────────────────
# Schema: anio;MODO;TOTAL  (semicolon, BOM UTF-8, date as MM/YYYY)

def _parse_mmodo(content: bytes) -> pd.DataFrame:
    """
    Parse Source B (cancelaciones_mes_mmodo.csv) into a normalised DataFrame.

    Returns DataFrame with columns:
        month_start (pd.Timestamp), modo (str), total_usos (int), source (str)
    """
    text = content.decode("utf-8-sig").strip()
    df   = pd.read_csv(io.StringIO(text), sep=";", dtype=str)

    # Normalise column names robustly (source uses inconsistent capitalisation)
    df.columns = [c.strip().lower() for c in df.columns]
    # Expected: anio, modo, total
    if "anio" not in df.columns:
        raise ValueError(f"Source B: expected 'anio' column, got {list(df.columns)}")

    # Parse period: MM/YYYY → first day of month
    df["month_start"] = pd.to_datetime(df["anio"].str.strip(), format="%m/%Y")

    # Normalise mode
    df["modo"] = df["modo"].str.strip().str.upper().map(MODE_MAP)
    df = df[df["modo"].isin(VALID_MODES)].copy()

    # Parse total
    df["total_usos"] = (
        df["total"].str.strip().str.replace(",", "").astype("Int64")
    )
    df = df.dropna(subset=["total_usos"])

    df["source"] = "mmodo_2016_2019"
    return df[["month_start", "modo", "total_usos", "source"]].copy()


# ── Source A parser: operaciones-de-viaje-por-periodo-modo.csv ────────────
# Schema varies — confirmed columns from metadata: periodo, modo,
# tipo_operacion, cantidad.  Separator may be comma or semicolon.

def _parse_periodo_modo(content: bytes) -> pd.DataFrame:
    """
    Parse Source A (operaciones-de-viaje-por-periodo-modo.csv).

    The file includes multiple tipo_operacion values. We keep only 'uso'
    (and 'uso con integración' / 'uso sin integración' variants) to match
    the post-2020 pipeline which counts ridership trips only.

    Returns DataFrame with columns:
        month_start (pd.Timestamp), modo (str), total_usos (int), source (str)
    """
    # Detect separator
    sample = content[:2048].decode("utf-8-sig", errors="replace")
    sep    = ";" if sample.count(";") > sample.count(",") else ","

    text = content.decode("utf-8-sig").strip()
    df   = pd.read_csv(io.StringIO(text), sep=sep, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    logger.debug(f"  Source A columns: {list(df.columns)}")

    # Flexible column name matching
    col_map = {}
    for col in df.columns:
        if "periodo" in col or col in ("anio", "fecha"):
            col_map["periodo"] = col
        elif col == "modo":
            col_map["modo"] = col
        elif "tipo" in col and "operacion" in col:
            col_map["tipo_operacion"] = col
        elif ("cantidad" in col or col in ("total", "total_usos")):
            col_map["cantidad"] = col

    missing = [k for k in ("periodo", "modo", "cantidad") if k not in col_map]
    if missing:
        raise ValueError(f"Source A: missing columns {missing}. Found: {list(df.columns)}")

    df = df.rename(columns={v: k for k, v in col_map.items()})

    # Filter to uso only if tipo_operacion column is present
    if "tipo_operacion" in df.columns:
        uso_mask = (
            df["tipo_operacion"].str.strip().str.lower()
            .str.startswith("uso")
        )
        df = df[uso_mask].copy()
        logger.debug(f"  Source A: {len(df):,} 'uso' rows after filter")
    else:
        logger.warning("  Source A: no tipo_operacion column — using all rows")

    # Parse period — format is YYYYMM (integer, e.g. 201301 = Jan 2013)
    # Fallback formats for robustness
    def _parse_period(val: str) -> pd.Timestamp | None:
        val = str(val).strip()
        # Primary: YYYYMM integer
        if val.isdigit() and len(val) == 6:
            try:
                return pd.to_datetime(val, format="%Y%m")
            except ValueError:
                pass
        # Fallbacks
        for fmt in ("%Y-%m", "%m/%Y", "%m/%y", "%Y/%m", "%b-%Y", "%B-%Y"):
            try:
                return pd.to_datetime(val, format=fmt)
            except ValueError:
                continue
        try:
            return pd.to_datetime(val, dayfirst=False)
        except Exception:
            return pd.NaT

    # Log a sample of raw periodo values to diagnose format
    sample_periods = df["periodo"].dropna().unique()[:5].tolist()
    logger.debug(f"  Source A sample periodo values: {sample_periods}")

    df["month_start"] = df["periodo"].apply(_parse_period)
    nat_count = df["month_start"].isna().sum()
    if nat_count > 0:
        bad = df[df["month_start"].isna()]["periodo"].unique()[:5].tolist()
        logger.warning(f"  Source A: {nat_count} rows with unparseable periodo: {bad}")
    df = df.dropna(subset=["month_start"])

    # Normalise mode
    df["modo"] = df["modo"].str.strip().str.upper().map(MODE_MAP)
    df = df[df["modo"].isin(VALID_MODES)].copy()

    # Parse cantidad — strip Argentine thousands separator (.) before converting
    df["cantidad"] = (
        df["cantidad"].str.strip().str.replace(".", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )
    df = df.dropna(subset=["cantidad"])

    result = (
        df.groupby(["month_start", "modo"])["cantidad"]
        .sum()
        .reset_index()
        .rename(columns={"cantidad": "total_usos"})
    )
    result["total_usos"] = result["total_usos"].astype("Int64")
    result["source"]     = "periodo_modo_2013_2019"
    return result[["month_start", "modo", "total_usos", "source"]].copy()


# ── Main public function ───────────────────────────────────────────────────

def download_historical(force: bool = False) -> pd.DataFrame:
    """
    Download, parse, and merge both historical sources into a single DataFrame.

    Merge strategy:
      - Source B (mmodo) is preferred — it's confirmed clean.
      - Source A (periodo_modo) fills months not covered by Source B
        (i.e. 2013-01 → 2016-06, or earlier than whatever Source B starts).
      - Where both cover the same month, Source B wins.

    Returns DataFrame with columns:
        month_start (pd.Timestamp)  — first day of month
        modo        (str)           — COLECTIVO | TREN | SUBTE
        total_usos  (int)           — ridership trips for that month+mode
        source      (str)           — provenance tag

    The result covers AMBA only (both sources are RMBA datasets).
    No Interior data exists before 2020.
    """
    logger.info("=== Ingesting historical SUBE data ===")

    # Source B — primary, confirmed schema
    logger.info("Source B: cancelaciones_mes_mmodo.csv")
    content_b = _download_csv(HISTORICAL_URL_MMODO, _CACHE_MMODO, force=force)
    df_b      = _parse_mmodo(content_b)
    logger.success(f"  Source B: {len(df_b):,} rows, "
                   f"{df_b['month_start'].min().strftime('%Y-%m')} → "
                   f"{df_b['month_start'].max().strftime('%Y-%m')}")

    # Source A — fills 2013-2015 gap
    logger.info("Source A: operaciones-de-viaje-por-periodo-modo.csv")
    content_a = _download_csv(HISTORICAL_URL_PERIODO_MODO, _CACHE_PERIODO_MODO, force=force)
    df_a      = _parse_periodo_modo(content_a)
    if df_a.empty:
        logger.warning("  Source A: no rows parsed — check periodo format in debug log above")
    else:
        logger.success(f"  Source A: {len(df_a):,} rows, "
                       f"{df_a['month_start'].min().strftime('%Y-%m')} → "
                       f"{df_a['month_start'].max().strftime('%Y-%m')}")

    # Merge: Source B wins on overlap.
    # Normalize month_start to string (YYYY-MM) to avoid Timestamp comparison
    # edge cases between the two parsers.
    b_keys = set(
        zip(df_b["month_start"].dt.strftime("%Y-%m"), df_b["modo"])
    )
    df_a["_key"] = df_a["month_start"].dt.strftime("%Y-%m")
    df_a_fill = df_a[
        ~df_a.apply(lambda r: (r["_key"], r["modo"]) in b_keys, axis=1)
    ].drop(columns=["_key"]).copy()

    logger.debug(
        f"  Merge: Source A total={len(df_a)}, "
        f"excluded by B overlap={len(df_a) - len(df_a_fill)}, "
        f"kept={len(df_a_fill)}"
    )
    logger.debug(
        f"  Source A date range: "
        f"{df_a['month_start'].min().strftime('%Y-%m')} → "
        f"{df_a['month_start'].max().strftime('%Y-%m')}"
    )
    logger.debug(
        f"  Source B date range: "
        f"{df_b['month_start'].min().strftime('%Y-%m')} → "
        f"{df_b['month_start'].max().strftime('%Y-%m')}"
    )

    df = pd.concat([df_b, df_a_fill], ignore_index=True)
    df = df.sort_values(["month_start", "modo"]).reset_index(drop=True)

    logger.success(
        f"Combined: {len(df):,} rows, "
        f"{df['month_start'].min().strftime('%Y-%m')} → "
        f"{df['month_start'].max().strftime('%Y-%m')} | "
        f"modes: {sorted(df['modo'].unique())}"
    )
    return df


# ── DuckDB loader ─────────────────────────────────────────────────────────

def load_historical(
    df: pd.DataFrame,
    conn,
    table_name: str = "monthly_historical",
) -> None:
    """
    Write the historical DataFrame into a DuckDB table.

    The table schema mirrors monthly_transactions but is AMBA-only and
    monthly-only. It is kept separate to avoid breaking the existing ETL.

    Data is clipped by mode:
    - COLECTIVO: from 2013-01 (fully SUBE-integrated from the start, ~2009)
    - SUBTE/TREN: from 2016-01 (SUBE integration was incomplete before 2016 —
      pre-2016 values represent SUBE-registered trips only, not total ridership,
      making them non-comparable to post-2020 data)

    Columns created:
        month_start  DATE
        year         INTEGER
        month        INTEGER
        modo         VARCHAR
        total_usos   BIGINT
        amba         VARCHAR  ('SI' — all rows are AMBA)
        era          VARCHAR  ('pre2020')
        source       VARCHAR

    Safe to call multiple times — uses CREATE OR REPLACE.
    """
    logger.info(f"Loading {len(df):,} rows into {table_name} ...")

    # Mode-specific clip:
    # - COLECTIVO: keep from 2013-01 (fully SUBE-integrated from the start)
    # - SUBTE/TREN: clip to 2016-01 (SUBE integration was incomplete before 2016 —
    #   pre-2016 values are SUBE-registered trips only, not total ridership)
    mask = (
        (df["modo"] == "COLECTIVO") |
        (df["month_start"] >= "2016-01-01")
    )
    df = df[mask].copy()
    logger.info(f"  After mode-specific clip: {len(df):,} rows "
                f"(COLECTIVO from 2013, SUBTE/TREN from 2016)")

    df = df.copy()
    df["year"]  = df["month_start"].dt.year
    df["month"] = df["month_start"].dt.month
    df["amba"]  = "SI"
    df["era"]   = "pre2020"

    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            month_start::DATE   AS month_start,
            year::INTEGER       AS year,
            month::INTEGER      AS month,
            modo::VARCHAR       AS modo,
            total_usos::BIGINT  AS total_usos,
            amba::VARCHAR       AS amba,
            era::VARCHAR        AS era,
            source::VARCHAR     AS source
        FROM df
        ORDER BY month_start, modo
    """)

    n = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.success(f"  {n:,} rows in {table_name}")

    # Sanity check: no gaps longer than 2 months within a mode
    gaps = conn.execute(f"""
        WITH lagged AS (
            SELECT modo,
                   month_start,
                   LAG(month_start) OVER (PARTITION BY modo ORDER BY month_start) AS prev,
                   DATEDIFF('month',
                       LAG(month_start) OVER (PARTITION BY modo ORDER BY month_start),
                       month_start
                   ) AS gap_months
            FROM {table_name}
        )
        SELECT modo, month_start, prev, gap_months
        FROM lagged
        WHERE gap_months > 2
        ORDER BY modo, month_start
    """).df()

    if not gaps.empty:
        logger.warning(f"  Gaps > 2 months detected:\n{gaps.to_string()}")
    else:
        logger.success("  No gaps > 2 months — series is continuous.")


# ── CLI entry point ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import duckdb
    from config import DB_PATH, DATA_PROC_DIR

    parser = argparse.ArgumentParser(
        description="Ingest pre-2020 SUBE historical data into DuckDB."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-download even if cached files exist."
    )
    parser.add_argument(
        "--db", default=str(DB_PATH),
        help=f"DuckDB path (default: {DB_PATH})"
    )
    args = parser.parse_args()

    df = download_historical(force=args.force)

    DATA_PROC_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.db)
    load_historical(df, conn)
    conn.close()

    print(f"\nDone. monthly_historical written to {args.db}")
    print(f"Preview:\n{df.head(10).to_string(index=False)}")