"""
etl/clean.py — Parse, validate, and normalize raw SUBE CSV files.

Real schema from datos.transporte.gob.ar:
    dia_transporte, nombre_empresa, linea, amba, tipo_transporte,
    jurisdiccion, provincia, municipio, cantidad_usos, dato_preliminar

This module handles:
  - Encoding issues (files are sometimes latin-1)
  - Column name normalization (headers change between years)
  - Type coercion and date parsing
  - Outlier flagging (extreme values, zeros on weekdays)
  - Deduplication
"""

from pathlib import Path

import pandas as pd
from loguru import logger

from config import DATA_RAW_DIR, TRANSPORT_MODES

# ── Column aliases across different file versions ──────────────────────────
COLUMN_ALIASES = {
    # date columns
    "fecha":              "fecha",
    "date":               "fecha",
    "dia":                "fecha",
    "dia_transporte":     "fecha",
    # mode columns
    "modo":               "modo",
    "mode":               "modo",
    "tipo":               "modo",
    "linea_modo":         "modo",
    "tipo_transporte":    "modo",
    # quantity columns
    "cantidad_usos":      "cantidad_usos",
    "usos":               "cantidad_usos",
    "transacciones":      "cantidad_usos",
    "cantidad":           "cantidad_usos",
    "cantidad_transacciones": "cantidad_usos",
}

# All columns we want to keep if present in the raw file
COLUMNS_TO_KEEP = [
    "fecha", "year", "month", "day_of_week",
    "modo", "cantidad_usos", "is_suspicious", "source_file",
    # enrichment columns (present in real data)
    "nombre_empresa", "linea", "amba", "jurisdiccion",
    "provincia", "municipio", "dato_preliminar",
]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and strip column names, then apply aliases."""
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in COLUMN_ALIASES.items() if k in df.columns})
    return df


def _try_parse_csv(path: Path) -> pd.DataFrame:
    """Try UTF-8, then latin-1; handle semicolon vs comma separators."""
    for enc in ("utf-8", "latin-1", "utf-8-sig"):
        for sep in (",", ";"):
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep, low_memory=False)
                if df.shape[1] >= 2:
                    return df
            except Exception:
                continue
    raise ValueError(f"Could not parse {path} with any encoding/separator combination.")


def clean_file(path: Path) -> pd.DataFrame:
    """
    Clean a single year's CSV file.
    Returns a tidy DataFrame retaining all useful columns from the raw data.
    """
    logger.info(f"Cleaning {path.name} ...")

    df = _try_parse_csv(path)
    df = _normalize_columns(df)

    # ── Validate required columns ──────────────────────────────────────────
    required = {"fecha", "modo", "cantidad_usos"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns {missing}. Got: {list(df.columns)}")

    # ── Parse dates ────────────────────────────────────────────────────────
    df["fecha"] = pd.to_datetime(df["fecha"], format="mixed", dayfirst=False, errors="coerce")
    bad_dates   = df["fecha"].isna().sum()
    if bad_dates > 0:
        logger.warning(f"  {bad_dates} rows with unparseable dates — dropping")
    df = df.dropna(subset=["fecha"])

    # ── Normalize transport mode ───────────────────────────────────────────
    df["modo"] = df["modo"].str.strip().str.upper()
    unknown_modes = ~df["modo"].isin(TRANSPORT_MODES.keys())
    if unknown_modes.any():
        logger.warning(
            f"  Unknown modes found: {df.loc[unknown_modes, 'modo'].unique()} — keeping as-is"
        )

    # ── Parse quantity ─────────────────────────────────────────────────────
    df["cantidad_usos"] = (
        df["cantidad_usos"]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
        .replace("", pd.NA)
        .astype("Int64")
    )
    bad_qty = df["cantidad_usos"].isna().sum()
    if bad_qty > 0:
        logger.warning(f"  {bad_qty} rows with unparseable cantidad_usos — dropping")
    df = df.dropna(subset=["cantidad_usos"])

    # ── Normalize enrichment columns ───────────────────────────────────────
    for col in ("nombre_empresa", "linea", "jurisdiccion", "provincia", "municipio"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    if "amba" in df.columns:
        df["amba"] = df["amba"].astype(str).str.strip().str.upper()

    if "dato_preliminar" in df.columns:
        df["dato_preliminar"] = df["dato_preliminar"].astype(str).str.strip().str.upper()

    # ── Outlier flagging (non-destructive) ────────────────────────────────
    df["day_of_week"] = df["fecha"].dt.dayofweek  # 0=Mon, 6=Sun
    df["is_suspicious"] = (df["cantidad_usos"] == 0) & (df["day_of_week"] < 5)

    suspicious = df["is_suspicious"].sum()
    if suspicious > 0:
        logger.warning(f"  {suspicious} suspicious zero-usage weekday rows flagged")

    # Flag statistical outliers per mode (>4 std deviations)
    for mode, group in df.groupby("modo"):
        mean = group["cantidad_usos"].mean()
        std  = group["cantidad_usos"].std()
        if pd.notna(std) and std > 0:
            mask = (df["modo"] == mode) & (
                (df["cantidad_usos"] > mean + 4 * std) |
                (df["cantidad_usos"] < mean - 4 * std)
            )
            df.loc[mask, "is_suspicious"] = True

    # ── Deduplication ──────────────────────────────────────────────────────
    # With granular data, the natural key is (fecha, nombre_empresa, linea)
    dedup_cols = ["fecha", "modo", "nombre_empresa", "linea"] if "nombre_empresa" in df.columns \
        else ["fecha", "modo"]
    before = len(df)
    df = df.drop_duplicates(subset=dedup_cols)
    after  = len(df)
    if before != after:
        logger.warning(f"  Dropped {before - after} duplicate rows")

    # ── Add metadata columns ───────────────────────────────────────────────
    df["year"]        = df["fecha"].dt.year
    df["month"]       = df["fecha"].dt.month
    df["source_file"] = path.name

    # ── Final column selection & sort ─────────────────────────────────────
    keep = [c for c in COLUMNS_TO_KEEP if c in df.columns]
    df   = df[keep]
    df   = df.sort_values(["fecha", "modo"]).reset_index(drop=True)

    logger.success(f"  {len(df):,} clean rows from {path.name}")
    return df


def clean_all() -> pd.DataFrame:
    """Clean all downloaded raw CSVs and return a single concatenated DataFrame."""
    raw_files = sorted(DATA_RAW_DIR.glob("dat-ab-usos-*.csv"))
    if not raw_files:
        raise FileNotFoundError(
            f"No raw CSV files found in {DATA_RAW_DIR}. Run etl/ingest.py first."
        )

    dfs = []
    for f in raw_files:
        try:
            dfs.append(clean_file(f))
        except Exception as e:
            logger.error(f"Failed to clean {f.name}: {e}")

    if not dfs:
        raise RuntimeError("No files were successfully cleaned.")

    combined = pd.concat(dfs, ignore_index=True)

    # Final dedup across years in case of overlapping data
    dedup_cols = ["fecha", "modo", "nombre_empresa", "linea"] if "nombre_empresa" in combined.columns \
        else ["fecha", "modo"]
    combined = combined.drop_duplicates(subset=dedup_cols).reset_index(drop=True)

    logger.success(f"Total clean rows: {len(combined):,} across {len(dfs)} files")
    logger.info(f"Date range: {combined['fecha'].min().date()} -> {combined['fecha'].max().date()}")
    logger.info(f"Modes: {sorted(combined['modo'].unique())}")
    logger.info(f"Columns: {list(combined.columns)}")
    return combined


if __name__ == "__main__":
    df = clean_all()
    print(df.dtypes)
    print(df.head(10))