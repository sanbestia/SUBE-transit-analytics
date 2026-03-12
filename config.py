"""
config.py — Central configuration for the SUBE Analytics project.
All hardcoded values live here so nothing is scattered across files.
"""
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT_DIR      = Path(__file__).parent
DATA_RAW_DIR  = ROOT_DIR / "data" / "raw"
DATA_PROC_DIR = ROOT_DIR / "data" / "processed"
DB_PATH       = DATA_PROC_DIR / "sube.duckdb"

# ── Source URLs ────────────────────────────────────────────────────────────
# The transporte.gob.ar portal follows a predictable pattern:
#   dat-ab-usos-{YEAR}.csv
# We derive URLs dynamically so the pipeline picks up new years automatically.
BASE_URL       = "https://archivos-datos.transporte.gob.ar/upload/Dat_Ab_Usos"
FIRST_YEAR     = 2020          # earliest year with reliable daily data
# Current year is resolved at runtime in the ETL so no manual update needed.

# ── Transport modes (as they appear in the raw data) ──────────────────────
TRANSPORT_MODES = {
    "COLECTIVO":  "Colectivo (Bus)",
    "TREN":       "Tren",
    "SUBTE":      "Subte",
    "PREMETRO":   "Premetro",
}

# ── Known anomaly events for annotation in charts ─────────────────────────
EVENTS = [
    {"date": "2020-03-20", "label": "ASPO (lockdown)",        "color": "red"},
    {"date": "2020-11-01", "label": "DISPO begins",           "color": "orange"},
    {"date": "2021-04-09", "label": "ASPO 2 (2nd lockdown)",  "color": "red"},
    {"date": "2022-01-01", "label": "Post-pandemic recovery",  "color": "green"},
    {"date": "2024-02-01", "label": "Tarifa aumentos",        "color": "purple"},
]

# ── DuckDB table names ─────────────────────────────────────────────────────
TABLE_RAW    = "raw_transactions"
TABLE_CLEAN  = "daily_transactions"
TABLE_MONTHLY = "monthly_transactions"
