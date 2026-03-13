"""
config.py — Central configuration for the SUBE Analytics project.

Runtime settings (paths, URLs, constants) live here as Python values.
Domain reference data (events, fare hikes) live in data/reference/*.yaml
and are loaded once at import time — the rest of the codebase still does:

    from config import EVENTS, FARE_HIKES
"""
from pathlib import Path
import yaml

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT_DIR      = Path(__file__).parent
DATA_RAW_DIR  = ROOT_DIR / "data" / "raw"
DATA_PROC_DIR = ROOT_DIR / "data" / "processed"
DATA_REF_DIR  = ROOT_DIR / "data" / "reference"
DB_PATH       = DATA_PROC_DIR / "sube.duckdb"

# ── Source URLs ────────────────────────────────────────────────────────────
BASE_URL   = "https://archivos-datos.transporte.gob.ar/upload/Dat_Ab_Usos"
FIRST_YEAR = 2020

# ── Transport modes ────────────────────────────────────────────────────────
TRANSPORT_MODES = {
    "COLECTIVO": "Colectivo (Bus)",
    "TREN":      "Tren",
    "SUBTE":     "Subte",
    "LANCHAS":   "Lanchas (Ferry)",
}

# Modes shown in the dashboard (LANCHAS excluded — statistically negligible)
DASHBOARD_MODES = ["COLECTIVO", "TREN", "SUBTE"]

MODE_COLORS = {
    "COLECTIVO": "#2563EB",
    "TREN":      "#16A34A",
    "SUBTE":     "#DC2626",
}

# ── DuckDB table names ─────────────────────────────────────────────────────
TABLE_RAW     = "raw_transactions"
TABLE_CLEAN   = "daily_transactions"
TABLE_MONTHLY = "monthly_transactions"

# ── Reference data (loaded from YAML) ─────────────────────────────────────
def _load_yaml(filename: str) -> list[dict]:
    path = DATA_REF_DIR / filename
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)

EVENTS     = _load_yaml("events.yaml")
FARE_HIKES = _load_yaml("fare_hikes.yaml")