"""
tests/conftest.py — Shared fixtures for the entire test suite.
"""

import pytest
import pandas as pd
import duckdb
from pathlib import Path


# ── Raw CSV content samples ────────────────────────────────────────────────

# Matches the real schema from datos.transporte.gob.ar
REAL_SCHEMA_CSV = """dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar
2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
2024-01-01,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,20000,N
2024-01-01,EMPRESA C,LINEA 3,NO,COLECTIVO,PROVINCIAL,CORDOBA,CORDOBA,15000,N
2024-01-02,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,48000,N
2024-01-02,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,19000,N
2024-01-03,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,52000,N
2024-01-03,EMPRESA D,LINEA 4,SI,SUBTE,NACIONAL,BUENOS AIRES,CABA,30000,N
"""

# Minimal schema (fecha, modo, cantidad_usos)
SIMPLE_SCHEMA_CSV = """FECHA,MODO,CANTIDAD_USOS
2024-01-01,COLECTIVO,3000000
2024-01-01,TREN,400000
2024-01-01,SUBTE,600000
2024-01-02,COLECTIVO,0
2024-01-03,COLECTIVO,3100000
2024-01-03,TREN,410000
"""


@pytest.fixture
def tmp_csv(tmp_path):
    """Write CSV content to a temp file and return its Path."""
    def _write(content, name="test.csv", encoding="utf-8"):
        p = tmp_path / name
        p.write_text(content, encoding=encoding)
        return p
    return _write


@pytest.fixture
def real_schema_csv(tmp_csv):
    """A CSV file matching the real datos.transporte.gob.ar schema."""
    return tmp_csv(REAL_SCHEMA_CSV, name="dat-ab-usos-2024.csv")


@pytest.fixture
def simple_schema_csv(tmp_csv):
    """A minimal CSV with just fecha, modo, cantidad_usos."""
    return tmp_csv(SIMPLE_SCHEMA_CSV, name="dat-ab-usos-2024.csv")


@pytest.fixture
def clean_real_df(real_schema_csv):
    """A pre-cleaned DataFrame from the real schema CSV."""
    from etl.clean import clean_file
    return clean_file(real_schema_csv)


@pytest.fixture
def in_memory_db(clean_real_df):
    """
    An in-memory DuckDB connection pre-loaded with clean data and all views.
    Used by load and analytics tests — no disk I/O required.
    """
    from etl.load import load
    conn = duckdb.connect(":memory:")
    load(clean_real_df, conn=conn)
    return conn