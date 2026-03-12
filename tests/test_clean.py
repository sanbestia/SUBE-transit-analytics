"""
tests/test_clean.py — Tests for etl/clean.py

Covers both the simple schema (fecha, modo, cantidad_usos) and
the real schema from datos.transporte.gob.ar.
"""

import pandas as pd
import pytest

from etl.clean import (
    COLUMNS_TO_KEEP,
    _normalize_columns,
    _try_parse_csv,
    clean_all,
    clean_file,
)


# ── CSV fixtures ───────────────────────────────────────────────────────────

REAL_SCHEMA_CSV = """dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar
2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
2024-01-01,EMPRESA B,LINEA 2,SI,TREN,NACIONAL,BUENOS AIRES,CABA,20000,N
2024-01-02,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,48000,N
2024-01-03,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,52000,N
"""

SIMPLE_SCHEMA_CSV = """FECHA,MODO,CANTIDAD_USOS
2024-01-01,COLECTIVO,3000000
2024-01-01,TREN,400000
2024-01-02,COLECTIVO,0
2024-01-03,COLECTIVO,3100000
"""

SEMICOLON_CSV = """FECHA;MODO;CANTIDAD_USOS
2024-01-01;COLECTIVO;3000000
2024-01-01;TREN;400000
"""

THOUSAND_SEP_CSV = """FECHA,MODO,CANTIDAD_USOS
2024-01-01,COLECTIVO,"3.000.000"
2024-01-01,TREN,"400.000"
"""

ALIAS_CSV = """fecha,tipo,transacciones
2024-01-01,COLECTIVO,3000000
2024-01-01,TREN,400000
"""

BAD_DATES_CSV = """FECHA,MODO,CANTIDAD_USOS
not-a-date,COLECTIVO,3000000
2024-01-01,TREN,400000
"""

DUPLICATE_CSV = """dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar
2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
"""


# ── _normalize_columns ─────────────────────────────────────────────────────

class TestNormalizeColumns:
    def test_lowercases_columns(self):
        df = pd.DataFrame(columns=["FECHA", "MODO", "CANTIDAD_USOS"])
        result = _normalize_columns(df)
        assert list(result.columns) == ["fecha", "modo", "cantidad_usos"]

    def test_strips_whitespace(self):
        df = pd.DataFrame(columns=["  fecha  ", " modo ", " cantidad_usos "])
        result = _normalize_columns(df)
        assert list(result.columns) == ["fecha", "modo", "cantidad_usos"]

    def test_dia_transporte_becomes_fecha(self):
        df = pd.DataFrame(columns=["dia_transporte", "tipo_transporte", "cantidad_usos"])
        result = _normalize_columns(df)
        assert "fecha" in result.columns
        assert "dia_transporte" not in result.columns

    def test_tipo_transporte_becomes_modo(self):
        df = pd.DataFrame(columns=["dia_transporte", "tipo_transporte", "cantidad_usos"])
        result = _normalize_columns(df)
        assert "modo" in result.columns
        assert "tipo_transporte" not in result.columns

    def test_unknown_columns_are_preserved(self):
        df = pd.DataFrame(columns=["fecha", "modo", "cantidad_usos", "nombre_empresa"])
        result = _normalize_columns(df)
        assert "nombre_empresa" in result.columns

    def test_alias_transacciones_becomes_cantidad_usos(self):
        df = pd.DataFrame(columns=["fecha", "modo", "transacciones"])
        result = _normalize_columns(df)
        assert "cantidad_usos" in result.columns


# ── _try_parse_csv ─────────────────────────────────────────────────────────

class TestTryParseCsv:
    def test_parses_utf8_comma(self, tmp_csv):
        path = tmp_csv(SIMPLE_SCHEMA_CSV)
        df = _try_parse_csv(path)
        assert df.shape[1] >= 3

    def test_parses_semicolon_separator(self, tmp_csv):
        path = tmp_csv(SEMICOLON_CSV)
        df = _try_parse_csv(path)
        assert df.shape[1] >= 3

    def test_parses_latin1_encoding(self, tmp_csv):
        path = tmp_csv(SIMPLE_SCHEMA_CSV, encoding="latin-1")
        df = _try_parse_csv(path)
        assert df.shape[1] >= 3

    def test_raises_on_unparseable_file(self, tmp_path):
        path = tmp_path / "bad.csv"
        path.write_bytes(b"\x00\x01\x02\x03")  # binary garbage
        with pytest.raises(ValueError, match="Could not parse"):
            _try_parse_csv(path)


# ── clean_file ─────────────────────────────────────────────────────────────

class TestCleanFile:

    # ── Output schema ──────────────────────────────────────────────────────
    def test_required_columns_present(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        for col in ("fecha", "modo", "cantidad_usos", "year", "month",
                    "day_of_week", "is_suspicious", "source_file"):
            assert col in df.columns, f"Missing required column: {col}"

    def test_enrichment_columns_preserved(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        for col in ("nombre_empresa", "linea", "amba", "provincia", "municipio"):
            assert col in df.columns, f"Missing enrichment column: {col}"

    def test_only_known_columns_kept(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        for col in df.columns:
            assert col in COLUMNS_TO_KEEP, f"Unexpected column: {col}"

    def test_source_file_column_contains_filename(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV, name="dat-ab-usos-2024.csv")
        df = clean_file(path)
        assert (df["source_file"] == "dat-ab-usos-2024.csv").all()

    # ── Date parsing ───────────────────────────────────────────────────────
    def test_fecha_is_datetime(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert pd.api.types.is_datetime64_any_dtype(df["fecha"])

    def test_year_and_month_derived_correctly(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert (df["year"] == 2024).all()
        assert (df["month"] == 1).all()

    def test_bad_dates_are_dropped(self, tmp_csv):
        path = tmp_csv(BAD_DATES_CSV)
        df = clean_file(path)
        assert len(df) == 1  # only the valid row survives
        assert df["modo"].iloc[0] == "TREN"

    # ── Mode normalization ─────────────────────────────────────────────────
    def test_mode_is_uppercase(self, tmp_csv):
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-01,colectivo,1000\n"
        path = tmp_csv(csv)
        df = clean_file(path)
        assert df["modo"].iloc[0] == "COLECTIVO"

    def test_mode_whitespace_stripped(self, tmp_csv):
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-01, TREN ,1000\n"
        path = tmp_csv(csv)
        df = clean_file(path)
        assert df["modo"].iloc[0] == "TREN"

    # ── Quantity parsing ───────────────────────────────────────────────────
    def test_cantidad_usos_is_numeric(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert pd.api.types.is_integer_dtype(df["cantidad_usos"])

    def test_thousand_separators_stripped(self, tmp_csv):
        path = tmp_csv(THOUSAND_SEP_CSV)
        df = clean_file(path)
        colectivo = df[df["modo"] == "COLECTIVO"]["cantidad_usos"].iloc[0]
        assert colectivo == 3_000_000

    def test_column_aliases_resolved(self, tmp_csv):
        path = tmp_csv(ALIAS_CSV)
        df = clean_file(path)
        assert "modo" in df.columns
        assert "cantidad_usos" in df.columns

    # ── Outlier flagging ───────────────────────────────────────────────────
    def test_zero_on_weekday_flagged_suspicious(self, tmp_csv):
        # 2024-01-02 is a Tuesday
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-02,COLECTIVO,0\n"
        path = tmp_csv(csv)
        df = clean_file(path)
        assert bool(df["is_suspicious"].iloc[0]) is True

    def test_zero_on_weekend_not_flagged(self, tmp_csv):
        # 2024-01-06 is a Saturday
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-06,COLECTIVO,0\n"
        path = tmp_csv(csv)
        df = clean_file(path)
        assert bool(df["is_suspicious"].iloc[0]) is False

    def test_normal_values_not_flagged(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert df["is_suspicious"].sum() == 0

    # ── Deduplication ──────────────────────────────────────────────────────
    def test_duplicates_removed(self, tmp_csv):
        path = tmp_csv(DUPLICATE_CSV)
        df = clean_file(path)
        assert len(df) == 1

    # ── Semicolon separator ────────────────────────────────────────────────
    def test_semicolon_separator_parsed(self, tmp_csv):
        path = tmp_csv(SEMICOLON_CSV)
        df = clean_file(path)
        assert len(df) == 2

    # ── Error handling ─────────────────────────────────────────────────────
    def test_missing_required_columns_raises(self, tmp_csv):
        bad = "col1,col2\nval1,val2\n"
        path = tmp_csv(bad)
        with pytest.raises(ValueError, match="missing columns"):
            clean_file(path)

    def test_latin1_encoding_handled(self, tmp_csv):
        path = tmp_csv(SIMPLE_SCHEMA_CSV, encoding="latin-1")
        df = clean_file(path)
        assert len(df) > 0

    # ── Sorting ────────────────────────────────────────────────────────────
    def test_output_sorted_by_fecha(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert df["fecha"].is_monotonic_increasing

    # ── Real schema specifics ──────────────────────────────────────────────
    def test_real_schema_enrichment_cols_normalized(self, tmp_csv):
        path = tmp_csv(REAL_SCHEMA_CSV)
        df = clean_file(path)
        # All string enrichment columns should be uppercase
        for col in ("nombre_empresa", "linea", "provincia", "municipio"):
            assert (df[col] == df[col].str.upper()).all()


# ── clean_all ──────────────────────────────────────────────────────────────

class TestCleanAll:
    def test_raises_if_no_files(self, tmp_path):
        from unittest.mock import patch
        with patch("etl.clean.DATA_RAW_DIR", tmp_path):
            with pytest.raises(FileNotFoundError):
                clean_all()

    def test_combines_multiple_files(self, tmp_path):
        from unittest.mock import patch

        for year in (2023, 2024):
            csv = f"""dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar
{year}-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
"""
            (tmp_path / f"dat-ab-usos-{year}.csv").write_text(csv)

        with patch("etl.clean.DATA_RAW_DIR", tmp_path):
            df = clean_all()

        assert df["year"].nunique() == 2
        assert set(df["year"].unique()) == {2023, 2024}

    def test_deduplicates_across_files(self, tmp_path):
        from unittest.mock import patch

        # Same row in both files
        row = "2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N"
        header = "dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar"
        for year in (2024, 2024):
            (tmp_path / f"dat-ab-usos-{year}b.csv").write_text(f"{header}\n{row}\n")

        with patch("etl.clean.DATA_RAW_DIR", tmp_path):
            df = clean_all()

        # Should only have 1 row despite appearing in both files
        assert len(df) == 1

    def test_raises_if_all_files_fail(self, tmp_path):
        from unittest.mock import patch

        (tmp_path / "dat-ab-usos-2024.csv").write_text("bad,data\nno,columns\n")

        with patch("etl.clean.DATA_RAW_DIR", tmp_path):
            with pytest.raises(RuntimeError, match="No files were successfully cleaned"):
                clean_all()