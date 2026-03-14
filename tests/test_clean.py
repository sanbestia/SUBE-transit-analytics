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


# ── Helper ─────────────────────────────────────────────────────────────────
# Plain function (not a fixture) — avoids all pytest fixture discovery issues.
# Tests receive tmp_path (pytest builtin) and call _write_csv directly.

def _write_csv(tmp_path, content, name="test.csv", encoding="utf-8"):
    """Write CSV content to a temp file and return its Path."""
    p = tmp_path / name
    p.write_text(content, encoding=encoding)
    return p


# ── CSV content constants ──────────────────────────────────────────────────

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

    def test_alias_usos_becomes_cantidad_usos(self):
        df = pd.DataFrame(columns=["fecha", "modo", "usos"])
        result = _normalize_columns(df)
        assert "cantidad_usos" in result.columns

    def test_alias_date_becomes_fecha(self):
        df = pd.DataFrame(columns=["date", "modo", "cantidad_usos"])
        result = _normalize_columns(df)
        assert "fecha" in result.columns
        assert "date" not in result.columns

    def test_alias_dia_becomes_fecha(self):
        df = pd.DataFrame(columns=["dia", "modo", "cantidad_usos"])
        result = _normalize_columns(df)
        assert "fecha" in result.columns

    def test_alias_linea_modo_becomes_modo(self):
        df = pd.DataFrame(columns=["fecha", "linea_modo", "cantidad_usos"])
        result = _normalize_columns(df)
        assert "modo" in result.columns
        assert "linea_modo" not in result.columns

    def test_idempotent_on_already_normalized(self):
        df = pd.DataFrame(columns=["fecha", "modo", "cantidad_usos"])
        result = _normalize_columns(df)
        assert list(result.columns) == ["fecha", "modo", "cantidad_usos"]


# ── _try_parse_csv ─────────────────────────────────────────────────────────

class TestTryParseCsv:
    def test_parses_utf8_comma(self, tmp_path):
        path = _write_csv(tmp_path, SIMPLE_SCHEMA_CSV)
        df = _try_parse_csv(path)
        assert df.shape[1] >= 3

    def test_parses_semicolon_separator(self, tmp_path):
        path = _write_csv(tmp_path, SEMICOLON_CSV)
        df = _try_parse_csv(path)
        assert df.shape[1] >= 3

    def test_parses_latin1_encoding(self, tmp_path):
        path = _write_csv(tmp_path, SIMPLE_SCHEMA_CSV, encoding="latin-1")
        df = _try_parse_csv(path)
        assert df.shape[1] >= 3

    def test_raises_on_unparseable_file(self, tmp_path):
        path = tmp_path / "bad.csv"
        path.write_text("notacsv\n" * 5)
        with pytest.raises(ValueError, match="Could not parse"):
            _try_parse_csv(path)

    def test_returns_dataframe(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = _try_parse_csv(path)
        assert isinstance(df, pd.DataFrame)

    def test_real_schema_has_correct_column_count(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = _try_parse_csv(path)
        assert df.shape[1] == 10


# ── clean_file ─────────────────────────────────────────────────────────────

class TestCleanFile:

    # ── Output schema ──────────────────────────────────────────────────────
    def test_required_columns_present(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        for col in ("fecha", "modo", "cantidad_usos", "year", "month",
                    "day_of_week", "is_suspicious", "source_file"):
            assert col in df.columns, f"Missing required column: {col}"

    def test_enrichment_columns_preserved(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        for col in ("nombre_empresa", "linea", "amba", "provincia", "municipio"):
            assert col in df.columns, f"Missing enrichment column: {col}"

    def test_only_known_columns_kept(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        for col in df.columns:
            assert col in COLUMNS_TO_KEEP, f"Unexpected column: {col}"

    def test_source_file_column_contains_filename(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV, name="dat-ab-usos-2024.csv")
        df = clean_file(path)
        assert (df["source_file"] == "dat-ab-usos-2024.csv").all()

    # ── Date parsing ───────────────────────────────────────────────────────
    def test_fecha_is_datetime(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert pd.api.types.is_datetime64_any_dtype(df["fecha"])

    def test_year_and_month_derived_correctly(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert (df["year"] == 2024).all()
        assert (df["month"] == 1).all()

    def test_day_of_week_range(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert df["day_of_week"].between(0, 6).all()

    def test_bad_dates_are_dropped(self, tmp_path):
        path = _write_csv(tmp_path, BAD_DATES_CSV)
        df = clean_file(path)
        assert len(df) == 1
        assert df["modo"].iloc[0] == "TREN"

    def test_all_bad_dates_returns_empty(self, tmp_path):
        all_bad = "FECHA,MODO,CANTIDAD_USOS\nnot-a-date,COLECTIVO,1000\nalso-bad,TREN,2000\n"
        path = _write_csv(tmp_path, all_bad)
        df = clean_file(path)
        assert len(df) == 0

    # ── Mode normalization ─────────────────────────────────────────────────
    def test_mode_is_uppercase(self, tmp_path):
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-01,colectivo,1000\n"
        path = _write_csv(tmp_path, csv)
        df = clean_file(path)
        assert df["modo"].iloc[0] == "COLECTIVO"

    def test_mode_whitespace_stripped(self, tmp_path):
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-01, TREN ,1000\n"
        path = _write_csv(tmp_path, csv)
        df = clean_file(path)
        assert df["modo"].iloc[0] == "TREN"

    def test_unknown_mode_is_kept_as_is(self, tmp_path):
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-01,BICICLETA,1000\n"
        path = _write_csv(tmp_path, csv)
        df = clean_file(path)
        assert df["modo"].iloc[0] == "BICICLETA"

    # ── Quantity parsing ───────────────────────────────────────────────────
    def test_cantidad_usos_is_numeric(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert pd.api.types.is_integer_dtype(df["cantidad_usos"])

    def test_thousand_separators_stripped(self, tmp_path):
        path = _write_csv(tmp_path, THOUSAND_SEP_CSV)
        df = clean_file(path)
        colectivo = df[df["modo"] == "COLECTIVO"]["cantidad_usos"].iloc[0]
        assert colectivo == 3_000_000

    def test_column_aliases_resolved(self, tmp_path):
        path = _write_csv(tmp_path, ALIAS_CSV)
        df = clean_file(path)
        assert "modo" in df.columns
        assert "cantidad_usos" in df.columns

    def test_bad_quantity_rows_dropped(self, tmp_path):
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-01,COLECTIVO,abc\n2024-01-02,TREN,5000\n"
        path = _write_csv(tmp_path, csv)
        df = clean_file(path)
        assert len(df) == 1
        assert df["modo"].iloc[0] == "TREN"

    # ── Outlier flagging ───────────────────────────────────────────────────
    def test_zero_on_weekday_flagged_suspicious(self, tmp_path):
        # 2024-01-02 is a Tuesday
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-02,COLECTIVO,0\n"
        path = _write_csv(tmp_path, csv)
        df = clean_file(path)
        assert bool(df["is_suspicious"].iloc[0]) is True

    def test_zero_on_weekend_not_flagged(self, tmp_path):
        # 2024-01-06 is a Saturday
        csv = "FECHA,MODO,CANTIDAD_USOS\n2024-01-06,COLECTIVO,0\n"
        path = _write_csv(tmp_path, csv)
        df = clean_file(path)
        assert bool(df["is_suspicious"].iloc[0]) is False

    def test_normal_values_not_flagged(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert df["is_suspicious"].sum() == 0

    def test_statistical_outlier_flagged(self, tmp_path):
        rows = ["FECHA,MODO,CANTIDAD_USOS"]
        for i in range(1, 30):
            rows.append(f"2024-01-{i:02d},COLECTIVO,1000")
        rows.append("2024-02-01,COLECTIVO,1000000")  # extreme outlier
        csv = "\n".join(rows) + "\n"
        path = _write_csv(tmp_path, csv)
        df = clean_file(path)
        outlier_row = df[df["cantidad_usos"] == 1_000_000]
        assert not outlier_row.empty
        assert bool(outlier_row["is_suspicious"].iloc[0]) is True

    # ── Deduplication ──────────────────────────────────────────────────────
    def test_duplicates_removed(self, tmp_path):
        path = _write_csv(tmp_path, DUPLICATE_CSV)
        df = clean_file(path)
        assert len(df) == 1

    def test_non_duplicates_both_kept(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        # REAL_SCHEMA_CSV has 4 distinct (fecha, nombre_empresa, linea) combos
        assert len(df) == 4

    # ── Semicolon separator ────────────────────────────────────────────────
    def test_semicolon_separator_parsed(self, tmp_path):
        path = _write_csv(tmp_path, SEMICOLON_CSV)
        df = clean_file(path)
        assert len(df) == 2

    # ── Error handling ─────────────────────────────────────────────────────
    def test_missing_required_columns_raises(self, tmp_path):
        bad = "col1,col2\nval1,val2\n"
        path = _write_csv(tmp_path, bad)
        with pytest.raises(ValueError, match="missing columns"):
            clean_file(path)

    def test_latin1_encoding_handled(self, tmp_path):
        path = _write_csv(tmp_path, SIMPLE_SCHEMA_CSV, encoding="latin-1")
        df = clean_file(path)
        assert len(df) > 0

    # ── Sorting ────────────────────────────────────────────────────────────
    def test_output_sorted_by_fecha(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert df["fecha"].is_monotonic_increasing

    # ── Real schema specifics ──────────────────────────────────────────────
    def test_real_schema_enrichment_cols_normalized(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        for col in ("nombre_empresa", "linea", "provincia", "municipio"):
            assert (df[col] == df[col].str.upper()).all()

    def test_simple_schema_no_enrichment_columns(self, tmp_path):
        path = _write_csv(tmp_path, SIMPLE_SCHEMA_CSV)
        df = clean_file(path)
        for col in ("nombre_empresa", "linea", "provincia"):
            assert col not in df.columns

    def test_dato_preliminar_normalized(self, tmp_path):
        path = _write_csv(tmp_path, REAL_SCHEMA_CSV)
        df = clean_file(path)
        assert "dato_preliminar" in df.columns
        assert (df["dato_preliminar"] == df["dato_preliminar"].str.upper()).all()


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

        row = "2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N"
        header = "dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar"
        (tmp_path / "dat-ab-usos-2024a.csv").write_text(f"{header}\n{row}\n")
        (tmp_path / "dat-ab-usos-2024b.csv").write_text(f"{header}\n{row}\n")

        with patch("etl.clean.DATA_RAW_DIR", tmp_path):
            df = clean_all()

        assert len(df) == 1

    def test_raises_if_all_files_fail(self, tmp_path):
        from unittest.mock import patch

        (tmp_path / "dat-ab-usos-2024.csv").write_text("bad,data\nno,columns\n")

        with patch("etl.clean.DATA_RAW_DIR", tmp_path):
            with pytest.raises(RuntimeError, match="No files were successfully cleaned"):
                clean_all()

    def test_partial_failure_skips_bad_file(self, tmp_path):
        from unittest.mock import patch

        good = """dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar
2024-01-01,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
"""
        (tmp_path / "dat-ab-usos-2024.csv").write_text(good)
        (tmp_path / "dat-ab-usos-2023.csv").write_text("bad,data\nno,columns\n")

        with patch("etl.clean.DATA_RAW_DIR", tmp_path):
            df = clean_all()

        assert len(df) == 1
        assert df["year"].iloc[0] == 2024

    def test_result_is_sorted_by_fecha(self, tmp_path):
        from unittest.mock import patch

        for year in (2023, 2024):
            csv = f"""dia_transporte,nombre_empresa,linea,amba,tipo_transporte,jurisdiccion,provincia,municipio,cantidad_usos,dato_preliminar
{year}-06-15,EMPRESA A,LINEA 1,SI,COLECTIVO,NACIONAL,BUENOS AIRES,CABA,50000,N
"""
            (tmp_path / f"dat-ab-usos-{year}.csv").write_text(csv)

        with patch("etl.clean.DATA_RAW_DIR", tmp_path):
            df = clean_all()

        assert df["fecha"].is_monotonic_increasing