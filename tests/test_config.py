"""
tests/test_config.py — Tests for config.py

Validates that all configuration values are sane and self-consistent.
"""

import datetime
from pathlib import Path

import pytest

from config import (
    BASE_URL,
    DASHBOARD_MODES,
    DATA_PROC_DIR,
    DATA_RAW_DIR,
    DB_PATH,
    EVENTS,
    FARE_HIKES,
    FIRST_YEAR,
    MODE_COLORS,
    ROOT_DIR,
    TABLE_CLEAN,
    TABLE_MONTHLY,
    TRANSPORT_MODES,
)


class TestPaths:
    def test_root_dir_is_directory(self):
        assert ROOT_DIR.is_dir()

    def test_data_raw_dir_is_under_root(self):
        assert DATA_RAW_DIR.is_relative_to(ROOT_DIR)

    def test_data_proc_dir_is_under_root(self):
        assert DATA_PROC_DIR.is_relative_to(ROOT_DIR)

    def test_db_path_is_under_proc_dir(self):
        assert DB_PATH.is_relative_to(DATA_PROC_DIR)

    def test_db_path_has_duckdb_extension(self):
        assert DB_PATH.suffix == ".duckdb"


class TestSourceConfig:
    def test_base_url_is_https(self):
        assert BASE_URL.startswith("https://")

    def test_first_year_is_reasonable(self):
        assert 2019 <= FIRST_YEAR <= datetime.date.today().year

    def test_first_year_not_in_future(self):
        assert FIRST_YEAR <= datetime.date.today().year


class TestTransportModes:
    def test_dashboard_modes_present_in_transport_modes(self):
        for mode in DASHBOARD_MODES:
            assert mode in TRANSPORT_MODES, f"Dashboard mode '{mode}' missing from TRANSPORT_MODES"

    def test_modes_are_uppercase_keys(self):
        for key in TRANSPORT_MODES:
            assert key == key.upper(), f"Mode key '{key}' should be uppercase"

    def test_mode_labels_are_strings(self):
        for key, label in TRANSPORT_MODES.items():
            assert isinstance(label, str), f"Label for '{key}' should be a string"
            assert len(label) > 0

    def test_dashboard_modes_is_list(self):
        assert isinstance(DASHBOARD_MODES, list)
        assert len(DASHBOARD_MODES) > 0

    def test_dashboard_modes_are_uppercase(self):
        for mode in DASHBOARD_MODES:
            assert mode == mode.upper()

    def test_mode_colors_defined_for_all_dashboard_modes(self):
        for mode in DASHBOARD_MODES:
            assert mode in MODE_COLORS, f"No color defined for dashboard mode '{mode}'"

    def test_mode_colors_are_hex_strings(self):
        for mode, color in MODE_COLORS.items():
            assert isinstance(color, str)
            assert color.startswith("#"), f"Color for '{mode}' should be a hex string: {color}"
            assert len(color) == 7, f"Color for '{mode}' should be 7 chars (#RRGGBB): {color}"


class TestEvents:
    def test_events_is_list(self):
        assert isinstance(EVENTS, list)

    def test_events_not_empty(self):
        assert len(EVENTS) > 0

    def test_each_event_has_required_keys(self):
        for event in EVENTS:
            assert "date" in event, f"Event missing 'date': {event}"
            assert "label_es" in event, f"Event missing 'label_es': {event}"
            assert "label_en" in event, f"Event missing 'label_en': {event}"
            assert "color" in event, f"Event missing 'color': {event}"

    def test_event_dates_are_valid(self):
        for event in EVENTS:
            try:
                datetime.date.fromisoformat(event["date"])
            except ValueError:
                pytest.fail(f"Invalid date format in event: {event['date']}")

    def test_event_dates_span_a_reasonable_range(self):
        # Sanity check: events should cover 2020 onwards (SUBE data window)
        dates = [datetime.date.fromisoformat(e["date"]) for e in EVENTS]
        assert min(dates) >= datetime.date(2020, 1, 1)
        assert max(dates) <= datetime.date.today() + datetime.timedelta(days=365)

    def test_event_labels_are_non_empty_strings(self):
        for event in EVENTS:
            assert isinstance(event["label_es"], str) and event["label_es"].strip()
            assert isinstance(event["label_en"], str) and event["label_en"].strip()

    def test_event_colors_are_non_empty_strings(self):
        for event in EVENTS:
            assert isinstance(event["color"], str) and event["color"].strip()


class TestFareHikes:
    def test_fare_hikes_is_list(self):
        assert isinstance(FARE_HIKES, list)

    def test_fare_hikes_not_empty(self):
        assert len(FARE_HIKES) > 0

    def test_each_hike_has_required_keys(self):
        for hike in FARE_HIKES:
            assert "date" in hike, f"Fare hike missing 'date': {hike}"
            assert "scope" in hike, f"Fare hike missing 'scope': {hike}"
            assert "magnitude" in hike, f"Fare hike missing 'magnitude': {hike}"

    def test_hike_dates_are_valid(self):
        for hike in FARE_HIKES:
            try:
                datetime.date.fromisoformat(hike["date"])
            except ValueError:
                pytest.fail(f"Invalid date format in fare hike: {hike['date']}")

    def test_hike_magnitudes_are_numeric(self):
        for hike in FARE_HIKES:
            assert isinstance(hike["magnitude"], (int, float)), (
                f"magnitude should be numeric for hike on {hike['date']}: {hike['magnitude']}"
            )

    def test_hike_magnitudes_are_non_negative(self):
        # Magnitude 0 = freeze (a structural break, not a price change) — still valid.
        for hike in FARE_HIKES:
            assert hike["magnitude"] >= 0, (
                f"Negative magnitude on {hike['date']}: {hike['magnitude']}"
            )

    def test_hike_scopes_are_valid(self):
        valid_scopes = {"national", "amba", "amba_local", "interior"}
        for hike in FARE_HIKES:
            assert hike["scope"] in valid_scopes, (
                f"Unknown scope '{hike['scope']}' on {hike['date']}"
            )

    def test_fare_hikes_are_in_non_decreasing_chronological_order(self):
        dates = [datetime.date.fromisoformat(h["date"]) for h in FARE_HIKES]
        assert dates == sorted(dates), "Fare hikes should be in non-decreasing chronological order"


class TestTableNames:
    def test_table_names_are_strings(self):
        assert isinstance(TABLE_CLEAN, str)
        assert isinstance(TABLE_MONTHLY, str)

    def test_table_names_not_empty(self):
        assert len(TABLE_CLEAN) > 0
        assert len(TABLE_MONTHLY) > 0

    def test_table_names_are_distinct(self):
        assert TABLE_CLEAN != TABLE_MONTHLY

    def test_table_names_are_valid_sql_identifiers(self):
        """Table names should contain only alphanumeric characters and underscores."""
        import re
        for name in (TABLE_CLEAN, TABLE_MONTHLY):
            assert re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name), (
                f"Table name '{name}' is not a valid SQL identifier"
            )