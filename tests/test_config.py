"""
tests/test_config.py — Tests for config.py

Validates that all configuration values are sane and self-consistent.
"""

import datetime
from pathlib import Path

import pytest

from config import (
    BASE_URL,
    DATA_PROC_DIR,
    DATA_RAW_DIR,
    DB_PATH,
    EVENTS,
    FIRST_YEAR,
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
    def test_all_expected_modes_present(self):
        for mode in ("COLECTIVO", "TREN", "SUBTE", "PREMETRO"):
            assert mode in TRANSPORT_MODES

    def test_modes_are_uppercase_keys(self):
        for key in TRANSPORT_MODES:
            assert key == key.upper(), f"Mode key '{key}' should be uppercase"

    def test_mode_labels_are_strings(self):
        for key, label in TRANSPORT_MODES.items():
            assert isinstance(label, str), f"Label for '{key}' should be a string"
            assert len(label) > 0


class TestEvents:
    def test_events_is_list(self):
        assert isinstance(EVENTS, list)

    def test_events_not_empty(self):
        assert len(EVENTS) > 0

    def test_each_event_has_required_keys(self):
        for event in EVENTS:
            assert "date" in event, f"Event missing 'date': {event}"
            assert "label" in event, f"Event missing 'label': {event}"
            assert "color" in event, f"Event missing 'color': {event}"

    def test_event_dates_are_valid(self):
        for event in EVENTS:
            try:
                datetime.date.fromisoformat(event["date"])
            except ValueError:
                pytest.fail(f"Invalid date format in event: {event['date']}")

    def test_events_are_sorted_chronologically(self):
        dates = [datetime.date.fromisoformat(e["date"]) for e in EVENTS]
        assert dates == sorted(dates), "Events should be in chronological order"

    def test_event_dates_not_in_future(self):
        today = datetime.date.today()
        for event in EVENTS:
            d = datetime.date.fromisoformat(event["date"])
            assert d <= today, f"Event date {event['date']} is in the future"


class TestTableNames:
    def test_table_names_are_strings(self):
        assert isinstance(TABLE_CLEAN, str)
        assert isinstance(TABLE_MONTHLY, str)

    def test_table_names_not_empty(self):
        assert len(TABLE_CLEAN) > 0
        assert len(TABLE_MONTHLY) > 0

    def test_table_names_are_distinct(self):
        assert TABLE_CLEAN != TABLE_MONTHLY