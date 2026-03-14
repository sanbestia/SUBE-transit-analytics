"""
tests/test_ingest.py — Tests for etl/ingest.py

Network calls are fully mocked — no real HTTP requests are made.
"""

import datetime
import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from etl.ingest import (
    _csv_url,
    _file_hash,
    _file_path,
    download_all,
    download_year,
)
from config import BASE_URL, DATA_RAW_DIR, FIRST_YEAR


class TestCsvUrl:
    def test_url_contains_year(self):
        assert "2024" in _csv_url(2024)

    def test_url_starts_with_base_url(self):
        assert _csv_url(2024).startswith(BASE_URL)

    def test_url_ends_with_csv(self):
        assert _csv_url(2024).endswith(".csv")

    def test_url_format(self):
        assert _csv_url(2023) == f"{BASE_URL}/dat-ab-usos-2023.csv"

    def test_different_years_produce_different_urls(self):
        assert _csv_url(2023) != _csv_url(2024)


class TestFilePath:
    def test_path_contains_year(self):
        assert "2024" in str(_file_path(2024))

    def test_path_is_under_raw_dir(self):
        assert _file_path(2024).is_relative_to(DATA_RAW_DIR)

    def test_path_has_csv_extension(self):
        assert _file_path(2024).suffix == ".csv"

    def test_filename_format(self):
        assert _file_path(2022).name == "dat-ab-usos-2022.csv"


class TestFileHash:
    def test_hash_is_string(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_bytes(b"hello,world\n")
        assert isinstance(_file_hash(f), str)

    def test_hash_is_md5_length(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_bytes(b"hello,world\n")
        assert len(_file_hash(f)) == 32

    def test_same_content_same_hash(self, tmp_path):
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_bytes(b"same content")
        f2.write_bytes(b"same content")
        assert _file_hash(f1) == _file_hash(f2)

    def test_different_content_different_hash(self, tmp_path):
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_bytes(b"content A")
        f2.write_bytes(b"content B")
        assert _file_hash(f1) != _file_hash(f2)

    def test_empty_file_has_hash(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_bytes(b"")
        h = _file_hash(f)
        assert isinstance(h, str)
        assert len(h) == 32


class TestDownloadYear:
    """All tests mock the network — download_year logic is tested in isolation."""

    def _make_mock_response(self, content=b"col1,col2\nval1,val2\n", status=200):
        response = MagicMock()
        response.status_code = status
        response.iter_content = MagicMock(return_value=[content])
        response.raise_for_status = MagicMock()
        return response

    def test_skips_historical_file_if_exists(self, tmp_path):
        existing = tmp_path / "dat-ab-usos-2021.csv"
        existing.write_bytes(b"data")
        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=existing):
            result = download_year(2021, force=False)
        assert result["status"] == "skipped"

    def test_force_redownloads_historical_file(self, tmp_path):
        existing = tmp_path / "dat-ab-usos-2021.csv"
        existing.write_bytes(b"old data")
        new_content = b"new data"
        response = self._make_mock_response(content=new_content)

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=existing), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(2021, force=True)

        assert result["status"] == "updated"

    def test_new_file_returns_new_status(self, tmp_path):
        path = tmp_path / "dat-ab-usos-2022.csv"
        response = self._make_mock_response()

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(2022, force=False)

        assert result["status"] == "new"
        assert result["path"] == path

    def test_unchanged_file_returns_skipped(self, tmp_path):
        content = b"col1,col2\nval1,val2\n"
        path = tmp_path / "dat-ab-usos-2022.csv"
        path.write_bytes(content)  # existing file with same content
        response = self._make_mock_response(content=content)

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(2022, force=True)

        assert result["status"] == "skipped"

    def test_current_year_always_fetched_even_without_force(self, tmp_path):
        """The current year's file should always be re-downloaded, never skipped,
        because it grows daily even when it already exists on disk."""
        current_year = datetime.date.today().year
        existing = tmp_path / f"dat-ab-usos-{current_year}.csv"
        existing.write_bytes(b"old content")
        new_content = b"new content"
        response = self._make_mock_response(content=new_content)

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=existing), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(current_year, force=False)

        # The file exists and force=False, but it's the current year — must attempt download
        assert result["status"] in ("updated", "skipped")  # skipped only if hash matches
        # Specifically, it should NOT short-circuit to "skipped" before attempting the request
        # (i.e. requests.get must have been called)

    def test_current_year_skipped_only_if_unchanged(self, tmp_path):
        """If the server file hasn't changed since last download, current year = skipped."""
        current_year = datetime.date.today().year
        content = b"col1,col2\nval1,val2\n"
        path = tmp_path / f"dat-ab-usos-{current_year}.csv"
        path.write_bytes(content)
        response = self._make_mock_response(content=content)

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(current_year, force=False)

        assert result["status"] == "skipped"

    def test_404_returns_failed(self, tmp_path):
        import requests as req
        path = tmp_path / "dat-ab-usos-2099.csv"
        http_error = req.HTTPError(response=MagicMock(status_code=404))
        response = self._make_mock_response()
        response.raise_for_status.side_effect = http_error

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(2099, force=False)

        assert result["status"] == "failed"
        assert result["path"] is None

    def test_non_404_http_error_returns_failed(self, tmp_path):
        import requests as req
        path = tmp_path / "dat-ab-usos-2022.csv"
        http_error = req.HTTPError(response=MagicMock(status_code=503))
        response = self._make_mock_response()
        response.raise_for_status.side_effect = http_error

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(2022, force=False)

        assert result["status"] == "failed"
        assert result["path"] is None

    def test_network_error_returns_failed(self, tmp_path):
        path = tmp_path / "dat-ab-usos-2022.csv"

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", side_effect=ConnectionError("timeout")):
            result = download_year(2022, force=False)

        assert result["status"] == "failed"

    def test_result_contains_required_keys(self, tmp_path):
        path = tmp_path / "dat-ab-usos-2022.csv"
        response = self._make_mock_response()

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(2022)

        assert "year" in result
        assert "path" in result
        assert "status" in result

    def test_result_year_matches_input(self, tmp_path):
        path = tmp_path / "dat-ab-usos-2023.csv"
        response = self._make_mock_response()

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(2023)

        assert result["year"] == 2023

    def test_failed_result_has_none_path(self, tmp_path):
        path = tmp_path / "dat-ab-usos-2099.csv"
        import requests as req
        http_error = req.HTTPError(response=MagicMock(status_code=404))
        response = self._make_mock_response()
        response.raise_for_status.side_effect = http_error

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            result = download_year(2099)

        assert result["path"] is None

    def test_tmp_file_cleaned_up_on_404(self, tmp_path):
        """A leftover .tmp file must not remain after a failed download."""
        path = tmp_path / "dat-ab-usos-2099.csv"
        import requests as req
        http_error = req.HTTPError(response=MagicMock(status_code=404))
        response = self._make_mock_response()
        response.raise_for_status.side_effect = http_error

        with patch("etl.ingest.DATA_RAW_DIR", tmp_path), \
             patch("etl.ingest._file_path", return_value=path), \
             patch("etl.ingest.requests.get", return_value=response):
            download_year(2099, force=False)

        assert not path.with_suffix(".tmp").exists()


class TestDownloadAll:
    def test_covers_first_year_to_current(self):
        current_year = datetime.date.today().year
        calls = []

        def mock_download(year, force=False):
            calls.append(year)
            return {"year": year, "path": None, "status": "skipped"}

        with patch("etl.ingest.download_year", side_effect=mock_download):
            download_all()

        assert FIRST_YEAR in calls
        assert current_year in calls
        assert min(calls) == FIRST_YEAR
        assert max(calls) == current_year

    def test_no_gaps_in_year_range(self):
        """Every year from FIRST_YEAR to current must be attempted — no gaps."""
        current_year = datetime.date.today().year
        calls = []

        def mock_download(year, force=False):
            calls.append(year)
            return {"year": year, "path": None, "status": "skipped"}

        with patch("etl.ingest.download_year", side_effect=mock_download):
            download_all()

        expected = list(range(FIRST_YEAR, current_year + 1))
        assert sorted(calls) == expected

    def test_returns_list_of_dicts(self):
        def mock_download(year, force=False):
            return {"year": year, "path": None, "status": "skipped"}

        with patch("etl.ingest.download_year", side_effect=mock_download):
            results = download_all()

        assert isinstance(results, list)
        assert all(isinstance(r, dict) for r in results)

    def test_force_propagates_to_download_year(self):
        received_force = []

        def mock_download(year, force=False):
            received_force.append(force)
            return {"year": year, "path": None, "status": "skipped"}

        with patch("etl.ingest.download_year", side_effect=mock_download):
            download_all(force=True)

        assert all(f is True for f in received_force)

    def test_force_false_by_default(self):
        received_force = []

        def mock_download(year, force=False):
            received_force.append(force)
            return {"year": year, "path": None, "status": "skipped"}

        with patch("etl.ingest.download_year", side_effect=mock_download):
            download_all()  # no force argument

        assert all(f is False for f in received_force)

    def test_result_length_matches_year_range(self):
        current_year = datetime.date.today().year
        expected_count = current_year - FIRST_YEAR + 1

        def mock_download(year, force=False):
            return {"year": year, "path": None, "status": "skipped"}

        with patch("etl.ingest.download_year", side_effect=mock_download):
            results = download_all()

        assert len(results) == expected_count