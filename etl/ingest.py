"""
etl/ingest.py — Download SUBE CSV files from datos.transporte.gob.ar.

Self-updating strategy:
  - Years 2020 → (current_year) are derived dynamically at runtime.
  - Historical files (already downloaded) are skipped unless --force is passed.
  - The current year's file is ALWAYS re-downloaded (it grows daily).
"""

import datetime
import hashlib
import shutil
import sys
from pathlib import Path

import requests
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import BASE_URL, DATA_RAW_DIR, FIRST_YEAR


def _csv_url(year: int) -> str:
    return f"{BASE_URL}/dat-ab-usos-{year}.csv"


def _file_path(year: int) -> Path:
    return DATA_RAW_DIR / f"dat-ab-usos-{year}.csv"


def _file_hash(path: Path) -> str:
    """MD5 of file contents — used to detect upstream changes."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_year(year: int, force: bool = False) -> dict:
    """
    Download a single year's CSV.

    Returns a dict with keys: year, path, status (skipped|updated|new|failed)
    """
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    url  = _csv_url(year)
    path = _file_path(year)

    current_year = datetime.date.today().year
    is_current   = (year == current_year)

    # Historical files: skip if already present and not forced
    if path.exists() and not force and not is_current:
        logger.info(f"[{year}] Already downloaded — skipping (use force=True to re-download)")
        return {"year": year, "path": path, "status": "skipped"}

    old_hash = _file_hash(path) if path.exists() else None

    try:
        logger.info(f"[{year}] Downloading {url} …")
        response = requests.get(url, timeout=60, stream=True)
        response.raise_for_status()

        tmp_path = path.with_suffix(".tmp")
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)

        new_hash = _file_hash(tmp_path)

        if old_hash and new_hash == old_hash:
            tmp_path.unlink()
            logger.info(f"[{year}] File unchanged (same hash) — skipping write")
            return {"year": year, "path": path, "status": "skipped"}

        shutil.move(str(tmp_path), str(path))
        status = "updated" if old_hash else "new"
        logger.success(f"[{year}] {status.upper()} — saved to {path}")
        return {"year": year, "path": path, "status": status}

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            # Year file doesn't exist yet on the server (e.g., future year)
            logger.warning(f"[{year}] File not found (404) — skipping")
        else:
            logger.error(f"[{year}] HTTP error: {e}")
        if tmp_path := path.with_suffix(".tmp"):
            tmp_path.unlink(missing_ok=True)
        return {"year": year, "path": None, "status": "failed"}

    except Exception as e:
        logger.error(f"[{year}] Unexpected error: {e}")
        return {"year": year, "path": None, "status": "failed"}


def download_all(force: bool = False) -> list[dict]:
    """
    Download all years from FIRST_YEAR to current year (inclusive).
    Always re-fetches the current year since it's updated daily.
    """
    current_year = datetime.date.today().year
    years        = list(range(FIRST_YEAR, current_year + 1))

    logger.info(f"Ingesting years: {years}")
    results = [download_year(y, force=force) for y in years]

    new_or_updated = [r for r in results if r["status"] in ("new", "updated")]
    logger.info(
        f"Done. {len(new_or_updated)} file(s) new/updated, "
        f"{len([r for r in results if r['status'] == 'skipped'])} skipped, "
        f"{len([r for r in results if r['status'] == 'failed'])} failed."
    )
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest SUBE CSV files")
    parser.add_argument("--force", action="store_true", help="Re-download all files")
    args = parser.parse_args()
    download_all(force=args.force)
