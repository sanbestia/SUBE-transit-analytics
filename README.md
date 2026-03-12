# 🚇 SUBE Transit Analytics

An end-to-end data engineering + data science project analyzing Argentina's SUBE public transit system. Built as a portfolio project showcasing ETL pipelines, DuckDB, statistical analysis, and interactive dashboards.

**Live data from [datos.transporte.gob.ar](https://datos.transporte.gob.ar) — updated daily, automatically.**

---

## Features

- **Self-updating ETL pipeline** — downloads new daily data automatically; skips already-downloaded historical files
- **DuckDB analytical layer** — columnar SQL, pre-built views for all dashboard queries
- **Smart data cleaning** — handles encoding issues, column aliases across years, thousand-separator formats, outlier flagging
- **Time series analysis** — STL decomposition (trend + seasonality + residuals), anomaly detection with z-score thresholding
- **Interactive Streamlit dashboard** — daily ridership, modal split, YoY % change, weekday heatmap, STL decomposition viewer
- **GitHub Actions** — automated daily refresh with database commit

---

## Project Structure

```
sube_analytics/
├── config.py                   # Central configuration (URLs, paths, events)
├── run_pipeline.py             # ETL entry point (run this first!)
│
├── etl/
│   ├── ingest.py               # Download CSVs from datos.transporte.gob.ar
│   ├── clean.py                # Parse, normalize, validate, flag outliers
│   └── load.py                 # Load into DuckDB, build views
│
├── analytics/
│   └── time_series.py          # STL decomposition, anomaly detection, recovery index
│
├── dashboard/
│   └── app.py                  # Streamlit dashboard
│
├── tests/
│   └── test_clean.py           # Pytest unit tests for cleaning logic
│
├── data/
│   ├── raw/                    # Downloaded CSVs (gitignored except .gitkeep)
│   └── processed/              # sube.duckdb (committed by GitHub Actions)
│
├── logs/                       # Pipeline logs (rotated daily)
├── requirements.txt
└── .github/workflows/
    └── update_data.yml         # Daily auto-update via GitHub Actions
```

---

## Quickstart

### 1. Clone and install
```bash
git clone https://github.com/YOUR_USERNAME/sube_analytics
cd sube_analytics
pip install -r requirements.txt
```

### 2. Run the ETL pipeline
```bash
python run_pipeline.py
```

This will:
1. Download CSVs for 2020 → current year from `datos.transporte.gob.ar`
2. Clean and normalize all data
3. Load into `data/processed/sube.duckdb`

On subsequent runs, only the current year's file is re-downloaded (historical files are skipped unless you pass `--force`).

### 3. Launch the dashboard
```bash
streamlit run dashboard/app.py
```

### 4. Run tests
```bash
pytest tests/ -v
```

---

## Self-Updating Setup

### Option A: Cron (local machine)
```bash
# Add to crontab (run `crontab -e`):
0 7 * * * cd /path/to/sube_analytics && python run_pipeline.py >> logs/cron.log 2>&1
```

### Option B: GitHub Actions (recommended)
1. Push this repo to GitHub
2. Enable Actions in your repo settings
3. The workflow in `.github/workflows/update_data.yml` runs daily at 7 AM (Argentina time)
4. Updated `sube.duckdb` is committed automatically

### Option C: Streamlit Cloud
Deploy `dashboard/app.py` to [share.streamlit.io](https://share.streamlit.io) for a public-facing dashboard. The app re-queries the DB on each session start.

---

## Data Source

| Dataset | URL | Update frequency |
|---------|-----|-----------------|
| SUBE transactions by day | `archivos-datos.transporte.gob.ar/upload/Dat_Ab_Usos/dat-ab-usos-{YEAR}.csv` | Daily |
| License | Creative Commons Attribution 4.0 | — |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data ingestion | `requests`, `hashlib` (change detection) |
| Data cleaning | `pandas` |
| Analytical DB | `DuckDB` |
| Statistical analysis | `statsmodels` (STL decomposition) |
| Dashboard | `Streamlit` + `Plotly` |
| Logging | `loguru` |
| Testing | `pytest` |
| Automation | GitHub Actions |

---

## Key Findings (to be filled after running)

- **COVID-19 impact**: ridership dropped ~XX% during the March 2020 ASPO lockdown
- **Recovery timeline**: pre-pandemic levels recovered by approximately [DATE]
- **Modal shift**: [COLECTIVO/SUBTE/TREN] share changed by X% since 2020
- **Seasonal patterns**: highest ridership in [MONTH], lowest in [MONTH]

---

## License

Data: Creative Commons Attribution 4.0 (datos.transporte.gob.ar)  
Code: MIT
