# 🚇 SUBE Transit Analytics

An end-to-end data engineering and data science project analyzing Argentina's SUBE public transit card system — from raw government CSVs to an interactive bilingual dashboard with statistical analysis and ML-powered demand forecasting.

**Live data from [datos.transporte.gob.ar](https://datos.transporte.gob.ar) — updated daily, automatically.**

![Update SUBE data](https://github.com/YOUR_USERNAME/SUBE-transit-analytics/actions/workflows/update_data.yaml/badge.svg)

---

## What this project does

Argentina's SUBE card is used for every bus, subway, and train trip in the country. The government publishes daily ridership counts per operator and mode going back to 2020 — over 5 years of data capturing COVID lockdowns, three inflation crises, a 118% devaluation, and the complete dismantling of transit subsidies.

This project ingests that data, cleans it, stores it in DuckDB, and surfaces it through a Streamlit dashboard with six analytical tabs and a Prophet-based forecasting engine.

---

## Dashboard

Six tabs, fully bilingual (Spanish / English):

| Tab | What it shows |
|-----|---------------|
| 📊 **Overview** | Daily ridership by mode with 7-day moving average, monthly modal split, top 10 operators by ridership |
| 🦠 **COVID-19** | The asymmetric collapse of March 2020 — SUBTE −92%, TREN −87%, COLECTIVO −58% — and the recovery curve through 2022 |
| 🔄 **Modal Substitution** | Month-over-month and year-over-year % change per mode across the full series, with fare hike and event annotations |
| 💪 **Resilience** | AMBA vs Interior ridership on dual axes, fare hike impact from Jan 2024, seasonal amplitude by region, ridership by province |
| 🔬 **Analysis** | Weekday × month heatmap; interactive STL decomposition (trend + seasonality + residuals) with anomaly detection |
| 🔮 **Forecast** | Prophet demand forecast 3–12 months ahead per mode, with 80% confidence intervals and a summary table |

All charts have annotated vertical lines for key historical events (ASPO lockdowns, devaluations, fare regime changes) and fare hike dates, with staggered labels to prevent overlap.

---

## Key findings

- **COVID-19 collapse was mode-specific.** SUBTE fell 92% in April 2020 because it almost exclusively serves office workers in CABA. TREN fell 87%. COLECTIVO fell only 58% — buses kept running for essential workers who couldn't work from home.

- **Recovery was also mode-specific.** When restrictions eased in 2021, SUBTE ridership grew faster month-over-month than COLECTIVO — suggesting the passengers who returned first were those with no alternative to the subway.

- **The 2024 fare shock is visible in the data.** Two fare hikes in January (+45%) and February (+66%) 2024 — triggered by the Milei devaluation — caused a measurable ridership drop in AMBA that does not appear in Interior provinces at the same timing. Interior absorbed a different shock (loss of the Compensation Fund) on a different schedule.

- **AMBA has lower seasonal amplitude than Interior.** The ratio of peak to trough month is consistently closer to 1.0 in AMBA than in Interior provinces, reflecting the higher share of formal commuting in the metro area versus seasonal and tourism-driven travel in the interior.

- **Seasonal patterns:** ridership peaks in March–April and August–September (Argentina's peak commuting months), and dips in January (summer holidays) and July (winter school break).

---

## Architecture

```
SUBE-transit-analytics/
│
├── config.py                    # All paths, URLs, constants, YAML loaders
├── run_pipeline.py              # Single entry point: ingest → clean → load
│
├── etl/
│   ├── ingest.py                # Download yearly CSVs; hash-based change detection;
│   │                            # always re-fetches current year (grows daily)
│   ├── clean.py                 # Encoding detection (UTF-8/latin-1), column alias
│   │                            # normalisation across schema versions, outlier flagging,
│   │                            # deduplication
│   └── load.py                  # Load into DuckDB; build 4 tables + 5 SQL views
│
├── analytics/
│   ├── time_series.py           # STL decomposition, anomaly detection, recovery index,
│   │                            # rolling averages
│   └── ml.py                    # Prophet forecasting with fare pressure regressor,
│                                # macro shock regressor, explicit structural changepoints
│
├── dashboard/
│   └── app.py                   # Streamlit dashboard — 6 tabs, bilingual, Plotly charts
│
├── data/
│   ├── raw/                     # Downloaded CSVs (gitignored)
│   ├── processed/               # sube.duckdb (committed by CI)
│   └── reference/
│       ├── events.yaml          # Historical events for chart annotations
│       └── fare_hikes.yaml      # Complete fare hike history — used as ML regressors
│
├── tests/                       # pytest suite — 240+ tests, no network calls
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_clean.py
│   ├── test_ingest.py
│   ├── test_load.py
│   ├── test_time_series.py
│   └── test_ml.py
│
└── .github/workflows/
    └── update_data.yaml          # Daily pipeline run + DB commit
```

---

## DuckDB schema

The ETL pipeline builds the following tables and views:

**Tables**

| Name | Description |
|------|-------------|
| `daily_transactions` | One row per (date, operator, line) — the core fact table |
| `monthly_transactions` | Pre-aggregated monthly rollup by mode |
| `monthly_by_provincia` | Monthly rollup by mode × province × AMBA flag |
| `top_empresas` | Cumulative ridership per operator, all-time |

**Views**

| Name | Description |
|------|-------------|
| `v_total_daily` | Total daily ridership across all modes (excludes suspicious rows) |
| `v_yoy_monthly` | Year-over-year % change per mode, using window functions |
| `v_modal_split` | Mode share % per month (sums to 100 per month) |
| `v_weekday_heatmap` | Average ridership by weekday × calendar month |
| `v_amba_vs_interior` | AMBA vs Interior share split by mode and month |

---

## Forecasting model

The `forecast_ridership()` function in `analytics/ml.py` fits a **Prophet** model per transit mode and forecasts up to 12 months ahead.

Beyond standard Prophet (trend + yearly seasonality + Argentine public holidays), two external regressors are added:

- **`fare_pressure`** — a cumulative fare hike index: the running sum of all hike magnitudes (in %) that have taken effect up to each month. More informative than a binary on/off flag because it encodes the accumulated burden of successive hikes. Standardized before fitting.

- **`macro_shock`** — a binary variable marking the regime change from the December 2023 devaluation (+118%) and subsidy cuts onward. Captures the purchasing-power collapse independently of the fare level.

Structural **changepoints** are fixed at the exact dates of every fare hike and macro event rather than discovered automatically — this substantially improves precision during high-volatility periods.

Mode-specific training windows handle the fact that SUBTE's COVID collapse was so severe it cannot be safely included in the trend fit.

---

## Tech stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| Language | Python 3.11+ | |
| Data ingestion | `requests` | Hash-based change detection, atomic writes |
| Data cleaning | `pandas` 2.0+ | |
| Analytical database | `DuckDB` | Columnar, in-process, SQL |
| Statistical analysis | `statsmodels` | STL decomposition |
| Forecasting | `prophet` | Meta's time series library |
| Dashboard | `Streamlit` + `Plotly` | 6 tabs, bilingual |
| Logging | `loguru` | Rotating daily log files |
| Testing | `pytest` | 240+ tests, fully mocked network |
| Dependency management | `uv` + `pyproject.toml` | |
| Automation | GitHub Actions | Daily pipeline run |

---

## Quickstart

### 1. Clone and install

```bash
git clone https://github.com/YOUR_USERNAME/SUBE-transit-analytics
cd SUBE-transit-analytics
pip install uv
uv sync
```

### 2. Run the ETL pipeline

```bash
python run_pipeline.py
```

This downloads CSVs for 2020 → current year, cleans them, and loads everything into `data/processed/sube.duckdb`. On subsequent runs only the current year is re-downloaded (historical files are skipped unless you pass `--force`).

### 3. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

### 4. Run the tests

```bash
pytest tests/ -v
```

---

## Self-updating setup

### Option A: Cron (local machine)

```bash
# Add to crontab (crontab -e):
0 7 * * * cd /path/to/SUBE-transit-analytics && python run_pipeline.py >> logs/cron.log 2>&1
```

### Option B: GitHub Actions (recommended)

The workflow in `.github/workflows/update_data.yaml` runs daily at 07:00 ART (10:00 UTC):

1. Checks out the repo
2. Installs dependencies with `uv sync`
3. Runs `run_pipeline.py` (re-downloads only the current year's file)
4. Commits the updated `data/processed/sube.duckdb` back to the repo with `[skip ci]` to avoid loops

Only commits when the database actually changed — if there's no new data from the source, the step exits cleanly. You can also trigger it manually from the Actions tab at any time.

### Option C: Streamlit Cloud

Deploy `dashboard/app.py` to [share.streamlit.io](https://share.streamlit.io). The app re-queries the DB on each session start and picks up the latest committed database automatically.

---

## Data source

| Dataset | URL pattern | Update frequency | License |
|---------|-------------|-----------------|---------|
| SUBE daily transactions | `archivos-datos.transporte.gob.ar/upload/Dat_Ab_Usos/dat-ab-usos-{YEAR}.csv` | Daily | CC Attribution 4.0 |

---

## License

Data: [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) (datos.transporte.gob.ar)  
Code: MIT