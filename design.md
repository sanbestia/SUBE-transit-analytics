# SUBE Transit Analytics — Design Document

## What this project does

This project downloads Argentina's public transit ridership data (SUBE card system), cleans and stores it, runs statistical analysis, and serves an interactive bilingual dashboard. It covers buses (Colectivo), trains (Tren), and the Buenos Aires subway (Subte) from 2013 to the present.

The pipeline runs daily via GitHub Actions: it downloads updated CSVs from the government's open data portal, processes them into a DuckDB database, and the Streamlit dashboard reads from that database.

---

## High-level architecture

```
Government CSVs (datos.transporte.gob.ar)
        │
        ▼
  etl/ingest.py        ← download, detect changes
        │
  etl/clean.py         ← parse, normalize, flag bad rows
        │
  etl/load.py          ← write to DuckDB, build views
        │
        ▼
  data/processed/sube.duckdb
        │
        ├── analytics/time_series.py   ← STL decomposition, anomaly detection
        ├── analytics/ml.py            ← Prophet demand forecasting
        └── analytics/causal.py        ← ITS regression (fare shock impact)
                │
                ▼
        dashboard/app.py               ← Streamlit UI (7 tabs, bilingual)
```

---

## File reference

### `config.py`
Central configuration. Defines paths, constants, and loads reference YAML files.

**Constants:**
- `ROOT_DIR`, `DATA_RAW_DIR`, `DATA_PROC_DIR`, `DATA_REF_DIR` — filesystem paths
- `BASE_URL` — government open data URL
- `FIRST_YEAR = 2020` — first year with daily granularity
- `TRANSPORT_MODES` — dict of all mode codes and their display names
- `DASHBOARD_MODES = ["COLECTIVO", "TREN", "SUBTE"]` — modes shown in the dashboard (LANCHAS excluded)
- `MODE_COLORS` — hex color per mode
- `TABLE_RAW`, `TABLE_CLEAN`, `TABLE_MONTHLY`, `TABLE_HISTORICAL` — DuckDB table name constants
- `EVENTS`, `FARE_HIKES`, `HOLIDAYS` — loaded from `data/reference/*.yaml` at import time

**Functions:**
- `_load_yaml(filename: str) -> list[dict]` — reads and parses a YAML file from `DATA_REF_DIR`

---

### `run_pipeline.py`
Single entry point to run the full ETL pipeline in sequence.

**Functions:**
- `main(force: bool = False) -> None`
  - Runs ingest → clean → load in order
  - Skips the whole run if no new CSVs were found and the database already exists
  - `force=True` re-downloads and reprocesses everything from scratch
  - Logs to `logs/pipeline_YYYY-MM-DD.log` and to the terminal

**Usage:**
```bash
python run_pipeline.py          # incremental (skip already-processed files)
python run_pipeline.py --force  # full reprocess
```

---

### `etl/ingest.py`
Downloads CSVs from the government portal. Only writes to disk when the file has actually changed (uses MD5 hash comparison).

**Functions:**
- `_csv_url(year: int) -> str` — builds the download URL for a given year
- `_file_path(year: int) -> Path` — returns the local path where the file will be saved
- `_file_hash(path: Path) -> str` — computes an MD5 hash of a file's contents
- `download_year(year: int, force: bool = False) -> dict`
  - Downloads one year's CSV
  - Returns a status dict: `{year, path, status}` where status is one of `new`, `updated`, `skipped`, `failed`
  - Skips past years if already downloaded and not forced; always re-downloads the current year (it grows daily)
  - Writes to a temp file first, then moves it atomically (prevents partial writes)
- `download_all(force: bool = False) -> list[dict]`
  - Calls `download_year()` for every year from 2020 to the current year
  - Returns a list of status dicts; logs a summary at the end

---

### `etl/ingest_historical.py`
Downloads and merges pre-2020 monthly ridership data from two different government sources, covering 2013–2019.

**Why two sources?** The government published historical data in two formats. Source B (2016–2019) is the cleaner one; Source A (2013–2019) fills the 2013–2015 gap where Source B doesn't exist.

**Functions:**
- `_download_csv(url: str, cache_name: str, force: bool = False) -> bytes`
  - Downloads and caches a CSV; skips re-download if content hasn't changed (SHA-256 sidecar)
- `_parse_mmodo(content: bytes) -> pd.DataFrame`
  - Parses Source B (semicolon-delimited, monthly totals per mode)
  - Returns: `(month_start, modo, total_usos, source)`
- `_parse_periodo_modo(content: bytes) -> pd.DataFrame`
  - Parses Source A (more flexible format, includes operation type)
  - Filters to trip-type rows only
  - Returns: same schema as `_parse_mmodo()`
- `download_historical(force: bool = False) -> pd.DataFrame`
  - Downloads both sources and merges them; Source B wins where both overlap
  - Returns: combined DataFrame with `(month_start, modo, total_usos, source)`
- `load_historical(df: pd.DataFrame, conn, table_name: str = "monthly_historical") -> None`
  - Writes the historical DataFrame to DuckDB
  - Clips SUBTE and TREN to 2016-01 (before that, SUBE registration was incomplete)
  - Adds columns: `year`, `month`, `amba='SI'`, `era='pre2020'`
  - Validates there are no gaps larger than 2 months per mode

---

### `etl/clean.py`
Parses raw CSVs, standardizes column names, validates data types, flags suspicious rows, and deduplicates.

**Key constants:**
- `COLUMN_ALIASES` — maps various upstream column name variations to standard names (e.g. `dia_transporte` → `fecha`)
- `COLUMNS_TO_KEEP` — the final set of columns retained after cleaning

**Functions:**
- `_normalize_columns(df: pd.DataFrame) -> pd.DataFrame` — lowercases and strips column names, applies alias mapping
- `_try_parse_csv(path: Path) -> pd.DataFrame`
  - Tries all combinations of encoding (UTF-8, Latin-1, UTF-8-BOM) and delimiter (comma, semicolon)
  - Returns the first parse that produces a valid table
- `clean_file(path: Path) -> pd.DataFrame`
  - Full cleaning pipeline for one file:
    1. Parse the CSV
    2. Normalize column names
    3. Check that required columns (`fecha`, `modo`, `cantidad_usos`) are present
    4. Parse dates
    5. Uppercase mode names, drop unknown modes
    6. Parse trip counts (removes non-digit characters)
    7. Normalize enrichment columns (operator name, line, jurisdiction, etc.)
    8. Flag suspicious rows: zero counts on weekdays, or statistical outliers (value is more than 4 standard deviations away from the mode's average)
    9. Deduplicate
    10. Add `year`, `month`, `source_file` columns
  - Returns cleaned DataFrame sorted by `(fecha, modo)`
- `clean_all() -> pd.DataFrame`
  - Cleans all CSVs in `DATA_RAW_DIR` and concatenates them
  - Runs a final deduplication pass across all years

---

### `etl/load.py`
Writes cleaned data into DuckDB and builds aggregated tables and views.

**Functions:**
- `get_connection() -> duckdb.DuckDBPyConnection` — opens (or creates) the database file
- `load(df: pd.DataFrame, conn=None) -> None`
  - Creates or replaces all tables from the clean DataFrame
  - All `CREATE` statements use `CREATE OR REPLACE` so re-running is safe
  - **Tables created:**
    - `daily_transactions` — one row per (date, operator, line, mode); the raw fact table
    - `monthly_transactions` — monthly totals per mode (suspicious rows excluded)
    - `monthly_by_provincia` — monthly totals broken down by province and AMBA flag
    - `top_empresas` — cumulative all-time trip count per operator
  - Calls `_create_views()` after loading tables
- `_create_views(conn) -> None`
  - Builds five SQL views on top of the tables (described in the schema section below)
- `query(sql: str, conn=None) -> pd.DataFrame` — runs any SQL string and returns a DataFrame; manages connection lifecycle

---

### `analytics/time_series.py`
Statistical time series functions: smoothing, STL decomposition, anomaly detection, and recovery indices.

**Functions:**
- `_get_total_daily(conn) -> pd.DataFrame` — pulls `(fecha, total_usos)` from `v_total_daily`
- `rolling_stats(conn) -> pd.DataFrame`
  - Adds 7-day and 30-day rolling averages to the daily total series
  - Returns: `(fecha, total_usos, ma_7d, ma_30d)`
- `decompose_series(conn, mode: str | None = None, period: int = 365) -> dict`
  - Splits the daily ridership series into three components:
    - **Trend** — the long-term direction (growth or decline), stripped of seasonal effects
    - **Seasonality** — the repeating pattern (weekly or yearly)
    - **Residual** — what's left after removing trend and seasonality; should be random noise in normal times
  - `mode=None` uses all modes combined; a specific mode string filters to that mode
  - `period=365` for yearly seasonality; `period=7` for weekly
  - Returns a dict: `{original, trend, seasonal, residual}` — each a `pd.Series`
- `detect_anomalies(residuals: pd.Series, z_threshold: float = 3.0, lang: str = "es") -> pd.DataFrame`
  - Flags dates where the residual is unusually large (more than `z_threshold` standard deviations from the mean)
  - Looks up matching dates in `EVENTS` and `HOLIDAYS` to attach human-readable labels
  - Returns: `(fecha, residual, z_score, is_anomaly, event_label)`
- `compute_recovery_index(conn, baseline_years: list[int] = [2022, 2023]) -> pd.DataFrame`
  - Expresses each month's ridership as a percentage of the average for the baseline years
  - Returns: `(month_start, year, month, modo, total_usos, baseline_avg, recovery_index)`
- `modal_statistics(conn) -> pd.DataFrame`
  - Summary KPIs per mode: total all-time trips, daily average, peak month, lowest month

---

### `analytics/ml.py`
Demand forecasting using Meta's Prophet model. Trains one model per transport mode and projects 6 months ahead.

**How it works:** Prophet decomposes ridership into trend + seasonal patterns + holidays + custom regressors. Four custom regressors are added:
- **COVID impact** — binary flag for March 2020–December 2021 (so the model doesn't confuse the pandemic with a structural trend change)
- **Fare pressure** — a running total of all cumulative fare increases, which grows each time a new hike is applied
- **Macro shock** — binary flag for December 2023 onward, capturing the devaluation's purchasing-power effect
- **Recovery momentum** — a smoothly decelerating variable that captures the gradual post-COVID bounce-back

Structural break dates (fare hikes, macro events) are given to the model explicitly rather than discovered automatically — this improves accuracy during volatile periods.

**Functions:**
- `_build_fare_pressure(df) -> pd.DataFrame` — adds the cumulative fare index column
- `_build_macro_shock(df) -> pd.DataFrame` — adds the macro shock binary column
- `_build_covid_impact(df) -> pd.DataFrame` — adds the COVID period binary column
- `_build_recovery_momentum(df) -> pd.DataFrame` — adds the post-COVID recovery momentum column
- `_all_changepoints(last_date) -> list[pd.Timestamp]` — returns all structural break dates before `last_date`
- `_forecast_floor(df) -> float` — returns 50% of the historical minimum as a lower bound (prevents forecasts going negative)
- `_make_future(model, horizon: int) -> pd.DataFrame` — builds the future DataFrame Prophet needs, with all regressors filled in
- `_load_monthly_mode(conn, mode: str) -> pd.DataFrame` — loads training data (pre-2020 historical + post-2020 pipeline) for one mode
- `forecast_ridership(conn, modes: list[str] | None = None, horizon: int = 6) -> dict[str, pd.DataFrame]`
  - **Main function.** Trains and runs Prophet for each mode.
  - Returns a dict keyed by mode; each value is a DataFrame with: `(ds, yhat, yhat_lower, yhat_upper, trend, is_forecast, actual)`
  - `yhat_lower`/`yhat_upper` form the 80% confidence band
- `forecast_summary(forecasts: dict) -> pd.DataFrame`
  - One-row-per-mode table comparing the average of the last 6 actual months against the forecast average
  - Returns: `(mode, last_actual, mean_forecast, pct_change, direction)` where direction is `up`, `down`, or `flat`

---

### `analytics/causal.py`
Estimates the causal impact of the January 2024 fare shock using a statistical method called Interrupted Time Series (ITS).

**How it works:** The model learns ridership behaviour in the years before the shock (controlling for seasonality, COVID, and long-term trend), then projects forward as if the shock never happened. The gap between the projected line and the actual line is the estimated impact. It measures two things: whether there was an abrupt jump in January 2024 and whether the monthly trend gradually changed afterwards.

**Important caveat:** the December 2023 devaluation (+118%) happened at the same time as the fare hikes, so the model cannot separate these two effects.

**Constants:**
- `TREATMENT_DATE = 2024-01-01` — when the shock started
- `HAC_MODES = {"TREN", "SUBTE"}` — modes where autocorrelation was detected; use Newey-West standard errors

**Functions:**
- `_cumulative_hike_pct(from_date, to_date, scopes) -> float` — computes the compound fare increase between two dates for the given fare scopes
- `_build_its_features(df, treatment_date) -> pd.DataFrame` — adds the regression variables to the data: time index, post-treatment indicator, months-since-treatment counter, COVID flag, and month-of-year dummies
- `_counterfactual(df, result) -> pd.Series` — given a fitted model, generates what ridership would have been without the shock (sets post-treatment variables to zero)
- `its_analysis(conn, modes=None, treatment_date=TREATMENT_DATE) -> pd.DataFrame`
  - **Main function.** Runs the regression for each mode.
  - Returns one row per mode with: model coefficients, standard errors, p-values, confidence intervals, R², standard error type (`OLS` or `HAC-12`), and the raw model object for counterfactual plotting
- `build_counterfactual_df(row, treatment_date) -> pd.DataFrame`
  - Given one row from `its_analysis()`, builds a DataFrame ready for plotting
  - Returns post-treatment months with: `(ds, actual, fitted, counterfactual, gap)`

---

### `analytics/diagnostics.py`
Validates Prophet model quality and determines whether its residuals show autocorrelation (which would affect the reliability of the ITS standard errors).

**Functions:**
- `mape(actual, predicted) -> float` — Mean Absolute Percentage Error (e.g. 5.2 for 5.2%)
- `rmse(actual, predicted) -> float` — Root Mean Squared Error
- `mae(actual, predicted) -> float` — Mean Absolute Error
- `ljung_box_test(residuals, lags: int = 12) -> dict`
  - Tests whether the model's residuals are random noise or contain patterns
  - Returns: `{lb_stat, lb_pvalue, autocorrelated (bool)}`
- `diagnose_mode(conn, mode: str, save_plots: bool = False, output_dir=None) -> dict`
  - Fits Prophet and computes diagnostic metrics broken down by period (pre-COVID, COVID, post-COVID)
  - Gives a verdict: `'good'` (MAPE ≤ 5%), `'acceptable'` (≤ 12%), or `'poor'` (> 12%)
  - Optionally saves diagnostic plots to disk
  - Returns a dict with all metrics and the verdict
- `print_summary(results: list[dict]) -> None` — prints a formatted table comparing all modes

---

### `dashboard/app.py`
The Streamlit web application. Reads from DuckDB, builds Plotly charts, and renders them in a 7-tab bilingual interface.

**Session state:**
- `st.session_state.lang` — `'es'` or `'en'`, toggled by a sidebar button

**Helper functions (not cached):**
- `t(key: str) -> str` — returns the translated string for the current language
- `mode_label(mode: str) -> str` — returns the display name for a mode (e.g. `"SUBTE"` → `"Subway"` in English)
- `event_label(ev: dict) -> str` — returns the event annotation text in the current language

**Cached data loaders** (refresh every hour, `ttl=3600`):
- `load_monthly()` — monthly aggregates (COLECTIVO, TREN, SUBTE)
- `load_combined_monthly()` — pre-2020 historical + post-2020 pipeline, unified series
- `load_daily_totals()` — daily trip counts per mode (2020–present)
- `load_modal_split()` — percentage share per mode per month
- `load_yoy()` — year-over-year % change per mode
- `load_heatmap()` — average trips by weekday × calendar month
- `load_amba_recovery()` — AMBA vs Interior monthly totals with recovery index
- `load_amba_by_mode()` — monthly totals broken down by AMBA/Interior and mode
- `load_top_empresas()` — top 10 operators by cumulative ridership
- `load_by_provincia()` — total trips per province
- `load_its()` — ITS regression results (cached; computation is expensive)

**Chart annotation helpers:**
- `add_event_annotations(fig, y_ref, x_min, x_max)` — draws dotted vertical lines for historical events
- `add_fare_annotations(fig)` — draws dashed vertical lines for fare hikes

**Dashboard tabs:**
| Tab | Contents |
|-----|----------|
| Data Structure | Daily ridership (with per-chart mode + date selectors), modal split area chart, top 10 operators bar chart |
| Anomalies | STL decomposition chart + anomaly table; includes weekday × month heatmap with lockdown filter |
| Forecast | Prophet 6-month forecast per mode with confidence band, summary table |
| Fare Impact | ITS counterfactual chart, gap (shaded area), per-mode metric cards |
| COVID-19 | Monthly collapse chart, modal recovery index, YoY change |
| Modal Substitution | Month-over-month % bars, modal share evolution, year-over-year % |
| AMBA vs Interior | Absolute ridership, recovery index with rolling average, seasonal amplitude, province bar chart |

All per-chart mode selectors are three individual checkboxes (one per mode, all on by default). Date selectors are two separate fields (From / To).

---

### `dashboard/utils.py`
Pure helper functions used by `app.py`. No Streamlit dependencies — fully unit-testable.

**DB query functions** (each takes a DuckDB connection, returns a DataFrame):

| Function | What it queries | Key output columns |
|---|---|---|
| `load_monthly(conn)` | `monthly_transactions` | `month_start, modo, total_usos` |
| `load_daily_totals(conn)` | `daily_transactions` | `fecha, modo, cantidad_usos` |
| `load_modal_split(conn)` | `v_modal_split` | `month_start, modo, mode_share_pct` |
| `load_yoy(conn)` | `v_yoy_monthly` | `month_start, modo, yoy_pct_change` |
| `load_heatmap(conn)` | `v_weekday_heatmap` | `day_of_week, month, avg_usos` |
| `load_amba_by_mode(conn)` | `monthly_by_provincia` | `month_start, amba, modo, total` |
| `load_amba_recovery(conn)` | `monthly_by_provincia` | `month_start, amba, total, recovery_index` |
| `load_top_empresas(conn)` | `top_empresas` | `nombre_empresa, modo, total_usos` |
| `load_by_provincia(conn)` | `monthly_by_provincia` | `provincia, total` |
| `load_historical_monthly(conn)` | `monthly_historical` | `month_start, modo, total_usos` |
| `load_combined_monthly(conn)` | historical + pipeline union | `month_start, modo, total_usos` |

**Data transform functions:**
- `compute_mom_pct(df) -> pd.DataFrame`
  - Adds a `mom_pct` column: the percentage change from the previous month for each mode
  - Input must have `(modo, month_start, total_usos)`; drops the first row per mode (no previous month to compare)
- `index_to_baseline(df, baseline_date, value_col='total_usos', group_col='modo') -> pd.DataFrame`
  - Sets the value at `baseline_date` to 100 and expresses all other values relative to it
  - Used to compare recovery across modes regardless of their different absolute volumes

**Chart annotation functions:**
- `_staggered_annotations(fig, entries, line_dash, x_min, x_max, position) -> go.Figure`
  - Draws non-overlapping vertical lines with labels that shift vertically to avoid collisions
  - `position='top'` for historical events, `position='bottom'` for fare hikes
- `add_event_annotations(fig, lang, x_min, x_max) -> go.Figure`
  - Draws dotted lines for events from `config.EVENTS`
- `add_fare_annotations(fig, lang, scope_filter, x_min, x_max) -> go.Figure`
  - Draws dashed lines for fare hikes from `config.FARE_HIKES`
  - Color-coded by scope: national (purple), AMBA (violet), local (pink)

**Helper functions:**
- `mode_color_map() -> dict` — returns `{mode: hex_color}` for all dashboard modes
- `hex_to_rgb(hex_color: str) -> tuple[float, float, float]` — converts `#RRGGBB` to `(r, g, b)` floats in 0–1 range

---

### `dashboard/strings.py`
All UI text in both Spanish and English, in a single dict.

**Structure:**
```python
STRINGS = {
    "es": { "key": "Spanish text", ... },
    "en": { "key": "English text", ... }
}

MODE_LABELS = {
    "es": {"COLECTIVO": "Colectivo", "TREN": "Tren",   "SUBTE": "Subte"},
    "en": {"COLECTIVO": "Bus",       "TREN": "Train",  "SUBTE": "Subway"},
}
```

Every string used in `app.py` (tab names, axis labels, chart explainers, KPI labels, button text, finding callouts) lives here. Adding a new language means adding a new top-level key.

---

## Database schema

### Tables

| Table | Description |
|-------|-------------|
| `daily_transactions` | Core fact table. One row per (date, operator, line). Includes a boolean `is_suspicious` flag. |
| `monthly_transactions` | Monthly totals per mode. Suspicious rows are excluded. |
| `monthly_by_provincia` | Monthly totals per mode × province × AMBA flag. |
| `top_empresas` | All-time cumulative trip count per operator. |
| `monthly_historical` | Pre-2020 AMBA monthly ridership (2013–2019). |

### Views (built on top of tables)

| View | What it computes |
|------|-----------------|
| `v_total_daily` | Daily trip total across all modes (suspicious rows excluded) |
| `v_yoy_monthly` | Year-over-year % change per mode — compares each month to the same month in the prior year |
| `v_modal_split` | Each mode's percentage share of total monthly trips |
| `v_weekday_heatmap` | Average daily trips for each combination of weekday and calendar month |
| `v_amba_vs_interior` | Monthly ridership split between AMBA and the rest of the country, with share % |

---

## Reference data files

All three files live in `data/reference/` and are loaded at startup by `config.py`.

### `events.yaml`
Historical events for chart annotations. Each entry has a date, Spanish label, English label, color code, and optional notes. Covers major events from 2014 to 2026 (devaluations, lockdowns, elections, strikes, fare shocks).

### `fare_hikes.yaml`
SUBE fare change history. Each entry has a date, scope (`national`, `amba`, `amba_local`, `interior`), magnitude (% increase), and labels. Scope matters because AMBA fares are set by the national government; interior fares are set by provinces.

### `holidays.yaml`
Argentine national holidays for 2020–2026, with the actual observed date after applying the moveable holiday rules. Types: fixed, moveable, bridge days, Easter week, and Carnival.

---

## Automation

A GitHub Actions workflow runs daily at 07:00 ART:
1. Checks out the repo
2. Installs dependencies via `uv sync`
3. Runs `python run_pipeline.py` (incremental — skips unchanged files)
4. Commits the updated `sube.duckdb` if it changed, with `[skip ci]` to avoid a loop

---

## Tech stack

| Purpose | Library |
|---------|---------|
| Data download | `requests` |
| Data processing | `pandas` |
| Database | `duckdb` |
| Statistical analysis | `statsmodels` (STL decomposition, regression) |
| Forecasting | `prophet` (Meta) |
| Dashboard | `streamlit` + `plotly` |
| Logging | `loguru` |
| Testing | `pytest` |
| Dependency management | `uv` |
