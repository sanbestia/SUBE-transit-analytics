# SUBE Transit Analytics

Exploring public transportation usage in Argentina using an automated data pipeline and time-series analysis.

This project analyzes public transit usage based on official **SUBE smart-card data** published by Argentina's Ministry of Transport.

The system automatically downloads, cleans, stores, and analyzes the data, producing statistical insights and an interactive dashboard.

The goal of the project is to demonstrate a **complete analytics pipeline**, including:

* Automated ETL
* Data validation and cleaning
* Analytics warehousing
* Time-series analysis
* Interactive visualization

---

# Architecture

datos.transporte.gob.ar
↓
ETL ingestion
↓
Data cleaning + normalization
↓
DuckDB analytics warehouse
↓
Statistical analysis
↓
Streamlit dashboard

---

# Repository Structure

```
etl/
    ingest.py   # download raw datasets
    clean.py    # schema normalization and validation
    load.py     # load into DuckDB and build analytics tables

analytics/
    time_series.py  # statistical analysis and anomaly detection

dashboard/
    app.py      # Streamlit dashboard

tests/
    automated unit tests

data/
    raw/        # downloaded CSV files
    processed/  # DuckDB database
    reference/  # contextual datasets (events, fare hikes)
```

---

# Features

## Automated ETL Pipeline

The pipeline downloads official SUBE datasets and processes them automatically.

Run the full pipeline:

```
python run_pipeline.py
```

Capabilities:

* incremental updates
* automatic schema normalization
* encoding detection
* outlier detection
* deduplication
* reproducible pipeline

---

# Analytics Warehouse

Processed data is stored in **DuckDB**, a fast analytical database embedded in Python.

Core tables:

```
daily_transactions
monthly_transactions
monthly_by_provincia
top_empresas
```

Derived views support analytics queries:

```
v_total_daily
v_yoy_monthly
v_modal_split
v_weekday_heatmap
v_amba_vs_interior
```

---

# Time-Series Analysis

The project includes statistical analysis of transit usage:

* rolling averages (7-day, 30-day)
* STL decomposition
* anomaly detection
* post-COVID recovery index

Libraries used:

* statsmodels
* pandas
* numpy

---

# Interactive Dashboard

Launch the dashboard:

```
streamlit run dashboard/app.py
```

The dashboard visualizes:

* daily ridership trends
* modal share (bus / train / subway)
* weekday usage patterns
* regional comparisons
* anomalies aligned with real-world events

---

# Example Insights

The dataset allows exploration of several interesting patterns:

* Bus ridership recovered faster after COVID lockdowns than rail.
* Ridership shows strong weekly seasonality aligned with work patterns.
* Major anomalies correspond to lockdowns, strikes, and fare hikes.

---

# Running the Project

Install dependencies:

```
pip install -e .
```

Run the pipeline:

```
python run_pipeline.py
```

Launch the dashboard:

```
streamlit run dashboard/app.py
```

---

# Testing

Run the test suite:

```
pytest
```

Tests cover:

* ingestion logic
* data cleaning
* DuckDB loading
* analytics functions

---

# Technologies

* Python
* Pandas
* DuckDB
* Statsmodels
* Prophet
* Streamlit
* Plotly
* Pytest

---

# Data Source

Argentina Ministry of Transport open data portal:

https://datos.transporte.gob.ar

Dataset used:

Dat_Ab_Usos — SUBE transaction usage data.

---

# License

MIT
