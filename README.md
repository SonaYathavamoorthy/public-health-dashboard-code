# Real-Time Public Health Surveillance Pipeline

An incremental-load data pipeline built on live CDC surveillance data,
demonstrating how a health system would keep an analytics warehouse
current against a regularly-updated government feed — without
re-processing the full history on every run.

## Business question

Public health teams need to know, every week, whether COVID-19,
pneumonia, or influenza deaths are trending up, down, or spiking
unexpectedly — and they need that signal fast, not after a month-long
batch reprocessing job. This project asks: *can we build a pipeline
that ingests a live government mortality feed, loads only what's new
each time it runs, and automatically flags weeks that look like the
start of a surge?*

**Data source:** [CDC/NCHS Provisional COVID-19 Death Counts by Week
Ending Date and State](https://data.cdc.gov/National-Center-for-Health-Statistics/Provisional-COVID-19-Death-Counts-by-Week-Ending-D/r8kw-7aab)
— a real, live, weekly-updated federal dataset (updated every Thursday),
pulled directly from CDC's public Socrata API. The snapshot in this repo
covers the United States, week ending 01/04/2020 through 08/29/2026 —
348 weekly records.

**Finding:** the data clearly shows three distinct COVID waves (peaking
~17,200 weekly deaths in April 2020, ~26,000 in January 2021, and
~21,400 in January 2022), a steady decline into an endemic pattern by
2023–2026, and 26 individual weeks where COVID deaths jumped more than
25% week-over-week — the kind of spike a surveillance dashboard would
flag for follow-up.

## Why this project is different from the EHR pipeline

The [Synthetic EHR Patient Journey Pipeline](../synthetic-ehr-pipeline)
is a **batch** pipeline: it wipes and reloads the full warehouse every
run. This project is an **incremental** pipeline: it tracks a
"watermark" (the last date successfully loaded) and only ingests rows
newer than that on each run — the pattern a real recurring data feed
actually needs, since re-loading years of history every time a new
week of data arrives doesn't scale.

## Architecture

```
CDC Socrata API (live, updates weekly)
        │
        ▼
  raw CSV snapshot  ──►  extract/transform  ──►  SQLite (watermarked,
  (data/raw/)             (pandas)                incremental load)
                                                        │
                                                        ▼
                                          dashboard.py: trend chart,
                                          spike detection, excess-
                                          mortality indicator
```

## How the incremental load actually works

1. On first run, there's no watermark — the pipeline loads everything
   and records the latest `week_ending_date` it saw.
2. On every subsequent run, it only loads rows **newer than the saved
   watermark**, then advances the watermark.
3. This is proven with tests, not just described: `tests/test_pipeline.py`
   includes a test that appends one new row to the source file and
   asserts exactly one row gets loaded — and a test that reruns the
   pipeline twice and asserts zero duplicate rows land in the database.
4. `--full-refresh` is available to force a complete reload when needed
   (e.g., after a schema change).

## Getting started

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the pipeline (first run loads everything; reruns are incremental)
python src/pipeline.py

# 3. Generate the dashboard outputs
python src/dashboard.py

# 4. Run tests
pytest tests/ -v

# 5. (Optional) Refresh with live data from CDC
#    Replace data/raw/cdc_covid_pneumonia_influenza_weekly.csv with a
#    fresh pull from the source URL below, then rerun step 2 --
#    only the new weeks will be loaded.
```

**Source URL for refreshing data:**
`https://data.cdc.gov/api/views/r8kw-7aab/rows.csv?accessType=DOWNLOAD`

## Outputs

- `data/processed/weekly_mortality_trend.png` — COVID-19 and influenza deaths over time
- `data/processed/pct_expected_deaths.png` — excess-mortality indicator (% of expected baseline deaths)
- `data/processed/covid_death_spikes.csv` — every week flagged for a >25% week-over-week increase
- `data/processed/national_weekly_mortality.csv` — full cleaned national time series

## Tech stack

Python, pandas, SQLite, matplotlib, pytest. Data sourced live from
CDC's Socrata Open Data API (SODA).

## Possible extensions

- Swap the manual CSV refresh for a scheduled pull (cron, Airflow, or
  a GitHub Action) hitting the CDC API directly
- Deploy the dashboard as a live Streamlit app instead of static PNGs
- Extend to state-level granularity (the source dataset includes all
  50 states; this snapshot focuses on the national series)
- Add email/Slack alerting when a new spike is detected

## Disclaimer

Data is provisional and sourced directly from the CDC's National
Center for Health Statistics. It is public federal data, not
independently verified by this project's author.
