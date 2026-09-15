"""
Incremental-load pipeline for CDC weekly COVID/pneumonia/influenza
mortality data (source: data.cdc.gov, dataset r8kw-7aab, NCHS).

Unlike the Synthetic EHR project (a one-time batch load), this pipeline
is designed to be run repeatedly against a live, regularly-updated CDC
feed -- it only loads rows it hasn't seen before, tracked via a
watermark file. This mirrors how a real production pipeline handles a
recurring data feed without re-processing the entire history every run.

Usage:
    python src/pipeline.py                  # normal incremental run
    python src/pipeline.py --full-refresh    # wipe and reload everything
"""
import argparse
import json
import logging
import sqlite3
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
RAW_CSV = ROOT / "data" / "raw" / "cdc_covid_pneumonia_influenza_weekly.csv"
DB_PATH = ROOT / "data" / "processed" / "public_health.db"
WATERMARK_PATH = ROOT / "data" / "processed" / "_watermark.json"

SOURCE_URL = "https://data.cdc.gov/api/views/r8kw-7aab/rows.csv?accessType=DOWNLOAD"


def load_watermark() -> str | None:
    """Return the last-loaded Week Ending Date we've already processed, or None."""
    if WATERMARK_PATH.exists():
        return json.loads(WATERMARK_PATH.read_text()).get("last_week_ending_date")
    return None


def save_watermark(week_ending_date: str) -> None:
    WATERMARK_PATH.write_text(json.dumps({"last_week_ending_date": week_ending_date}))


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_mortality (
            week_ending_date TEXT,
            state TEXT,
            year TEXT,
            mmwr_week INTEGER,
            covid_deaths REAL,
            total_deaths REAL,
            pct_expected_deaths REAL,
            pneumonia_deaths REAL,
            pneumonia_and_covid_deaths REAL,
            influenza_deaths REAL,
            pic_deaths REAL,
            loaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (week_ending_date, state)
        )
    """)
    conn.commit()


def extract_transform() -> pd.DataFrame:
    df = pd.read_csv(RAW_CSV, low_memory=False)
    df["week_ending_dt"] = pd.to_datetime(df["Week Ending Date"], format="%m/%d/%Y")
    df = df.rename(columns={
        "Week Ending Date": "week_ending_date", "State": "state", "Year": "year",
        "MMWR Week": "mmwr_week", "COVID-19 Deaths": "covid_deaths",
        "Total Deaths": "total_deaths", "Percent of Expected Deaths": "pct_expected_deaths",
        "Pneumonia Deaths": "pneumonia_deaths",
        "Pneumonia and COVID-19 Deaths": "pneumonia_and_covid_deaths",
        "Influenza Deaths": "influenza_deaths",
        "Pneumonia, Influenza, or COVID-19 Deaths": "pic_deaths",
    })
    cols = ["week_ending_date", "week_ending_dt", "state", "year", "mmwr_week",
            "covid_deaths", "total_deaths", "pct_expected_deaths", "pneumonia_deaths",
            "pneumonia_and_covid_deaths", "influenza_deaths", "pic_deaths"]
    return df[cols].sort_values("week_ending_dt")


def run(full_refresh: bool = False) -> dict:
    conn = get_connection()
    create_schema(conn)

    df = extract_transform()

    if full_refresh:
        log.info("Full refresh requested: clearing existing table and watermark.")
        conn.execute("DELETE FROM weekly_mortality")
        conn.commit()
        watermark = None
    else:
        watermark = load_watermark()

    if watermark:
        cutoff = pd.to_datetime(watermark, format="%m/%d/%Y")
        new_rows = df[df["week_ending_dt"] > cutoff]
        log.info(f"Watermark found ({watermark}). {len(new_rows)} new row(s) since last run.")
    else:
        new_rows = df
        log.info(f"No watermark found -- treating as first run. Loading all {len(new_rows)} rows.")

    if len(new_rows) == 0:
        log.info("Nothing new to load. Pipeline is up to date.")
        conn.close()
        return {"rows_loaded": 0, "status": "up_to_date"}

    load_df = new_rows.drop(columns=["week_ending_dt"])
    load_df.to_sql("weekly_mortality", conn, if_exists="append", index=False)
    conn.commit()

    max_date = new_rows["week_ending_dt"].max()
    save_watermark(max_date.strftime("%m/%d/%Y"))

    total = conn.execute("SELECT COUNT(*) FROM weekly_mortality").fetchone()[0]
    conn.close()

    log.info(f"Loaded {len(new_rows)} new row(s). Warehouse now has {total} total rows. "
             f"Watermark advanced to {max_date.date()}.")
    return {"rows_loaded": len(new_rows), "total_rows": total, "watermark": str(max_date.date())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-refresh", action="store_true",
                         help="Ignore the watermark and reload all data from scratch.")
    args = parser.parse_args()
    run(full_refresh=args.full_refresh)
