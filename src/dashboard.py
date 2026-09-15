"""
Builds the analysis outputs on top of the incrementally-loaded warehouse:
  1. Weekly trend chart (COVID / pneumonia / influenza deaths, US)
  2. Week-over-week spike detection -- flags weeks where deaths jumped
     sharply, the kind of signal a real surveillance dashboard would alert on
  3. Percent-of-expected-deaths trend (a standard excess-mortality indicator)

Usage: python src/dashboard.py
"""
import logging
import sqlite3
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "processed" / "public_health.db"
OUT_DIR = ROOT / "data" / "processed"

SPIKE_THRESHOLD = 0.25  # flag any week where covid_deaths rose >25% week-over-week


def load_national(conn) -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT week_ending_date, covid_deaths, total_deaths, pct_expected_deaths,
               pneumonia_deaths, influenza_deaths, pic_deaths
        FROM weekly_mortality
        WHERE state = 'United States'
        ORDER BY week_ending_date
    """, conn)
    df["week_ending_dt"] = pd.to_datetime(df["week_ending_date"], format="%m/%d/%Y")
    return df.sort_values("week_ending_dt").reset_index(drop=True)


def detect_spikes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["covid_deaths_pct_change"] = df["covid_deaths"].pct_change()
    spikes = df[df["covid_deaths_pct_change"] > SPIKE_THRESHOLD].copy()
    return spikes[["week_ending_date", "covid_deaths", "covid_deaths_pct_change"]]


def main():
    conn = sqlite3.connect(DB_PATH)
    df = load_national(conn)
    conn.close()

    log.info(f"Loaded {len(df)} weekly national records "
             f"({df['week_ending_dt'].min().date()} to {df['week_ending_dt'].max().date()})")

    # --- Spike detection ---
    spikes = detect_spikes(df)
    spikes.to_csv(OUT_DIR / "covid_death_spikes.csv", index=False)
    log.info(f"Flagged {len(spikes)} weeks with >{int(SPIKE_THRESHOLD*100)}% "
              f"week-over-week increase in COVID deaths")

    # --- Trend chart ---
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df["week_ending_dt"], df["covid_deaths"], label="COVID-19 deaths", linewidth=1.3)
    ax.plot(df["week_ending_dt"], df["influenza_deaths"], label="Influenza deaths", linewidth=1.3)
    ax.set_ylabel("Weekly deaths (United States)")
    ax.set_title("Weekly COVID-19 and Influenza Deaths, US (NCHS Provisional Data)")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    fig.savefig(OUT_DIR / "weekly_mortality_trend.png", dpi=150)
    log.info(f"Saved chart: {OUT_DIR / 'weekly_mortality_trend.png'}")

    # --- Percent of expected deaths (excess mortality indicator) ---
    fig2, ax2 = plt.subplots(figsize=(12, 5))
    ax2.plot(df["week_ending_dt"], df["pct_expected_deaths"], color="#c05621", linewidth=1.3)
    ax2.axhline(100, color="gray", linestyle="--", linewidth=1, label="Expected baseline (100%)")
    ax2.set_ylabel("% of expected deaths")
    ax2.set_title("Total Deaths as % of Expected Baseline, US (Excess Mortality Indicator)")
    ax2.legend()
    ax2.grid(alpha=0.3)
    plt.tight_layout()
    fig2.savefig(OUT_DIR / "pct_expected_deaths.png", dpi=150)
    log.info(f"Saved chart: {OUT_DIR / 'pct_expected_deaths.png'}")

    df.drop(columns=["week_ending_dt"]).to_csv(OUT_DIR / "national_weekly_mortality.csv", index=False)
    log.info("Analysis complete. Outputs in data/processed/")


if __name__ == "__main__":
    main()
