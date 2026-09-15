"""
Tests for the incremental-load watermark logic -- the core mechanism
that makes this pipeline different from a simple batch script.
"""
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src import pipeline as pl


@pytest.fixture
def isolated_env(tmp_path, monkeypatch):
    """Point the pipeline at a throwaway temp directory for each test."""
    raw_dir = tmp_path / "data" / "raw"
    processed_dir = tmp_path / "data" / "processed"
    raw_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)

    csv_path = raw_dir / "cdc_covid_pneumonia_influenza_weekly.csv"
    csv_path.write_text(
        "Week Ending Date,State,Year,MMWR Week,COVID-19 Deaths,Total Deaths,"
        "Percent of Expected Deaths,Pneumonia Deaths,Pneumonia and COVID-19 Deaths,"
        "Influenza Deaths,\"Pneumonia, Influenza, or COVID-19 Deaths\"\n"
        "01/04/2020,United States,2020,1,0,60170,98,4111,0,434,4545\n"
        "01/11/2020,United States,2020,2,1,60734,97,4153,1,475,4628\n"
    )

    monkeypatch.setattr(pl, "RAW_CSV", csv_path)
    monkeypatch.setattr(pl, "DB_PATH", processed_dir / "public_health.db")
    monkeypatch.setattr(pl, "WATERMARK_PATH", processed_dir / "_watermark.json")
    return csv_path


def test_first_run_loads_all_rows(isolated_env):
    result = pl.run()
    assert result["rows_loaded"] == 2
    assert result["total_rows"] == 2


def test_second_run_with_no_new_data_loads_nothing(isolated_env):
    pl.run()
    result = pl.run()
    assert result["status"] == "up_to_date"
    assert result["rows_loaded"] == 0


def test_new_row_appended_triggers_incremental_load(isolated_env):
    pl.run()
    with open(isolated_env, "a") as f:
        f.write("01/18/2020,United States,2020,3,2,59362,98,4066,2,468,4534\n")
    result = pl.run()
    assert result["rows_loaded"] == 1
    assert result["total_rows"] == 3


def test_full_refresh_reloads_everything(isolated_env):
    pl.run()
    with open(isolated_env, "a") as f:
        f.write("01/18/2020,United States,2020,3,2,59362,98,4066,2,468,4534\n")
    pl.run()
    result = pl.run(full_refresh=True)
    assert result["rows_loaded"] == 3
    assert result["total_rows"] == 3


def test_watermark_persists_across_runs(isolated_env):
    pl.run()
    watermark = json.loads(pl.WATERMARK_PATH.read_text())
    assert watermark["last_week_ending_date"] == "01/11/2020"


def test_no_duplicate_rows_on_rerun(isolated_env):
    pl.run()
    pl.run()
    conn = sqlite3.connect(pl.DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM weekly_mortality").fetchone()[0]
    conn.close()
    assert count == 2
