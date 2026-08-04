"""
Entry point Dagster actually loads. Everything Dagster needs to know about
your project -- assets, jobs, schedules -- gets registered here.
"""

from dagster import Definitions
from dagster_project.assets.silver_assets import cleaned_trades
from dagster_project.assets.gold_assets import ohlcv_candles
from dagster_project.jobs import pipeline_job
from dagster_project.schedules import pipeline_schedule

defs = Definitions(
    assets=[cleaned_trades, ohlcv_candles],
    jobs=[pipeline_job],
    schedules=[pipeline_schedule],
)