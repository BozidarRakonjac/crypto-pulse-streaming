"""
Entry point Dagster actually loads. Everything Dagster needs to know about
your project -- assets, jobs, schedules -- gets registered here.
"""

from dagster import Definitions
from dagster_project.assets.silver_assets import cleaned_trades
from dagster_project.jobs import silver_job
from dagster_project.schedules import silver_schedule

defs = Definitions(
    assets=[cleaned_trades],
    jobs=[silver_job],
    schedules=[silver_schedule],
)