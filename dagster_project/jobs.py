"""
Jobs: named, runnable groupings of assets.

Right now we only have one asset, so this job just runs that. Once Gold
assets exist, this is where you'd define e.g. a job that runs Silver THEN
Gold together, in dependency order.
"""

from dagster import define_asset_job

silver_job = define_asset_job(name="silver_job", selection=["cleaned_trades"])