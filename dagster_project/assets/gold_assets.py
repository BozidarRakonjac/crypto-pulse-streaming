"""
Dagster asset: ohlcv_candles

Depends on cleaned_trades (an actual Dagster-managed asset, not a source
asset like raw_trades was) -- so Dagster knows to always run Silver BEFORE
Gold, in the correct order, whenever both are part of the same job.
"""

from dagster import asset, MaterializeResult, MetadataValue
from pipelines.gold_ohlcv_candles import run_gold_aggregation
from dagster_project.assets.silver_assets import cleaned_trades


@asset(deps=[cleaned_trades])
def ohlcv_candles() -> MaterializeResult:
    """Aggregates cleaned_trades into 1-minute OHLCV candles."""
    summary = run_gold_aggregation()

    return MaterializeResult(
        metadata={
            "trades_read": MetadataValue.int(summary["trades_read"]),
            "candles_written": MetadataValue.int(summary["candles_written"]),
        }
    )