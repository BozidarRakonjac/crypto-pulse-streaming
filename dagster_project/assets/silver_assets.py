"""
Dagster asset: cleaned_trades

This is intentionally a thin wrapper. All the real logic (reading,
filtering, deduping, inserting) lives in pipelines/silver_cleaned_trades.py.
Dagster's job is just to CALL that logic on a schedule and track that it ran.
"""

from dagster import asset, AssetKey, MaterializeResult, MetadataValue
from pipelines.silver_cleaned_trades import run_silver_cleaning


@asset(
    # Declares that cleaned_trades depends on raw_trades, purely for lineage/
    # documentation in the Dagster UI. Dagster does NOT run/produce raw_trades
    # itself -- that table is continuously filled by the Kafka consumer, a
    # separate always-on process outside Dagster's control entirely.
    deps=[AssetKey("raw_trades")],
)
def cleaned_trades() -> MaterializeResult:
    """Cleans raw_trades (Bronze) and loads new rows into cleaned_trades (Silver)."""
    summary = run_silver_cleaning()

    # MetadataValue lets these numbers show up in the Dagster UI for each run --
    # genuinely useful for spotting a run where, say, rows_inserted suddenly
    # drops to 0 for a suspicious reason (pipeline stalled, producer down, etc).
    return MaterializeResult(
        metadata={
            "rows_read": MetadataValue.int(summary["rows_read"]),
            "rows_after_filters": MetadataValue.int(summary["rows_after_filters"]),
            "duplicates_dropped": MetadataValue.int(summary["duplicates_dropped"]),
            "rows_inserted": MetadataValue.int(summary["rows_inserted"]),
        }
    )