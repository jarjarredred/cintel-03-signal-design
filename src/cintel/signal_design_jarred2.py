"""
signal_design_jarred.py - Performance Monitoring Edition

Author: Jarred Gastreich
Date: 2026-03

New Problem:
Categorize system health into "Health Zones" to make visual reporting easier.
"""

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header

# === CONFIGURE LOGGER ===
LOG: logging.Logger = get_logger("P3", level="DEBUG")

# === PATHS ===
ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

DATA_FILE: Final[Path] = DATA_DIR / "system_metrics_jarred2.csv"
OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "signals_jarred2.csv"


def main() -> None:
    log_header(LOG, "CINTEL - HEALTH MONITOR")

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------
    # STEP 1: READ CSV DATA
    # ----------------------------------------------------
    df: pl.DataFrame = pl.read_csv(DATA_FILE)
    LOG.info(f"Loaded {df.height} records")

    # ----------------------------------------------------
    # STEP 2: DESIGN SIGNALS
    # ----------------------------------------------------
    is_requests_positive: pl.Expr = pl.col("requests") > 0
    avg_lat_expr = pl.col("total_latency_ms") / pl.col("requests")

    # --- Signal A: Success Rate Percentage ---
    success_rate_recipe = (
        ((1.0 - (pl.col("errors") / pl.col("requests"))) * 100)
        .fill_nan(100.0)
        .round(2)
        .alias("success_rate_pct")
    )

    # --- Signal B: Performance Category (Visual Improvement) ---
    # Categorizing latency helps identify bottlenecks immediately.
    performance_zone_recipe = (
        pl.when(avg_lat_expr > 500)
        .then(pl.lit("CRITICAL"))
        .when(avg_lat_expr > 200)
        .then(pl.lit("WARNING"))
        .when(is_requests_positive)
        .then(pl.lit("HEALTHY"))
        .otherwise(pl.lit("INACTIVE"))
        .alias("health_zone")
    )

    # --- Signal C: Average Latency (Original) ---
    avg_latency_recipe = (
        pl.when(is_requests_positive)
        .then(avg_lat_expr)
        .otherwise(0.0)
        .alias("avg_latency_ms")
    )

    # ----------------------------------------------------
    # STEP 3: APPLY AND FILTER
    # ----------------------------------------------------
    df_with_signals = df.with_columns(
        [success_rate_recipe, performance_zone_recipe, avg_latency_recipe]
    )

    # Improved Visualization in Logs: Summary of Health Zones
    health_summary = df_with_signals.group_by("health_zone").count()
    LOG.info(f"System Health Summary:\n{health_summary}")

    # ----------------------------------------------------
    # STEP 4: SAVE THE REPORT
    # ----------------------------------------------------
    # We select the columns that tell the clearest story
    final_df = df_with_signals.select(
        ["requests", "avg_latency_ms", "success_rate_pct", "health_zone"]
    )

    final_df.write_csv(OUTPUT_FILE)
    LOG.info(f"Enhanced report saved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
