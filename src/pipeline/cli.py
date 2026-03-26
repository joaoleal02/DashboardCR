from __future__ import annotations

import argparse
import logging
import sys

from src.logging_utils import configure_logging
from src.pipeline.runner import PipelineRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the equity research pipeline for one or more B3 tickers.")
    parser.add_argument(
        "--ticker",
        dest="tickers",
        action="append",
        required=True,
        help="Ticker to run. Repeat the flag to process multiple tickers.",
    )
    parser.add_argument(
        "--trigger",
        default="manual_cli",
        help="Execution trigger stored in the pipeline_runs table.",
    )
    return parser.parse_args()


def main() -> int:
    configure_logging()
    logger = logging.getLogger(__name__)
    args = parse_args()
    runner = PipelineRunner()
    exit_code = 0

    for ticker in args.tickers:
        result = runner.run(ticker=ticker, trigger=args.trigger)
        logger.info(
            "Run %s completed for %s with status=%s llm_status=%s",
            result.run_id,
            result.ticker,
            result.run_status,
            result.llm_status,
        )
        if result.run_status.startswith("failed"):
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
