"""CLI entrypoint for the data pipeline."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src.cleaning import clean_dataframe
from src.io_utils import read_table, write_table
from src.reporting import build_report, write_report_files
from src.schema import validate_collect


logger = logging.getLogger(__name__)


def setup_logging(log_dir: Path, level: str) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "app.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level.upper())

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level.upper())

    root_logger.handlers = []
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return log_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Data cleaning and validation pipeline")
    parser.add_argument("--input", required=True, help="Input CSV or XLSX file")
    parser.add_argument("--output", required=True, help="Output CSV or XLSX file")
    parser.add_argument("--report-dir", default="reports", help="Directory to store reports")
    parser.add_argument("--log-dir", default="logs", help="Directory to store logs")
    parser.add_argument("--strict", action="store_true", help="Exit with code 2 on validation errors")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    log_dir = Path(args.log_dir)
    report_dir = Path(args.report_dir)
    log_path = setup_logging(log_dir, args.log_level)
    logger.info("Logging initialized at %s", log_path)

    input_path = Path(args.input)
    output_path = Path(args.output)

    try:
        df_raw = read_table(input_path)
        logger.info("Loaded input file with %d rows and %d columns", df_raw.shape[0], df_raw.shape[1])
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to read input file: %s", exc)
        return 1

    df_clean, cleaning_stats = clean_dataframe(df_raw)
    logger.info("Cleaning finished: %s", cleaning_stats)

    validation_ok, validation_summary = validate_collect(df_clean)
    if validation_ok:
        logger.info("Validation passed")
    else:
        logger.warning("Validation failed with %d errors", validation_summary.get("errors_count", 0))

    rows_removed = df_raw.shape[0] - df_clean.shape[0]
    report = build_report(
        input_path=str(input_path),
        output_path=str(output_path),
        rows_in=df_raw.shape[0],
        rows_out=df_clean.shape[0],
        rows_removed=rows_removed,
        cleaning_stats=cleaning_stats,
        validation_ok=validation_ok,
        validation_summary=validation_summary,
        columns_out=list(df_clean.columns),
    )

    write_report_files(report_dir, report)
    logger.info("Report written to %s", report_dir)

    try:
        write_table(df_clean, output_path)
        logger.info("Output written to %s", output_path)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to write output: %s", exc)
        return 1

    if not validation_ok and args.strict:
        logger.error("Strict mode enabled and validation failed. Exiting with code 2")
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
