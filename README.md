# 01_data_pipeline

A lightweight data pipeline that reads CSV/Excel files, cleans and normalizes fields, validates schema with pandera, writes cleaned data, and generates human-friendly and machine-readable reports. Logging is sent both to console and `logs/app.log`.

## Features
- Supports `.csv` and `.xlsx` input/output
- Cleans messy strings, emails, amounts, statuses, and dates
- Ensures required columns exist and drops rows missing critical fields
- Schema validation with detailed error preview (pandera)
- Generates Markdown and JSON reports in `reports/`
- Logs to console and file in `logs/`
- Strict mode can fail the run when validation fails

## Requirements
- Python 3.11+
- Dependencies (see `requirements.txt`):
  - pandas>=2.0
  - openpyxl>=3.1
  - pandera>=0.18
  - numpy>=1.24

Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage
Generate sample dirty data (optional):
```bash
python generate_sample.py
```

Run the pipeline (CSV to CSV example):
```bash
python pipeline.py --input data/input_dirty.csv --output data/output_clean.csv
```

Run the pipeline (Excel to Excel example):
```bash
python pipeline.py --input data/input_dirty.xlsx --output data/output_clean.xlsx
```

### Arguments
- `--input` (required): path to input `.csv` or `.xlsx`
- `--output` (required): path to output `.csv` or `.xlsx`
- `--report-dir` (default: `reports`): where to save `report.md` and `report.json`
- `--log-dir` (default: `logs`): where to save `app.log`
- `--strict` flag: if enabled, exit code is `2` when validation fails
- `--log-level` (default: `INFO`): logging verbosity

Reports are saved under the specified `report-dir` and logs under `log-dir`. Output files are written to the path you provide.

## Project structure
- `pipeline.py` — CLI entrypoint
- `src/` — cleaning, schema, reporting, IO helpers
- `generate_sample.py` — creates dirty sample CSV/XLSX for quick testing
- `reports/` and `logs/` — created automatically when running the pipeline
