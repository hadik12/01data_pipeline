"""Reporting utilities for the data pipeline."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd


def build_report(
    *,
    input_path: str,
    output_path: str,
    rows_in: int,
    rows_out: int,
    rows_removed: int,
    cleaning_stats: Dict[str, Any],
    validation_ok: bool,
    validation_summary: Dict[str, Any],
    columns_out: list[str],
) -> Dict[str, Any]:
    return {
        "meta": {
            "created_at": datetime.utcnow().isoformat() + "Z",
            "input_path": input_path,
            "output_path": output_path,
        },
        "rows": {
            "in": rows_in,
            "out": rows_out,
            "removed": rows_removed,
        },
        "columns_out": columns_out,
        "cleaning_stats": cleaning_stats,
        "validation": {
            "ok": validation_ok,
            "summary": validation_summary,
        },
    }


def _format_errors_table(errors: list[Dict[str, Any]]) -> str:
    if not errors:
        return "No validation errors."
    df = pd.DataFrame(errors)
    headers = " | ".join(df.columns)
    separator = " | ".join(["---"] * len(df.columns))
    lines = [f"| {headers} |", f"| {separator} |"]
    for _, row in df.iterrows():
        values = " | ".join(str(v) for v in row.values)
        lines.append(f"| {values} |")
    return "\n".join(lines)


def write_report_files(report_dir: str | Path, report: Dict[str, Any]) -> None:
    path = Path(report_dir)
    path.mkdir(parents=True, exist_ok=True)

    json_path = path / "report.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    md_path = path / "report.md"
    validation = report.get("validation", {})
    errors_table = _format_errors_table(validation.get("summary", {}).get("errors", []))
    md_lines = [
        "# Data Pipeline Report",
        "",
        "## Meta",
        f"- Created at: {report['meta']['created_at']}",
        f"- Input: {report['meta']['input_path']}",
        f"- Output: {report['meta']['output_path']}",
        "",
        "## Rows",
        f"- Input rows: {report['rows']['in']}",
        f"- Output rows: {report['rows']['out']}",
        f"- Removed rows: {report['rows']['removed']}",
        "",
        "## Columns",
        f"- Output columns: {', '.join(report['columns_out'])}",
        "",
        "## Cleaning stats",
    ]

    for key, value in report["cleaning_stats"].items():
        md_lines.append(f"- {key}: {value}")

    md_lines.extend(
        [
            "",
            "## Validation",
            f"- OK: {validation.get('ok')}",
            f"- Errors count: {validation.get('summary', {}).get('errors_count')}",
            f"- Errors preview count: {validation.get('summary', {}).get('errors_preview_count')}",
            "",
            "### Errors preview",
            errors_table,
        ]
    )

    with md_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
