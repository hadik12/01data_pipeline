"""Input/output utilities for tabular files."""
from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd


PathLike = Union[str, Path]


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_table(path: PathLike) -> pd.DataFrame:
    """Read a CSV or Excel file into a dataframe.

    Args:
        path: Path to a .csv or .xlsx file.

    Returns:
        A pandas DataFrame.
    """

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, encoding="utf-8-sig")
    if path.suffix.lower() in {".xls", ".xlsx"}:
        return pd.read_excel(path)

    raise ValueError(f"Unsupported file extension: {path.suffix}")


def write_table(df: pd.DataFrame, path: PathLike) -> None:
    """Write a dataframe to CSV or Excel.

    Args:
        df: DataFrame to write.
        path: Target path (.csv or .xlsx).
    """

    path = Path(path)
    _ensure_parent(path)

    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return
    if path.suffix.lower() in {".xls", ".xlsx"}:
        df.to_excel(path, index=False)
        return

    raise ValueError(f"Unsupported output extension: {path.suffix}")
