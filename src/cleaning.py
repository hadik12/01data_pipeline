"""Data cleaning utilities for the pipeline."""
from __future__ import annotations

import re
from typing import Dict, Tuple

import numpy as np
import pandas as pd

EXPECTED_COLUMNS = [
    "order_id",
    "customer_name",
    "email",
    "amount_usd",
    "created_at",
    "status",
    "country",
]


def _normalize_column_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^0-9a-zA-Z_]+", "", name)
    return name


def _normalize_status(value: str | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    val = str(value).strip().lower()
    mapping = {
        "payed": "paid",
        "paid": "paid",
        "done": "paid",
        "completed": "paid",
        "success": "paid",
        "waiting": "pending",
        "in progress": "pending",
        "in_progress": "pending",
        "pending": "pending",
        "canceled": "cancelled",
        "cancelled": "cancelled",
        "return": "refunded",
        "refunded": "refunded",
    }
    return mapping.get(val, val if val in {"paid", "pending", "cancelled", "refunded"} else None)


def _parse_amount(value: object) -> tuple[float | None, bool]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None, True

    raw = str(value).strip()
    if not raw:
        return None, True

    sanitized = re.sub(r"[^0-9,.-]", "", raw)
    if sanitized.count(",") > 0 and sanitized.count(".") == 0:
        sanitized = sanitized.replace(",", ".")
    elif sanitized.count(",") > 0 and sanitized.count(".") > 0:
        last_comma = sanitized.rfind(",")
        last_dot = sanitized.rfind(".")
        if last_comma > last_dot:
            sanitized = sanitized.replace(".", "")
            sanitized = sanitized.replace(",", ".")
        else:
            sanitized = sanitized.replace(",", "")
    try:
        return float(sanitized), False
    except ValueError:
        return None, True


def clean_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int | list[str]]]:
    stats: Dict[str, int | list[str]] = {
        "columns_before": list(df.columns),
        "columns_after": list(EXPECTED_COLUMNS),
        "trimmed_strings": 0,
        "parsed_dates_nulls": 0,
        "invalid_emails": 0,
        "invalid_amounts": 0,
        "dropped_duplicates": 0,
        "dropped_missing_required": 0,
    }

    df = df.copy()
    df.columns = [_normalize_column_name(col) for col in df.columns]

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    string_cols = ["order_id", "customer_name", "email", "status", "country"]
    for col in string_cols:
        if col in df.columns:
            before = df[col].astype(str).where(~df[col].isna(), other="")
            stripped = before.str.strip()
            stats["trimmed_strings"] += int((before != stripped).sum())
            df[col] = stripped.replace({"": np.nan})

    df["email"] = df["email"].str.lower()
    invalid_email_mask = ~df["email"].isna() & ~df["email"].str.contains(r"@", na=False)
    stats["invalid_emails"] = int(invalid_email_mask.sum())
    df.loc[invalid_email_mask, "email"] = np.nan

    amounts = df["amount_usd"].apply(_parse_amount)
    df["amount_usd"] = amounts.apply(lambda x: x[0])
    stats["invalid_amounts"] = int(sum(flag for _, flag in amounts))

    df["status"] = df["status"].apply(_normalize_status)

    before_dates = df["created_at"].notna().sum()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    after_dates = df["created_at"].notna().sum()
    stats["parsed_dates_nulls"] = int(before_dates - after_dates)

    df["order_id"] = df["order_id"].apply(lambda x: str(x).strip() if not pd.isna(x) else x)

    df = df.sort_values(by="created_at", ascending=True, na_position="first")
    before_dupes = len(df)
    df = df.drop_duplicates(subset=["order_id"], keep="last")
    stats["dropped_duplicates"] = int(before_dupes - len(df))

    before_dropna = len(df)
    df = df.dropna(subset=["order_id", "customer_name", "amount_usd"])
    stats["dropped_missing_required"] = int(before_dropna - len(df))

    df = df[EXPECTED_COLUMNS]

    return df.reset_index(drop=True), stats
