"""Generate sample dirty data for testing the pipeline."""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd


COUNTRIES = ["USA", "Canada", "Germany", "France", "Spain", "Brazil"]
STATUSES = [
    "payed",
    "done",
    "completed",
    "waiting",
    "in progress",
    "pending",
    "canceled",
    "return",
    "refunded",
]


def random_date(start: datetime, end: datetime) -> str:
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=random_seconds)).strftime("%Y-%m-%d %H:%M:%S")


def maybe_invalid_date() -> str:
    if random.random() < 0.2:
        return "invalid-date"
    return random_date(datetime(2020, 1, 1), datetime(2024, 12, 31))


def maybe_invalid_email(name: str) -> str:
    domains = ["example.com", "mail.com", "test.org"]
    if random.random() < 0.25:
        return f"{name}#example.com"
    return f"{name.lower()}@{random.choice(domains)}"


def amount_variations(base: float) -> str:
    variants = [
        f"${base:,.2f}",
        f"{base:,.2f}",
        f"{base:,.2f}".replace(",", " ").replace(".", ","),
        f"{base:,.0f}",
    ]
    return random.choice(variants)


def generate_rows(n: int) -> List[dict]:
    rows = []
    for i in range(n):
        order_id = f"ORD-{random.randint(1000, 1020)}"  # duplicates likely
        name = random.choice(["Alice", "Bob", "Charlie", "Dana", "Eve", "Frank", None])
        email = maybe_invalid_email(name or "user") if random.random() > 0.1 else ""
        amount = amount_variations(random.uniform(10, 5000)) if random.random() > 0.05 else "abc"
        status = random.choice(STATUSES)
        created_at = maybe_invalid_date()
        country = random.choice(COUNTRIES)

        if random.random() < 0.15:
            order_id = None
        if random.random() < 0.15:
            amount = ""

        rows.append(
            {
                "Order ID": order_id,
                "Customer Name": name,
                "Email": email,
                "Amount (USD)": amount,
                "Created At": created_at,
                "Status": status,
                "Country": country,
            }
        )
    return rows


def main() -> None:
    random.seed(42)
    np.random.seed(42)

    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    rows = generate_rows(random.randint(20, 50))
    df = pd.DataFrame(rows)

    csv_path = data_dir / "input_dirty.csv"
    xlsx_path = data_dir / "input_dirty.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)

    print(f"Generated sample CSV: {csv_path}")
    print(f"Generated sample Excel: {xlsx_path}")


if __name__ == "__main__":
    main()
