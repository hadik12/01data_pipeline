"""Schema validation using pandera."""
from __future__ import annotations

from typing import Dict, Tuple

import pandera as pa
from pandera import Column, DataFrameSchema, Check


PipelineSchema = DataFrameSchema(
    {
        "order_id": Column(pa.String, nullable=False, checks=Check.str_length(min_value=1, max_value=64)),
        "customer_name": Column(
            pa.String, nullable=False, checks=Check.str_length(min_value=1, max_value=120)
        ),
        "email": Column(pa.String, nullable=True, checks=Check.str_matches(r".+@.+\..+")),
        "amount_usd": Column(pa.Float, nullable=False, checks=Check.in_range(min_value=0, max_value=100000)),
        "created_at": Column(pa.DateTime, nullable=True),
        "status": Column(pa.String, nullable=True, checks=Check.isin(["paid", "pending", "cancelled", "refunded"])),
        "country": Column(pa.String, nullable=True),
    },
    coerce=True,
)


def validate_collect(df) -> Tuple[bool, Dict]:
    """Validate dataframe and collect errors without raising."""
    try:
        PipelineSchema.validate(df, lazy=True)
        return True, {"errors_count": 0, "errors_preview_count": 0, "errors": []}
    except pa.errors.SchemaErrors as err:  # type: ignore[attr-defined]
        failure_cases = err.failure_cases
        errors_preview = failure_cases.head(50)
        return False, {
            "errors_count": int(len(failure_cases)),
            "errors_preview_count": int(len(errors_preview)),
            "errors": errors_preview.to_dict(orient="records"),
        }
