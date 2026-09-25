"""Pure decision logic shared by the Databricks preload task and its tests."""

from __future__ import annotations

import re

MONTH_NAMES = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)


def validate_year_month(value: str) -> tuple[int, int]:
    """Return year and month for a valid YYYY-MM value."""
    if re.fullmatch(r"\d{4}-\d{2}", value or "") is None:
        raise ValueError("year_month must be YYYY-MM")
    year, month = (int(part) for part in value.split("-"))
    if year < 2000 or not 1 <= month <= 12:
        raise ValueError("year_month must be YYYY-MM")
    return year, month


def weather_filename(year_month: str) -> str:
    """Build the weather filename expected by the DABs job."""
    year, month = validate_year_month(year_month)
    return f"weather_{MONTH_NAMES[month - 1]}_{year}.csv"


def weather_month(filename: str) -> str | None:
    """Extract a YYYY-MM value from a weather filename."""
    match = re.fullmatch(r"weather_([a-z]+)_(\d{4})\.csv", filename.lower())
    if not match or match.group(1) not in MONTH_NAMES:
        return None
    month = MONTH_NAMES.index(match.group(1)) + 1
    return f"{match.group(2)}-{month:02d}"


def choose_batch_month(
    requested: str,
    landed: set[str],
    loaded: set[str],
) -> tuple[str, str]:
    """Choose the requested month, oldest pending month, or newest recheck."""
    if requested:
        validate_year_month(requested)
        return requested, "widget"

    pending = sorted(landed - loaded)
    if pending:
        return pending[0], f"next unprocessed of {len(pending)}"
    if not landed:
        raise ValueError("No landed source months are available")
    return max(landed), "re-check (nothing outstanding)"
