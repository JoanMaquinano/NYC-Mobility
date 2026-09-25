import pytest
from preload_logic import choose_batch_month, validate_year_month, weather_filename, weather_month


@pytest.mark.parametrize("value, expected", [("2026-01", (2026, 1)), ("2030-12", (2030, 12))])
def test_validate_year_month(value: str, expected: tuple[int, int]) -> None:
    assert validate_year_month(value) == expected


@pytest.mark.parametrize("value", ["", "2026", "1999-12", "2026-00", "2026-13", "2026-1"])
def test_validate_year_month_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError, match="YYYY-MM"):
        validate_year_month(value)


def test_weather_filename_and_month_round_trip() -> None:
    filename = weather_filename("2026-03")

    assert filename == "weather_march_2026.csv"
    assert weather_month(filename) == "2026-03"


@pytest.mark.parametrize("filename", ["weather_march_2026.txt", "weather_fake_2026.csv", "march_2026.csv"])
def test_weather_month_rejects_unrecognized_files(filename: str) -> None:
    assert weather_month(filename) is None


def test_choose_batch_month_prefers_requested_month() -> None:
    assert choose_batch_month("2026-05", {"2026-03"}, set()) == ("2026-05", "widget")


def test_choose_batch_month_processes_oldest_pending_month() -> None:
    assert choose_batch_month("", {"2026-05", "2026-03"}, {"2026-03"}) == (
        "2026-05",
        "next unprocessed of 1",
    )


def test_choose_batch_month_rechecks_newest_when_all_loaded() -> None:
    assert choose_batch_month("", {"2026-03", "2026-05"}, {"2026-03", "2026-05"}) == (
        "2026-05",
        "re-check (nothing outstanding)",
    )
