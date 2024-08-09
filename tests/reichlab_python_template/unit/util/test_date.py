"""Unit tests for the date module."""

from freezegun import freeze_time
from standardize_repo_settings.util.date import get_current_date


@freeze_time("2024-01-02")
def test_current_date():
    cd = get_current_date()
    assert cd == "January 02, 2024"
