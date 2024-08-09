from freezegun import freeze_time
from standardize_repo_settings.app import main


@freeze_time("2019-07-13")
def test_main_date():
    output = main()
    assert "July 13, 2019" in output
