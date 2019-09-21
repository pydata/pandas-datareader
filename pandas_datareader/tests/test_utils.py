import datetime as dt
import pandas as pd
import pytest

from pandas_datareader._utils import _sanitize_dates


class TestUtils(object):
    @pytest.mark.parametrize(
        "input_date",
        [
            "2019-01-01",
            "JAN-01-2010",
            dt.datetime(2019, 1, 1),
            dt.date(2019, 1, 1),
            pd.Timestamp(2019, 1, 1),
        ],
    )
    def test_sanitize_dates(self, input_date):
        expected_start = pd.to_datetime(input_date)
        expected_end = pd.to_datetime(dt.date.today())
        result = _sanitize_dates(input_date, None)
        assert result == (expected_start, expected_end)

    def test_sanitize_dates_int(self):
        start_int = 2018
        end_int = 2019
        expected_start = pd.to_datetime(dt.datetime(start_int, 1, 1))
        expected_end = pd.to_datetime(dt.datetime(end_int, 1, 1))
        assert _sanitize_dates(start_int, end_int) == (expected_start, expected_end)

    def test_sanitize_invalid_dates(self):
        with pytest.raises(ValueError):
            _sanitize_dates(2019, 2018)

        with pytest.raises(ValueError):
            _sanitize_dates("2019-01-01", "2018-01-01")

        with pytest.raises(ValueError):
            _sanitize_dates("20199", None)

    def test_sanitize_dates_defaults(self):
        default_start = pd.to_datetime(dt.date.today() - dt.timedelta(days=365 * 5))
        default_end = pd.to_datetime(dt.date.today())
        assert _sanitize_dates(None, None) == (default_start, default_end)
