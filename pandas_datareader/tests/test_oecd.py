from datetime import datetime

import numpy as np
from packaging.version import Version
import pandas as pd
from pandas import testing as tm
import pytest

from pandas_datareader import data as web
from pandas_datareader._utils import RemoteDataError

PD_LT_3 = Version(pd.__version__) < Version("2.99.0")


class TestOECD:
    def test_get_un_den(self):
        df = web.DataReader(
            "TUD", "oecd", start=datetime(1960, 1, 1), end=datetime(2012, 1, 1)
        )
        japan = df["Japan"].iloc[:, 0]
        united_states = df["United States"].iloc[:, 0]

        expected_index = pd.date_range(
            "1960-01-01", "1964-01-01", freq="YS", name="Time period"
        )
        expected = pd.Series(
            [24.299999, 24.299999, 24.0, 23.4, 22.200001],
            index=expected_index,
            name=("Trade union density", "Percentage of employees"),
        )
        tm.assert_series_equal(japan.dropna().iloc[:5], expected, check_freq=False)
        if len(united_states.dropna()) > 0:
            assert united_states.first_valid_index() == pd.Timestamp("1982-01-01")
            assert united_states.last_valid_index() == pd.Timestamp("2002-01-01")
            assert int(united_states.notna().sum()) == 19
        else:
            raise AssertionError("United States data is missing in the OECD dataset.")

    @pytest.mark.skipif(PD_LT_3, reason="JSON transformation fails for pandas<3")
    def test_get_tourism(self):
        df = web.DataReader(
            "TOURISM_INBOUND",
            "oecd",
            start=datetime(2008, 1, 1),
            end=datetime(2012, 1, 1),
        )

        jp = np.array([8351000, 6790000, 8611000, 6219000, 8368000], dtype=float)
        us = np.array(
            [175702309, 160507417, 164079732, 167600277, 171320408], dtype=float
        )
        index = pd.date_range("2008-01-01", "2012-01-01", freq="YS", name="Time period")
        current_name = (
            "Not applicable",
            "Arrivals",
            "Persons",
            "All visitors (overnight and same-day)",
            "Not applicable",
            "Tourism demand surveys",
        )
        for label, values in [("Japan", jp), ("United States", us)]:
            expected = pd.Series(values, index=index, name="Tourism demand surveys")
            expected.index.freq = None
            series = df[label][current_name].loc["2008":"2012"].astype(float)
            np.testing.assert_allclose(series.to_numpy(), values, rtol=0, atol=10000)
            tm.assert_index_equal(series.index, expected.index)

    def test_oecd_invalid_symbol(self):
        with pytest.raises(RemoteDataError):
            web.DataReader("INVALID_KEY", "oecd")

        with pytest.raises(ValueError):
            web.DataReader(1234, "oecd")
