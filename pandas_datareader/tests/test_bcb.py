from datetime import datetime

import pandas as pd
import pytest

from pandas_datareader import data as web
from pandas_datareader._utils import RemoteDataError

pytestmark = pytest.mark.stable


class TestBCB(object):
    def test_bcb(self):

        start = datetime(2021, 1, 4)
        end = datetime(2021, 9, 1)

        df = web.DataReader("1", "bcb", start, end)
        ts = df["1"]

        assert ts.index.name == "date"
        assert ts.index[0] == pd.to_datetime("2021-01-04")
        assert ts.index[-1] == pd.to_datetime("2021-09-01")
        assert ts.name == "1"
        assert len(ts) == 168

    def test_bcb_int_symbol(self):

        start = datetime(2021, 1, 4)
        end = datetime(2021, 9, 1)

        df = web.DataReader(1, "bcb", start, end)
        ts = df["1"]

        assert ts.index.name == "date"
        assert ts.index[0] == pd.to_datetime("2021-01-04")
        assert ts.index[-1] == pd.to_datetime("2021-09-01")
        assert ts.name == "1"
        assert len(ts) == 168

    def test_bcb_multi(self):
        names = ["433", "189"]
        start = datetime(2002, 1, 1)
        end = datetime(2021, 1, 1)

        df = web.DataReader(names, "bcb", start, end)

        assert df.index.name == "date"
        assert df.index[0] == pd.to_datetime("2002-01-01")
        assert df.index[-1] == pd.to_datetime("2021-01-01")
        assert list(df.columns) == names
        assert df.shape[0] == 229

    def test_bcb_multi_bad_series(self):
        names = ["NOTAREALSERIES", "1", "ALSOFAKE"]
        with pytest.raises(RemoteDataError):
            web.DataReader(names, data_source="bcb")

    def test_bcb_raises_exception(self):
        # Raises an exception when DataReader can't
        # get the series.

        start = datetime(2021, 1, 4)
        end = datetime(2021, 9, 1)

        with pytest.raises(RemoteDataError):
            web.DataReader("NON EXISTENT SERIES", "bcb", start, end)

    def test_bcb_helper(self):
        start = datetime(2021, 1, 4)
        end = datetime(2021, 9, 1)
        df = web.get_data_bcb("1", start, end)
        ts = df["1"]
        assert ts.index.name == "date"
        assert ts.index[0] == pd.to_datetime("2021-01-04")
        assert ts.index[-1] == pd.to_datetime("2021-09-01")
        assert ts.name == "1"
        assert len(ts) == 168
