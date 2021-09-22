from datetime import datetime

import pandas as pd
import pytest

from pandas_datareader import data as web

pytestmark = pytest.mark.stable


class TestTSE(object):
    @property
    def start(self):
        return datetime(2021, 3, 1)

    @property
    def end(self):
        return datetime(2021, 9, 15)

    def test_tse(self):
        df = web.DataReader("نوری", "tse", self.start, self.end)
        assert df.index.name == "Date"
        assert df.index[0] == pd.to_datetime(self.start)
        assert df.index[-1] == pd.to_datetime(self.end)
        assert len(df) == 123

    def test_tse_int_symbol(self):
        df = web.DataReader("19040514831923530", "tse", self.start, self.end)
        assert df.index.name == "Date"
        assert df.index[0] == pd.to_datetime(self.start)
        assert df.index[-1] == pd.to_datetime(self.end)
        assert len(df) == 123

    def test_tse_multi(self):
        names = ["خصدرا", "زاگرس"]
        df = web.DataReader(names, "tse", self.start, self.end)
        assert df.index.name == "Date"
        assert df.index[0] == pd.to_datetime(self.start)
        assert df.index[-1] == pd.to_datetime(self.end)
        assert list(df.columns.get_level_values(1)[0 : len(names)]) == names
        assert len(df) == 126

    def test_tse_multi_bad_series(self):
        names = ["NOTAREALSERIES", "نوری", "ALSOFAKE"]
        with pytest.raises(Exception):
            web.DataReader(names, data_source="tse")

    def test_tse_raises_exception(self):
        with pytest.raises(Exception):
            web.DataReader("NON EXISTENT SERIES", "tse", self.start, self.end)

    def test_tse_helper(self):
        df = web.get_data_tse("نوری", self.start, self.end)
        assert df.index.name == "Date"
        assert df.index[0] == pd.to_datetime(self.start)
        assert df.index[-1] == pd.to_datetime(self.end)
        assert len(df) == 123
