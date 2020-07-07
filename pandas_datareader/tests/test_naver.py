from datetime import datetime

import pytest

from pandas_datareader import DataReader


class TestNaver(object):
    @pytest.mark.parametrize(
        "symbol, start, end",
        [
            ("005930", (2019, 10, 1), (2019, 10, 7)),
            ("000660", (2018, 1, 1), (2018, 12, 31)),
            ("069500", (2017, 6, 3), (2018, 9, 9)),
        ],
    )
    def test_naver_daily_reader(self, symbol, start, end):
        start = datetime(*start)
        end = datetime(*end)
        reader = DataReader(symbol, "naver", start, end)

        assert reader.shape[1] == 5
        assert reader.index.min() >= start
        assert reader.index.max() <= end

    def test_bulk_fetch(self):
        with pytest.raises(NotImplementedError):
            DataReader(["005930", "000660"])
