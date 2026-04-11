from pandas import DataFrame
import pytest

import pandas_datareader as pdr
from pandas_datareader.data import DataReader

pytestmark = pytest.mark.stable


class TestDataReader:

    def test_read_fred(self):
        vix = DataReader("VIXCLS", "fred")
        assert isinstance(vix, DataFrame)

    def test_not_implemented(self):
        with pytest.raises(NotImplementedError):
            DataReader("NA", "NA")

    @pytest.mark.parametrize(
        "source",
        [
            "yahoo",
            "iex",
            "stooq",
            "quandl",
            "moex",
            "tiingo",
            "naver",
            "av-daily",
        ],
    )
    def test_non_macro_routes_not_implemented(self, source):
        with pytest.raises(NotImplementedError):
            DataReader("AAPL", source)

    def test_top_level_exports_focus_on_macro_and_factor_sources(self):
        assert hasattr(pdr, "get_data_fred")
        assert hasattr(pdr, "get_data_famafrench")
        assert hasattr(pdr, "read_macro")
        assert not hasattr(pdr, "get_data_yahoo")
        assert not hasattr(pdr, "get_data_tiingo")
        assert not hasattr(pdr, "get_quote_yahoo")
