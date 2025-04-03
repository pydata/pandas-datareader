import numpy as np
import pandas as pd
import pytest

from pandas_datareader import data as web

pytestmark = pytest.mark.stable


def assert_equal(x, y):
    assert np.isclose(x, y, rtol=1e-2)


class TestEcondb:
    def test_override_start_end(self):
        df = web.DataReader(
            "&".join(
                [
                    "dataset=RBI_BULLETIN",
                    "v=TIME",
                    "h=Indicator",
                    "from=2022-01-01",
                    "to=2022-07-01",
                ]
            ),
            "econdb",
            start="2020-01-01",
            end="2022-01-01",
        )
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_infer_start_end_from_symbols(self):
        df = web.DataReader(
            (
                "dataset=NAMQ_10_GDP&v=Geopolitical entity (reporting)"
                "&h=TIME&from=2010-02-01&to=2018-10-01&GEO=[AL,AT,BE,BA,"
                "BG,HR,CY,CZ,DK,EE,EA19,FI,FR,DE,EL,HU,IS,IE,IT,XK,LV,LT,"
                "LU,MT,ME,NL,MK,NO,PL,PT,RO,RS,SK,SI,ES,SE,CH,TR,UK]"
                "&NA_ITEM=[B1GQ]&S_ADJ=[SCA]&UNIT=[CLV10_MNAC]"
            ),
            "econdb",
        )
        assert df.index[0].year == 2010
        assert df.index[-1].year == 2018

    tickers = [
        f"{sec}{geo}"
        for sec in ["RGDP", "CPI", "URATE"]
        for geo in ["US", "UK", "ES", "AR"]
    ]

    @pytest.mark.parametrize("ticker", tickers)
    def test_fetch_single_ticker_series(self, ticker):
        df = web.DataReader(
            f"ticker={ticker}",
            "econdb",
            start=pd.Timestamp("2010-01-01"),
            end=pd.Timestamp("2013-01-27"),
        )
        assert df.shape[1] == 1
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_single_nonticker_series(self):
        df = web.DataReader(
            "ticker=BLS_CU.CUSR0000SA0.M.US",
            "econdb",
            start=pd.Timestamp("2010-01-01"),
            end=pd.Timestamp("2013-01-27"),
        )
        assert df.shape[1] == 1
        assert isinstance(df.index, pd.DatetimeIndex)
        assert_equal(df.loc["2010-05-01"][0], 217.3)

    def test_filtered_dataset(self):
        df = web.DataReader(
            "&".join(
                [
                    "dataset=PRC_HICP_MIDX",
                    "v=Geopolitical entity (reporting)",
                    "h=TIME",
                    "from=2022-03-01",
                    "to=2022-09-01",
                    "COICOP=[CP00]",
                    "FREQ=[M]",
                    "GEO=[ES,AT,CZ,IT,CH]",
                    "UNIT=[I15]",
                ]
            ),
            "econdb",
        )
        assert df.shape[1] == 5
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_australia_gdp(self):
        df = web.DataReader(
            "&".join(
                [
                    "dataset=ABS_GDP",
                    "4=[7]",
                    "6=[11]",
                    "16=[1267]",
                    "v=TIME",
                    "h=Indicator",
                    "from=2019-10-01",
                    "to=2022-06-01",
                    "GEO=[13]",
                ]
            ),
            "econdb",
        )
        assert_equal(df.squeeze().loc["2020-10-01"], 508603)
