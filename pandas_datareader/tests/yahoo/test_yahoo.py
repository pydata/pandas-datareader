from datetime import datetime

import numpy as np
import pandas as pd
from pandas import DataFrame
import pandas.testing as tm
import pytest
import requests
from requests.exceptions import ConnectionError

from pandas_datareader._testing import skip_on_exception
from pandas_datareader._utils import RemoteDataError
import pandas_datareader.data as web
from pandas_datareader.data import YahooDailyReader

XFAIL_REASON = "Known connection failures on Yahoo when testing!"

pytestmark = pytest.mark.stable


class TestYahoo(object):
    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    @skip_on_exception(RemoteDataError)
    def test_yahoo(self):
        # Asserts that yahoo is minimally working
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 25)

        assert round(web.DataReader("F", "yahoo", start, end)["Close"][-1], 2) == 13.68

    def test_yahoo_fails(self):
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)

        with pytest.raises(Exception):
            web.DataReader("NON EXISTENT TICKER", "yahoo", start, end)

    def test_get_quote_series(self):
        stringlist = ["GOOG", "AAPL"]
        fields = ["exchange", "sharesOutstanding", "epsForward"]
        try:
            AAPL = web.get_quote_yahoo("AAPL")
            df = web.get_quote_yahoo(pd.Series(stringlist))
        except ConnectionError:
            pytest.xfail(reason=XFAIL_REASON)
        tm.assert_series_equal(AAPL.iloc[0][fields], df.loc["AAPL"][fields])
        assert sorted(stringlist) == sorted(list(df.index.values))

    def test_get_quote_string(self):
        try:
            df = web.get_quote_yahoo("GOOG")
        except ConnectionError:
            pytest.xfail(reason=XFAIL_REASON)

        assert not pd.isnull(df["marketCap"][0])

    def test_get_quote_stringlist(self):
        stringlist = ["GOOG", "AAPL"]
        try:
            df = web.get_quote_yahoo(stringlist)
        except ConnectionError:
            pytest.xfail(reason=XFAIL_REASON)
        assert sorted(stringlist) == sorted(list(df.index.values))

    def test_get_quote_comma_name(self):
        try:
            df = web.get_quote_yahoo(["RGLD"])
        except ConnectionError:
            pytest.xfail(reason=XFAIL_REASON)
        assert df["longName"][0] == "Royal Gold, Inc."

    @pytest.mark.skip(
        "Unreliable test, receive partial " "components back for dow_jones"
    )
    def test_get_components_dow_jones(self):  # pragma: no cover
        df = web.get_components_yahoo("^DJI")  # Dow Jones
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 30

    @pytest.mark.skip("Unreliable test, receive partial " "components back for dax")
    def test_get_components_dax(self):  # pragma: no cover
        df = web.get_components_yahoo("^GDAXI")  # DAX
        assert isinstance(df, pd.DataFrame)

        assert len(df) == 30
        assert df[df.name.str.contains("adidas", case=False)].index == "ADS.DE"

    @pytest.mark.skip(
        "Unreliable test, receive partial " "components back for nasdaq_100"
    )
    def test_get_components_nasdaq_100(self):  # pragma: no cover
        # As of 7/12/13, the conditional will
        # return false because the link is invalid

        df = web.get_components_yahoo("^NDX")  # NASDAQ-100
        assert isinstance(df, pd.DataFrame)

        if len(df) > 1:
            # Usual culprits, should be around for a while
            assert "AAPL" in df.index
            assert "GOOG" in df.index
            assert "AMZN" in df.index
        else:
            expected = DataFrame({"exchange": "N/A", "name": "@^NDX"}, index=["@^NDX"])
            tm.assert_frame_equal(df, expected)

    @skip_on_exception(RemoteDataError)
    def test_get_data_single_symbol(self):
        # single symbol
        # http://finance.yahoo.com/q/hp?s=GOOG&a=09&b=08&c=2010&d=09&e=10&f=2010&g=d
        # just test that we succeed
        web.get_data_yahoo("GOOG")

    @skip_on_exception(RemoteDataError)
    def test_data_with_no_actions(self):
        web.get_data_yahoo("TSLA")

    @skip_on_exception(RemoteDataError)
    def test_get_data_adjust_price(self):
        goog = web.get_data_yahoo("GOOG")
        goog_adj = web.get_data_yahoo("GOOG", adjust_price=True)
        assert "Adj Close" not in goog_adj.columns
        assert (goog["Open"] * goog_adj["Adj_Ratio"]).equals(goog_adj["Open"])

    @pytest.mark.xfail(reason="Yahoo are returning an extra day 31st Dec 2012")
    def test_get_data_interval(self):
        # daily interval data
        pan = web.get_data_yahoo("XOM", "2013-01-01", "2013-12-31", interval="d")
        assert len(pan) == 252

        # weekly interval data
        pan = web.get_data_yahoo("XOM", "2013-01-01", "2013-12-31", interval="w")
        assert len(pan) == 53

        # monthly interval data
        pan = web.get_data_yahoo("XOM", "2012-12-31", "2013-12-31", interval="m")
        assert len(pan) == 12

        # test fail on invalid interval
        with pytest.raises(ValueError):
            web.get_data_yahoo("XOM", interval="NOT VALID")

    @skip_on_exception(RemoteDataError)
    def test_get_data_multiple_symbols(self):
        # just test that we succeed
        sl = ["AAPL", "AMZN", "GOOG"]
        web.get_data_yahoo(sl, "2012")

    @pytest.mark.parametrize("adj_pr", [True, False])
    @skip_on_exception(RemoteDataError)
    def test_get_data_null_as_missing_data(self, adj_pr):
        result = web.get_data_yahoo("SRCE", "20160626", "20160705", adjust_price=adj_pr)
        # sanity checking
        floats = ["Open", "High", "Low", "Close"]
        if adj_pr:
            floats.append("Adj_Ratio")
        else:
            floats.append("Adj Close")

        assert result[floats].dtypes.all() == np.floating

    @skip_on_exception(RemoteDataError)
    def test_get_data_multiple_symbols_two_dates(self):
        data = web.get_data_yahoo(["GE", "MSFT", "INTC"], "JAN-01-12", "JAN-31-12")
        result = data.Close.loc["01-18-12"].T
        assert result.size == 3

        # sanity checking
        assert result.dtypes == np.floating

        expected = np.array(
            [
                [18.99, 28.4, 25.18],
                [18.58, 28.31, 25.13],
                [19.03, 28.16, 25.52],
                [18.81, 28.82, 25.87],
            ]
        )
        df = data.Open
        result = df[(df.index >= "Jan-15-12") & (df.index <= "Jan-20-12")]
        assert expected.shape == result.shape

    def test_get_date_ret_index(self):
        pan = web.get_data_yahoo(["GE", "INTC", "IBM"], "1977", "1987", ret_index=True)
        assert hasattr(pan, "Ret_Index")

        if hasattr(pan, "Ret_Index") and hasattr(pan.Ret_Index, "INTC"):
            tstamp = pan.Ret_Index.INTC.first_valid_index()
            result = pan.Ret_Index.loc[tstamp, "INTC"]
            assert result == 1.0

        # sanity checking
        assert np.issubdtype(pan.values.dtype, np.floating)

    def test_get_data_yahoo_actions(self):
        start = datetime(1990, 1, 1)
        end = datetime(2018, 4, 5)

        actions = web.get_data_yahoo_actions("AAPL", start, end, adjust_dividends=False)

        assert sum(actions["action"] == "DIVIDEND") == 47
        assert sum(actions["action"] == "SPLIT") == 3

        assert actions.loc["2005-02-28", "action"][0] == "SPLIT"
        assert actions.loc["2005-02-28", "value"][0] == 1 / 2.0

        assert actions.loc["1995-11-21", "action"][0] == "DIVIDEND"
        assert round(actions.loc["1995-11-21", "value"][0], 3) == 0.120

        actions = web.get_data_yahoo_actions("AAPL", start, end, adjust_dividends=True)

        assert actions.loc["1995-11-21", "action"][0] == "DIVIDEND"
        assert round(actions.loc["1995-11-21", "value"][0], 4) == 0.0043

    def test_get_data_yahoo_actions_invalid_symbol(self):
        start = datetime(1990, 1, 1)
        end = datetime(2000, 4, 5)

        with pytest.raises(IOError):
            web.get_data_yahoo_actions("UNKNOWN TICKER", start, end)

    @skip_on_exception(RemoteDataError)
    def test_yahoo_reader_class(self):
        r = YahooDailyReader("GOOG", start="JAN-01-2015")
        df = r.read()

        assert df.Volume.loc["JAN-02-2015"] == 1447500

        session = requests.Session()

        r = YahooDailyReader("GOOG", session=session)
        assert r.session is session

    def test_yahoo_DataReader(self):
        start = datetime(2010, 1, 1)
        end = datetime(2015, 5, 9)
        # yahoo will adjust for dividends by default
        result = web.DataReader("AAPL", "yahoo-actions", start, end)

        exp_idx = pd.DatetimeIndex(
            [
                "2015-05-07",
                "2015-02-05",
                "2014-11-06",
                "2014-08-07",
                "2014-06-09",
                "2014-05-08",
                "2014-02-06",
                "2013-11-06",
                "2013-08-08",
                "2013-05-09",
                "2013-02-07",
                "2012-11-07",
                "2012-08-09",
            ]
        )

        exp = pd.DataFrame(
            {
                "action": [
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "SPLIT",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                ],
                "value": [
                    0.52,
                    0.47,
                    0.47,
                    0.47,
                    0.14285714,
                    0.47,
                    0.43571,
                    0.43571,
                    0.43571,
                    0.43571,
                    0.37857,
                    0.37857,
                    0.37857,
                ],
            },
            index=exp_idx,
        )
        exp.index.name = "Date"
        tm.assert_frame_equal(result.reindex_like(exp).round(2), exp.round(2))

        # where dividends are not adjusted for splits
        result = web.get_data_yahoo_actions("AAPL", start, end, adjust_dividends=False)

        exp = pd.DataFrame(
            {
                "action": [
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "SPLIT",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                    "DIVIDEND",
                ],
                "value": [
                    0.52,
                    0.47,
                    0.47,
                    0.47,
                    0.14285714,
                    3.29,
                    3.05,
                    3.05,
                    3.05,
                    3.05,
                    2.65,
                    2.65,
                    2.65,
                ],
            },
            index=exp_idx,
        )
        exp.index.name = "Date"
        tm.assert_frame_equal(result.reindex_like(exp).round(4), exp.round(4))

        # test cases with "1/0" split ratio in actions -
        # no split, just chnage symbol from POT to NTR
        start = datetime(2017, 12, 30)
        end = datetime(2018, 12, 30)

        result = web.DataReader("NTR", "yahoo-actions", start, end)

        exp_idx = pd.DatetimeIndex(
            ["2018-12-28", "2018-09-27", "2018-06-28", "2018-03-28", "2018-01-02"]
        )

        exp = pd.DataFrame(
            {
                "action": ["DIVIDEND", "DIVIDEND", "DIVIDEND", "DIVIDEND", "SPLIT"],
                "value": [0.43, 0.40, 0.40, 0.40, 1.00],
            },
            index=exp_idx,
        )
        exp.index.name = "Date"
        tm.assert_frame_equal(result.reindex_like(exp).round(2), exp.round(2))

    @skip_on_exception(RemoteDataError)
    def test_yahoo_DataReader_multi(self):
        start = datetime(2010, 1, 1)
        end = datetime(2015, 5, 9)
        result = web.DataReader(["AAPL", "F"], "yahoo-actions", start, end)
        assert isinstance(result, dict)
