from datetime import datetime
import os

import numpy as np
import pandas as pd
import pandas.testing as tm
import pytest

import pandas_datareader.data as web


@pytest.yield_fixture
def aapl():
    aapl = web.Options("aapl", "yahoo")
    yield aapl
    aapl.close()


@pytest.fixture
def month():

    # AAPL has monthlies
    today = datetime.today()
    month = today.month + 1

    if month > 12:  # pragma: no cover
        month = 1

    return month


@pytest.fixture
def year():

    # AAPL has monthlies
    today = datetime.today()
    year = today.year
    month = today.month + 1

    if month > 12:  # pragma: no cover
        year = year + 1

    return year


@pytest.fixture
def expiry(month, year):
    return datetime(year, month, 1)


@pytest.fixture
def json1(datapath):
    dirpath = datapath("yahoo", "data")
    json1 = "file://" + os.path.join(dirpath, "yahoo_options1.json")
    return json1


@pytest.fixture
def json2(datapath):
    # see gh-22: empty table
    dirpath = datapath("yahoo", "data")
    json2 = "file://" + os.path.join(dirpath, "yahoo_options2.json")
    return json2


@pytest.fixture
def data1(aapl, json1):
    return aapl._process_data(aapl._parse_url(json1))


class TestYahooOptions(object):
    @classmethod
    def setup_class(cls):
        pytest.skip("Skip all Yahoo! tests.")

    def assert_option_result(self, df):
        """
        Validate returned option data has expected format.
        """
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 1

        exp_columns = pd.Index(
            [
                "Last",
                "Bid",
                "Ask",
                "Chg",
                "PctChg",
                "Vol",
                "Open_Int",
                "IV",
                "Root",
                "IsNonstandard",
                "Underlying",
                "Underlying_Price",
                "Quote_Time",
                "Last_Trade_Date",
                "JSON",
            ]
        )
        tm.assert_index_equal(df.columns, exp_columns)
        assert df.index.names == [u"Strike", u"Expiry", u"Type", u"Symbol"]

        dtypes = [
            np.dtype(x)
            for x in ["float64"] * 7
            + [
                "float64",
                "object",
                "bool",
                "object",
                "float64",
                "datetime64[ns]",
                "datetime64[ns]",
                "object",
            ]
        ]
        tm.assert_series_equal(df.dtypes, pd.Series(dtypes, index=exp_columns))

    def test_get_options_data(self, aapl, expiry):
        # see gh-6105: regression test
        with pytest.raises(ValueError):
            aapl.get_options_data(month=3)

        with pytest.raises(ValueError):
            aapl.get_options_data(year=1992)

        options = aapl.get_options_data(expiry=expiry)
        self.assert_option_result(options)

    def test_get_near_stock_price(self, aapl, expiry):
        options = aapl.get_near_stock_price(call=True, put=True, expiry=expiry)
        self.assert_option_result(options)

    def test_options_is_not_none(self):
        option = web.Options("aapl", "yahoo")
        assert option is not None

    def test_get_call_data(self, aapl, expiry):
        calls = aapl.get_call_data(expiry=expiry)

        self.assert_option_result(calls)
        assert calls.index.levels[2][0] == "call"

    def test_get_put_data(self, aapl, expiry):
        puts = aapl.get_put_data(expiry=expiry)

        self.assert_option_result(puts)
        assert puts.index.levels[2][1] == "put"

    def test_get_expiry_dates(self, aapl):
        dates = aapl._get_expiry_dates()
        assert len(dates) > 1

    def test_get_all_data(self, aapl):
        data = aapl.get_all_data(put=True)

        assert len(data) > 1
        self.assert_option_result(data)

    def test_get_data_with_list(self, aapl):
        data = aapl.get_call_data(expiry=aapl.expiry_dates)

        assert len(data) > 1
        self.assert_option_result(data)

    def test_get_all_data_calls_only(self, aapl):
        data = aapl.get_all_data(call=True, put=False)

        assert len(data) > 1
        self.assert_option_result(data)

    def test_get_underlying_price(self, aapl):
        # see gh-7
        options_object = web.Options("^spxpm", "yahoo")
        quote_price = options_object.underlying_price

        assert isinstance(quote_price, float)

        # Tests the weekend quote time format
        price, quote_time = aapl.underlying_price, aapl.quote_time

        assert isinstance(price, (int, float, complex))
        assert isinstance(quote_time, (datetime, pd.Timestamp))

    @pytest.mark.xfail(reason="Invalid URL scheme")
    def test_chop(self, aapl, data1):
        # gh-7625: regression test
        aapl._chop_data(data1, above_below=2, underlying_price=np.nan)
        chopped = aapl._chop_data(data1, above_below=2, underlying_price=100)

        assert isinstance(chopped, pd.DataFrame)
        assert len(chopped) > 1

        chopped2 = aapl._chop_data(data1, above_below=2, underlying_price=None)

        assert isinstance(chopped2, pd.DataFrame)
        assert len(chopped2) > 1

    @pytest.mark.xfail(reason="Invalid URL scheme")
    def test_chop_out_of_strike_range(self, aapl, data1):
        # gh-7625: regression test
        aapl._chop_data(data1, above_below=2, underlying_price=np.nan)
        chopped = aapl._chop_data(data1, above_below=2, underlying_price=100000)

        assert isinstance(chopped, pd.DataFrame)
        assert len(chopped) > 1

    @pytest.mark.xfail(reason="Invalid URL scheme")
    def test_sample_page_chg_float(self, data1):
        # Tests that numeric columns with comma's are appropriately dealt with
        assert data1["Chg"].dtype == "float64"

    def test_month_year(self, aapl, month, year):
        # see gh-168
        data = aapl.get_call_data(month=month, year=year)

        assert len(data) > 1
        assert data.index.levels[0].dtype == "float64"

        self.assert_option_result(data)

    @pytest.mark.xfail(reason="Invalid URL scheme")
    def test_empty_table(self, aapl, json2):
        # see gh-22
        empty = aapl._process_data(aapl._parse_url(json2))
        assert len(empty) == 0
