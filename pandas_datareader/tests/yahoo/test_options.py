import os
from datetime import datetime

import numpy as np
import pandas as pd

import pytest
import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError
from pandas_datareader._testing import skip_on_exception


class TestYahooOptions(object):

    @classmethod
    def setup_class(cls):
        # AAPL has monthlies
        cls.aapl = web.Options('aapl', 'yahoo')
        today = datetime.today()
        cls.year = today.year
        cls.month = today.month + 1

        if cls.month > 12:  # pragma: no cover
            cls.month = 1
            cls.year = cls.year + 1

        cls.expiry = datetime(cls.year, cls.month, 1)
        cls.dirpath = tm.get_data_path()
        cls.json1 = 'file://' + os.path.join(
            cls.dirpath, 'yahoo_options1.json')

        # see gh-22: empty table
        cls.json2 = 'file://' + os.path.join(
            cls.dirpath, 'yahoo_options2.json')
        cls.data1 = cls.aapl._process_data(cls.aapl._parse_url(cls.json1))

    @classmethod
    def teardown_class(cls):
        del cls.aapl, cls.expiry

    def assert_option_result(self, df):
        """
        Validate returned option data has expected format.
        """
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 1

        exp_columns = pd.Index(['Last', 'Bid', 'Ask', 'Chg', 'PctChg', 'Vol',
                                'Open_Int', 'IV', 'Root', 'IsNonstandard',
                                'Underlying', 'Underlying_Price', 'Quote_Time',
                                'Last_Trade_Date', 'JSON'])
        tm.assert_index_equal(df.columns, exp_columns)
        assert df.index.names == [u'Strike', u'Expiry', u'Type', u'Symbol']

        dtypes = [np.dtype(x) for x in ['float64'] * 7 +
                  ['float64', 'object', 'bool', 'object', 'float64',
                   'datetime64[ns]', 'datetime64[ns]', 'object']]
        tm.assert_series_equal(df.dtypes, pd.Series(dtypes, index=exp_columns))

    @skip_on_exception(RemoteDataError)
    def test_get_options_data(self):
        # see gh-6105: regression test
        with pytest.raises(ValueError):
            self.aapl.get_options_data(month=3)

        with pytest.raises(ValueError):
            self.aapl.get_options_data(year=1992)

        options = self.aapl.get_options_data(expiry=self.expiry)
        self.assert_option_result(options)

    @skip_on_exception(RemoteDataError)
    def test_get_near_stock_price(self):
        options = self.aapl.get_near_stock_price(call=True, put=True,
                                                 expiry=self.expiry)
        self.assert_option_result(options)

    def test_options_is_not_none(self):
        option = web.Options('aapl', 'yahoo')
        assert option is not None

    @skip_on_exception(RemoteDataError)
    def test_get_call_data(self):
        calls = self.aapl.get_call_data(expiry=self.expiry)

        self.assert_option_result(calls)
        assert calls.index.levels[2][0] == 'call'

    @skip_on_exception(RemoteDataError)
    def test_get_put_data(self):
        puts = self.aapl.get_put_data(expiry=self.expiry)

        self.assert_option_result(puts)
        assert puts.index.levels[2][1] == 'put'

    @skip_on_exception(RemoteDataError)
    def test_get_expiry_dates(self):
        dates = self.aapl._get_expiry_dates()
        assert len(dates) > 1

    @skip_on_exception(RemoteDataError)
    def test_get_all_data(self):
        data = self.aapl.get_all_data(put=True)

        assert len(data) > 1
        self.assert_option_result(data)

    @skip_on_exception(RemoteDataError)
    def test_get_data_with_list(self):
        data = self.aapl.get_call_data(expiry=self.aapl.expiry_dates)

        assert len(data) > 1
        self.assert_option_result(data)

    @skip_on_exception(RemoteDataError)
    def test_get_all_data_calls_only(self):
        data = self.aapl.get_all_data(call=True, put=False)

        assert len(data) > 1
        self.assert_option_result(data)

    @skip_on_exception(RemoteDataError)
    def test_get_underlying_price(self):
        # see gh-7
        options_object = web.Options('^spxpm', 'yahoo')
        quote_price = options_object.underlying_price

        assert isinstance(quote_price, float)

        # Tests the weekend quote time format
        price, quote_time = self.aapl.underlying_price, self.aapl.quote_time

        assert isinstance(price, (int, float, complex))
        assert isinstance(quote_time, (datetime, pd.Timestamp))

    def test_chop(self):
        # gh-7625: regression test
        self.aapl._chop_data(self.data1, above_below=2,
                             underlying_price=np.nan)
        chopped = self.aapl._chop_data(self.data1, above_below=2,
                                       underlying_price=100)

        assert isinstance(chopped, pd.DataFrame)
        assert len(chopped) > 1

        chopped2 = self.aapl._chop_data(self.data1, above_below=2,
                                        underlying_price=None)

        assert isinstance(chopped2, pd.DataFrame)
        assert len(chopped2) > 1

    def test_chop_out_of_strike_range(self):
        # gh-7625: regression test
        self.aapl._chop_data(self.data1, above_below=2,
                             underlying_price=np.nan)
        chopped = self.aapl._chop_data(self.data1, above_below=2,
                                       underlying_price=100000)

        assert isinstance(chopped, pd.DataFrame)
        assert len(chopped) > 1

    def test_sample_page_chg_float(self):
        # Tests that numeric columns with comma's are appropriately dealt with
        assert self.data1['Chg'].dtype == 'float64'

    @skip_on_exception(RemoteDataError)
    def test_month_year(self):
        # see gh-168
        data = self.aapl.get_call_data(month=self.month, year=self.year)

        assert len(data) > 1
        assert data.index.levels[0].dtype == 'float64'

        self.assert_option_result(data)

    def test_empty_table(self):
        # see gh-22
        empty = self.aapl._process_data(self.aapl._parse_url(self.json2))
        assert len(empty) == 0
