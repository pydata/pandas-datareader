import os
from datetime import datetime
import sys

import numpy as np
import pandas as pd

import nose
import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError


class TestYahooOptions(tm.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestYahooOptions, cls).setUpClass()

        # aapl has monthlies
        cls.aapl = web.Options('aapl', 'yahoo')
        today = datetime.today()
        cls.year = today.year
        cls.month = today.month + 1
        if cls.month > 12:  # pragma: no cover
            cls.month = 1
            cls.year = cls.year + 1
        cls.expiry = datetime(cls.year, cls.month, 1)
        cls.dirpath = tm.get_data_path()
        cls.json1 = 'file://' + os.path.join(cls.dirpath, 'yahoo_options1.json')
        cls.json2 = 'file://' + os.path.join(cls.dirpath, 'yahoo_options2.json')  # Empty table GH#22
        cls.data1 = cls.aapl._process_data(cls.aapl._parse_url(cls.json1))

    @classmethod
    def tearDownClass(cls):
        super(TestYahooOptions, cls).tearDownClass()
        del cls.aapl, cls.expiry

    def assert_option_result(self, df):
        """
        Validate returned option data has expected format.
        """
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertTrue(len(df) > 1)

        exp_columns = pd.Index(['Last', 'Bid', 'Ask', 'Chg', 'PctChg', 'Vol', 'Open_Int',
                                'IV', 'Root', 'IsNonstandard', 'Underlying',
                                'Underlying_Price', 'Quote_Time', 'Last_Trade_Date', 'JSON'])
        tm.assert_index_equal(df.columns, exp_columns)
        tm.assert_equal(df.index.names, [u'Strike', u'Expiry', u'Type', u'Symbol'])

        dtypes = [np.dtype(x) for x in ['float64'] * 7 +
                  ['float64', 'object', 'bool', 'object', 'float64', 'datetime64[ns]',
                   'datetime64[ns]', 'object']]
        tm.assert_series_equal(df.dtypes, pd.Series(dtypes, index=exp_columns))

    def test_get_options_data(self):
        # regression test GH6105
        with tm.assertRaises(ValueError):
            self.aapl.get_options_data(month=3)

        with tm.assertRaises(ValueError):
            self.aapl.get_options_data(year=1992)

        try:
            options = self.aapl.get_options_data(expiry=self.expiry)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)

        self.assert_option_result(options)

    def test_get_near_stock_price(self):
        try:
            options = self.aapl.get_near_stock_price(call=True, put=True,
                                                     expiry=self.expiry)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)

        self.assert_option_result(options)

    def test_options_is_not_none(self):
        option = web.Options('aapl', 'yahoo')
        self.assertTrue(option is not None)

    def test_get_call_data(self):
        try:
            calls = self.aapl.get_call_data(expiry=self.expiry)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)

        self.assert_option_result(calls)
        self.assertTrue(calls.index.levels[2][0], 'call')

    def test_get_put_data(self):
        try:
            puts = self.aapl.get_put_data(expiry=self.expiry)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)

        self.assert_option_result(puts)
        self.assertTrue(puts.index.levels[2][0], 'put')

    def test_get_expiry_dates(self):
        try:
            dates = self.aapl._get_expiry_dates()
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(dates) > 1)

    def test_get_all_data(self):
        try:
            data = self.aapl.get_all_data(put=True)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(data) > 1)

        self.assert_option_result(data)

    def test_get_data_with_list(self):
        try:
            data = self.aapl.get_call_data(expiry=self.aapl.expiry_dates)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(data) > 1)

        self.assert_option_result(data)

    def test_get_all_data_calls_only(self):
        try:
            data = self.aapl.get_all_data(call=True, put=False)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(data) > 1)

        self.assert_option_result(data)

    def test_get_underlying_price(self):
        # GH7
        try:
            options_object = web.Options('^spxpm', 'yahoo')
            quote_price = options_object.underlying_price
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(isinstance(quote_price, float))

        # Tests the weekend quote time format
        price, quote_time = self.aapl.underlying_price, self.aapl.quote_time
        self.assertTrue(isinstance(price, (int, float, complex)))
        self.assertTrue(isinstance(quote_time, (datetime, pd.Timestamp)))

    def test_chop(self):
        # regression test for #7625
        self.aapl._chop_data(self.data1, above_below=2, underlying_price=np.nan)
        chopped = self.aapl._chop_data(self.data1, above_below=2, underlying_price=100)
        self.assertTrue(isinstance(chopped, pd.DataFrame))
        self.assertTrue(len(chopped) > 1)
        chopped2 = self.aapl._chop_data(self.data1, above_below=2, underlying_price=None)
        self.assertTrue(isinstance(chopped2, pd.DataFrame))
        self.assertTrue(len(chopped2) > 1)

    def test_chop_out_of_strike_range(self):
        # regression test for #7625
        self.aapl._chop_data(self.data1, above_below=2, underlying_price=np.nan)
        chopped = self.aapl._chop_data(self.data1, above_below=2, underlying_price=100000)
        self.assertTrue(isinstance(chopped, pd.DataFrame))
        self.assertTrue(len(chopped) > 1)

    def test_sample_page_chg_float(self):
        # Tests that numeric columns with comma's are appropriately dealt with
        self.assertEqual(self.data1['Chg'].dtype, 'float64')

    def test_month_year(self):

        try:
            data = self.aapl.get_call_data(month=self.month, year=self.year)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)

        self.assertTrue(len(data) > 1)

        if sys.version_info[0] == 2 and sys.version_info[1] == 6:
            raise nose.SkipTest('skip dtype check in python 2.6')
        self.assertEqual(data.index.levels[0].dtype, 'float64')  # GH168

        self.assert_option_result(data)

    def test_empty_table(self):
        # GH22
        empty = self.aapl._process_data(self.aapl._parse_url(self.json2))
        self.assertTrue(len(empty) == 0)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)  # pragma: no cover
