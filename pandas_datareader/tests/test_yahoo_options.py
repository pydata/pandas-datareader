import os
from datetime import datetime
import sys

import numpy as np
from pandas import DataFrame, Timestamp

import nose
import pandas.util.testing as tm
from pandas_datareader.tests._utils import _skip_if_no_lxml

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError


class TestYahooOptions(tm.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestYahooOptions, cls).setUpClass()
        _skip_if_no_lxml()

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
        cls.html1 = 'file://' + os.path.join(cls.dirpath, 'yahoo_options1.html')
        cls.html2 = 'file://' + os.path.join(cls.dirpath, 'yahoo_options2.html')
        cls.html3 = 'file://' + os.path.join(cls.dirpath, 'yahoo_options3.html')  # Empty table GH#22
        cls.data1 = cls.aapl._option_frames_from_url(cls.html1)['puts']

    @classmethod
    def tearDownClass(cls):
        super(TestYahooOptions, cls).tearDownClass()
        del cls.aapl, cls.expiry

    def test_get_options_data(self):
        # regression test GH6105
        self.assertRaises(ValueError, self.aapl.get_options_data, month=3)
        self.assertRaises(ValueError, self.aapl.get_options_data, year=1992)

        try:
            options = self.aapl.get_options_data(expiry=self.expiry)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(options) > 1)

    def test_get_near_stock_price(self):
        try:
            options = self.aapl.get_near_stock_price(call=True, put=True,
                                                     expiry=self.expiry)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(options) > 1)

    def test_options_is_not_none(self):
        option = web.Options('aapl', 'yahoo')
        self.assertTrue(option is not None)

    def test_get_call_data(self):
        try:
            calls = self.aapl.get_call_data(expiry=self.expiry)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(calls) > 1)

    def test_get_put_data(self):
        try:
            puts = self.aapl.get_put_data(expiry=self.expiry)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(puts) > 1)

    def test_get_expiry_dates(self):
        try:
            dates, _ = self.aapl._get_expiry_dates_and_links()
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(dates) > 1)

    def test_get_all_data(self):
        try:
            data = self.aapl.get_all_data(put=True)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(data) > 1)

    def test_get_data_with_list(self):
        try:
            data = self.aapl.get_call_data(expiry=self.aapl.expiry_dates)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(data) > 1)

    def test_get_all_data_calls_only(self):
        try:
            data = self.aapl.get_all_data(call=True, put=False)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(data) > 1)

    def test_get_underlying_price(self):
        # GH7
        try:
            options_object = web.Options('^spxpm', 'yahoo')
            url = options_object._yahoo_url_from_expiry(options_object.expiry_dates[0])
            root = options_object._parse_url(url)
            quote_price = options_object._underlying_price_from_root(root)
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(isinstance(quote_price, float))

    def test_sample_page_price_quote_time1(self):
        # Tests the weekend quote time format
        price, quote_time = self.aapl._underlying_price_and_time_from_url(self.html1)
        self.assertTrue(isinstance(price, (int, float, complex)))
        self.assertTrue(isinstance(quote_time, (datetime, Timestamp)))

    def test_chop(self):
        # regression test for #7625
        self.aapl.chop_data(self.data1, above_below=2, underlying_price=np.nan)
        chopped = self.aapl.chop_data(self.data1, above_below=2, underlying_price=100)
        self.assertTrue(isinstance(chopped, DataFrame))
        self.assertTrue(len(chopped) > 1)
        chopped2 = self.aapl.chop_data(self.data1, above_below=2, underlying_price=None)
        self.assertTrue(isinstance(chopped2, DataFrame))
        self.assertTrue(len(chopped2) > 1)

    def test_chop_out_of_strike_range(self):
        # regression test for #7625
        self.aapl.chop_data(self.data1, above_below=2, underlying_price=np.nan)
        chopped = self.aapl.chop_data(self.data1, above_below=2, underlying_price=100000)
        self.assertTrue(isinstance(chopped, DataFrame))
        self.assertTrue(len(chopped) > 1)

    def test_sample_page_price_quote_time2(self):
        # Tests the EDT page format
        # regression test for #8741
        price, quote_time = self.aapl._underlying_price_and_time_from_url(self.html2)
        self.assertTrue(isinstance(price, (int, float, complex)))
        self.assertTrue(isinstance(quote_time, (datetime, Timestamp)))

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

    def test_empty_table(self):
        # GH22
        empty = self.aapl._option_frames_from_url(self.html3)['puts']
        self.assertTrue(len(empty) == 0)

if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)  # pragma: no cover
