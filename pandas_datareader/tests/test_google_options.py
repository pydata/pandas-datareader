import nose

from datetime import date

import numpy as np
import pandas as pd
import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError


class TestGoogleOptions(tm.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestGoogleOptions, cls).setUpClass()

        # goog has monthlies
        cls.goog = web.Options('GOOG', 'google')

    def assert_option_result(self, df):
        """
        Validate returned option data has expected format.
        """
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertTrue(len(df) > 1)

        exp_columns = pd.Index(['Last', 'Bid', 'Ask', 'Chg', 'PctChg', 'Vol', 'Open_Int',
                                'Root', 'Underlying_Price', 'Quote_Time'])
        tm.assert_index_equal(df.columns, exp_columns)
        tm.assert_equal(df.index.names, [u'Strike', u'Expiry', u'Type', u'Symbol'])

        dtypes = ['float64'] * 7 + ['object', 'float64', 'datetime64[ns]']
        dtypes = [np.dtype(x) for x in dtypes]
        tm.assert_series_equal(df.dtypes, pd.Series(dtypes, index=exp_columns))

    def test_get_options_data(self):
        try:
            options = self.goog.get_options_data(expiry=self.goog.expiry_dates[0])
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(options) > 10)

        self.assert_option_result(options)

        for typ in options.index.levels[2]:
            self.assertTrue(typ in ['put', 'call'])

    def test_get_options_data_yearmonth(self):
        with tm.assertRaises(NotImplementedError):
            self.goog.get_options_data(month=1, year=2016)

    def test_expiry_dates(self):
        try:
            dates = self.goog.expiry_dates
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)

        self.assertTrue(len(dates) >= 4)
        self.assertIsInstance(dates, list)
        self.assertTrue(all(isinstance(dt, date) for dt in dates))

    def test_get_call_data(self):
        with tm.assertRaises(NotImplementedError):
            self.goog.get_call_data()

    def test_get_put_data(self):
        with tm.assertRaises(NotImplementedError):
            self.goog.get_put_data()

    def test_get_near_stock_price(self):
        with tm.assertRaises(NotImplementedError):
            self.goog.get_near_stock_price()

    def test_get_forward_data(self):
        with tm.assertRaises(NotImplementedError):
            self.goog.get_forward_data([1, 2, 3])

    def test_get_all_data(self):
        with tm.assertRaises(NotImplementedError):
            self.goog.get_all_data()

    def test_get_options_data_with_year(self):
        with tm.assertRaises(NotImplementedError):
            self.goog.get_options_data(year=2016)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)  # pragma: no cover
