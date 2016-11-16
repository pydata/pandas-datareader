from datetime import datetime
import requests

import numpy as np
import pandas as pd
from pandas import DataFrame

import nose
import pandas.util.testing as tm
from pandas.util.testing import (assert_series_equal, assert_frame_equal)
from pandas_datareader.tests._utils import _skip_if_no_lxml

import pandas_datareader.data as web
from pandas_datareader.data import YahooDailyReader
from pandas_datareader.yahoo.quotes import _yahoo_codes


class TestYahoo(tm.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestYahoo, cls).setUpClass()
        _skip_if_no_lxml()

    def test_yahoo(self):
        # asserts that yahoo is minimally working and that it throws
        # an exception when DataReader can't get a 200 response from
        # yahoo
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)

        self.assertEqual(web.DataReader("F", 'yahoo', start, end)['Close'][-1],
                         13.68)

    def test_yahoo_fails(self):
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)
        self.assertRaises(Exception, web.DataReader, "NON EXISTENT TICKER",
                          'yahoo', start, end)

    def test_get_quote_series(self):
        df = web.get_quote_yahoo(pd.Series(['GOOG', 'AAPL', 'GOOG']))
        assert_series_equal(df.ix[0], df.ix[2])

    def test_get_quote_string(self):
        _yahoo_codes.update({'MarketCap': 'j1'})
        df = web.get_quote_yahoo('GOOG')
        self.assertFalse(pd.isnull(df['MarketCap'][0]))

    def test_get_quote_stringlist(self):
        df = web.get_quote_yahoo(['GOOG', 'AAPL', 'GOOG'])
        assert_series_equal(df.ix[0], df.ix[2])

    def test_get_quote_comma_name(self):
        _yahoo_codes.update({'name': 'n'})
        df = web.get_quote_yahoo(['RGLD'])
        del _yahoo_codes['name']
        self.assertEqual(df['name'][0], 'Royal Gold, Inc.')

    def test_get_components_dow_jones(self):  # pragma: no cover
        raise nose.SkipTest('unreliable test, receive partial components back for dow_jones')

        df = web.get_components_yahoo('^DJI')  # Dow Jones
        assert isinstance(df, pd.DataFrame)
        self.assertEqual(len(df), 30)

    def test_get_components_dax(self):  # pragma: no cover
        raise nose.SkipTest('unreliable test, receive partial components back for dax')

        df = web.get_components_yahoo('^GDAXI')  # DAX
        assert isinstance(df, pd.DataFrame)
        self.assertEqual(len(df), 30)
        self.assertEqual(df[df.name.str.contains('adidas', case=False)].index,
                         'ADS.DE')

    def test_get_components_nasdaq_100(self):  # pragma: no cover
        # as of 7/12/13 the conditional will test false because the link is invalid
        raise nose.SkipTest('unreliable test, receive partial components back for nasdaq_100')

        df = web.get_components_yahoo('^NDX')  # NASDAQ-100
        assert isinstance(df, pd.DataFrame)

        if len(df) > 1:
            # Usual culprits, should be around for a while
            assert 'AAPL' in df.index
            assert 'GOOG' in df.index
            assert 'AMZN' in df.index
        else:
            expected = DataFrame({'exchange': 'N/A', 'name': '@^NDX'},
                                 index=['@^NDX'])
            assert_frame_equal(df, expected)

    def test_get_data_single_symbol(self):
        # single symbol
        # http://finance.yahoo.com/q/hp?s=GOOG&a=09&b=08&c=2010&d=09&e=10&f=2010&g=d
        # just test that we succeed
        web.get_data_yahoo('GOOG')

    def test_get_data_adjust_price(self):
        goog = web.get_data_yahoo('GOOG')
        goog_adj = web.get_data_yahoo('GOOG', adjust_price=True)
        self.assertTrue('Adj Close' not in goog_adj.columns)
        self.assertTrue((goog['Open'] * goog_adj['Adj_Ratio']).equals(goog_adj['Open']))

    def test_get_data_interval(self):
        # daily interval data
        pan = web.get_data_yahoo('XOM', '2013-01-01', '2013-12-31', interval='d')
        self.assertEqual(len(pan), 252)

        # weekly interval data
        pan = web.get_data_yahoo('XOM', '2013-01-01', '2013-12-31', interval='w')
        self.assertEqual(len(pan), 53)

        # montly interval data
        pan = web.get_data_yahoo('XOM', '2013-01-01', '2013-12-31', interval='m')
        self.assertEqual(len(pan), 12)

        # dividend data
        pan = web.get_data_yahoo('XOM', '2013-01-01', '2013-12-31', interval='v')
        self.assertEqual(len(pan), 4)

        # test fail on invalid interval
        self.assertRaises(ValueError, web.get_data_yahoo, 'XOM', interval='NOT VALID')

    def test_get_data_multiple_symbols(self):
        # just test that we succeed
        sl = ['AAPL', 'AMZN', 'GOOG']
        web.get_data_yahoo(sl, '2012')

    def test_get_data_multiple_symbols_two_dates(self):
        pan = web.get_data_yahoo(['GE', 'MSFT', 'INTC'], 'JAN-01-12',
                                 'JAN-31-12')
        result = pan.Close.ix['01-18-12']
        self.assertEqual(len(result), 3)

        # sanity checking
        assert np.issubdtype(result.dtype, np.floating)

        expected = np.array([[18.99, 28.4, 25.18],
                             [18.58, 28.31, 25.13],
                             [19.03, 28.16, 25.52],
                             [18.81, 28.82, 25.87]])
        result = pan.Open.ix['Jan-15-12':'Jan-20-12']
        self.assertEqual(expected.shape, result.shape)

    def test_get_date_ret_index(self):
        pan = web.get_data_yahoo(['GE', 'INTC', 'IBM'], '1977', '1987',
                                 ret_index=True)
        self.assertTrue(hasattr(pan, 'Ret_Index'))
        if hasattr(pan, 'Ret_Index') and hasattr(pan.Ret_Index, 'INTC'):
            tstamp = pan.Ret_Index.INTC.first_valid_index()
            result = pan.Ret_Index.ix[tstamp]['INTC']
            self.assertEqual(result, 1.0)

        # sanity checking
        assert np.issubdtype(pan.values.dtype, np.floating)

    def test_get_data_yahoo_actions(self):
        start = datetime(1990, 1, 1)
        end = datetime(2000, 4, 5)

        actions = web.get_data_yahoo_actions('BHP.AX', start, end)

        self.assertEqual(sum(actions['action'] == 'DIVIDEND'), 20)
        self.assertEqual(sum(actions['action'] == 'SPLIT'), 1)

        self.assertEqual(actions.ix['1995-05-11']['action'][0], 'SPLIT')
        self.assertEqual(actions.ix['1995-05-11']['value'][0], 1 / 1.1)

        self.assertEqual(actions.ix['1993-05-10']['action'][0], 'DIVIDEND')
        self.assertEqual(actions.ix['1993-05-10']['value'][0], 0.3)

    def test_get_data_yahoo_actions_invalid_symbol(self):
        start = datetime(1990, 1, 1)
        end = datetime(2000, 4, 5)

        self.assertRaises(IOError, web.get_data_yahoo_actions, 'UNKNOWN TICKER', start, end)

    def test_yahoo_reader_class(self):
        r = YahooDailyReader('GOOG')
        df = r.read()
        self.assertEqual(df.Volume.loc['JAN-02-2015'], 1447500)

        session = requests.Session()
        r = YahooDailyReader('GOOG', session=session)
        self.assertTrue(r.session is session)

    def test_yahoo_DataReader(self):
        start = datetime(2010, 1, 1)
        end = datetime(2015, 5, 9)
        result = web.DataReader('AAPL', 'yahoo-actions', start, end)

        exp_idx = pd.DatetimeIndex(['2015-05-07', '2015-02-05', '2014-11-06', '2014-08-07',
                                    '2014-06-09', '2014-05-08', '2014-02-06', '2013-11-06',
                                    '2013-08-08', '2013-05-09', '2013-02-07', '2012-11-07',
                                    '2012-08-09'])
        exp = pd.DataFrame({'action': ['DIVIDEND', 'DIVIDEND', 'DIVIDEND', 'DIVIDEND',
                                       'SPLIT', 'DIVIDEND', 'DIVIDEND', 'DIVIDEND',
                                       'DIVIDEND', 'DIVIDEND', 'DIVIDEND', 'DIVIDEND', 'DIVIDEND'],
                            'value': [0.52, 0.47, 0.47, 0.47, 0.14285714, 0.47, 0.43571, 0.43571,
                                      0.43571, 0.43571, 0.37857, 0.37857, 0.37857]}, index=exp_idx)
        tm.assert_frame_equal(result, exp)

    def test_yahoo_DataReader_multi(self):
        start = datetime(2010, 1, 1)
        end = datetime(2015, 5, 9)
        result = web.DataReader(['AAPL', 'F'], 'yahoo-actions', start, end)
        assert isinstance(result, pd.Panel)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)  # pragma: no cover
