from datetime import datetime
import requests

import numpy as np
import pandas as pd
from pandas import DataFrame

import pytest
import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader.data import YahooDailyReader
from pandas_datareader.yahoo.quotes import _yahoo_codes


class TestYahoo(object):

    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    def test_yahoo(self):
        # Asserts that yahoo is minimally working
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)

        assert web.DataReader('F', 'yahoo', start, end)['Close'][-1] == 13.68

    def test_yahoo_fails(self):
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)

        with pytest.raises(Exception):
            web.DataReader('NON EXISTENT TICKER', 'yahoo', start, end)

    def test_get_quote_series(self):
        df = web.get_quote_yahoo(pd.Series(['GOOG', 'AAPL', 'GOOG']))
        tm.assert_series_equal(df.ix[0], df.ix[2])

    def test_get_quote_string(self):
        _yahoo_codes.update({'MarketCap': 'j1'})
        df = web.get_quote_yahoo('GOOG')
        assert not pd.isnull(df['MarketCap'][0])

    def test_get_quote_stringlist(self):
        df = web.get_quote_yahoo(['GOOG', 'AAPL', 'GOOG'])
        tm.assert_series_equal(df.ix[0], df.ix[2])

    def test_get_quote_comma_name(self):
        _yahoo_codes.update({'name': 'n'})
        df = web.get_quote_yahoo(['RGLD'])
        del _yahoo_codes['name']
        assert df['name'][0] == 'Royal Gold, Inc.'

    @pytest.mark.skip('Unreliable test, receive partial '
                      'components back for dow_jones')
    def test_get_components_dow_jones(self):  # pragma: no cover
        df = web.get_components_yahoo('^DJI')  # Dow Jones
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 30

    @pytest.mark.skip('Unreliable test, receive partial '
                      'components back for dax')
    def test_get_components_dax(self):  # pragma: no cover
        df = web.get_components_yahoo('^GDAXI')  # DAX
        assert isinstance(df, pd.DataFrame)

        assert len(df) == 30
        assert df[df.name.str.contains('adidas', case=False)].index == 'ADS.DE'

    @pytest.mark.skip('Unreliable test, receive partial '
                      'components back for nasdaq_100')
    def test_get_components_nasdaq_100(self):  # pragma: no cover
        # As of 7/12/13, the conditional will
        # return false because the link is invalid

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
            tm.assert_frame_equal(df, expected)

    def test_get_data_single_symbol(self):
        # single symbol
        # http://finance.yahoo.com/q/hp?s=GOOG&a=09&b=08&c=2010&d=09&e=10&f=2010&g=d
        # just test that we succeed
        web.get_data_yahoo('GOOG')

    def test_get_data_adjust_price(self):
        goog = web.get_data_yahoo('GOOG')
        goog_adj = web.get_data_yahoo('GOOG', adjust_price=True)
        assert 'Adj Close' not in goog_adj.columns
        assert (goog['Open'] * goog_adj['Adj_Ratio']).equals(goog_adj['Open'])

    def test_get_data_interval(self):
        # daily interval data
        pan = web.get_data_yahoo('XOM', '2013-01-01',
                                 '2013-12-31', interval='d')
        assert len(pan) == 252

        # weekly interval data
        pan = web.get_data_yahoo('XOM', '2013-01-01',
                                 '2013-12-31', interval='w')
        assert len(pan) == 53

        # montly interval data
        pan = web.get_data_yahoo('XOM', '2013-01-01',
                                 '2013-12-31', interval='m')
        assert len(pan) == 12

        # dividend data
        pan = web.get_data_yahoo('XOM', '2013-01-01',
                                 '2013-12-31', interval='v')
        assert len(pan) == 4

        # test fail on invalid interval
        with pytest.raises(ValueError):
            web.get_data_yahoo('XOM', interval='NOT VALID')

    def test_get_data_multiple_symbols(self):
        # just test that we succeed
        sl = ['AAPL', 'AMZN', 'GOOG']
        web.get_data_yahoo(sl, '2012')

    def test_get_data_multiple_symbols_two_dates(self):
        pan = web.get_data_yahoo(['GE', 'MSFT', 'INTC'], 'JAN-01-12',
                                 'JAN-31-12')
        result = pan.Close.ix['01-18-12']
        assert len(result) == 3

        # sanity checking
        assert np.issubdtype(result.dtype, np.floating)

        expected = np.array([[18.99, 28.4, 25.18],
                             [18.58, 28.31, 25.13],
                             [19.03, 28.16, 25.52],
                             [18.81, 28.82, 25.87]])
        result = pan.Open.ix['Jan-15-12':'Jan-20-12']
        assert expected.shape == result.shape

    def test_get_date_ret_index(self):
        pan = web.get_data_yahoo(['GE', 'INTC', 'IBM'], '1977', '1987',
                                 ret_index=True)
        assert hasattr(pan, 'Ret_Index')

        if hasattr(pan, 'Ret_Index') and hasattr(pan.Ret_Index, 'INTC'):
            tstamp = pan.Ret_Index.INTC.first_valid_index()
            result = pan.Ret_Index.ix[tstamp]['INTC']
            assert result == 1.0

        # sanity checking
        assert np.issubdtype(pan.values.dtype, np.floating)

    def test_get_data_yahoo_actions(self):
        start = datetime(1990, 1, 1)
        end = datetime(2000, 4, 5)

        actions = web.get_data_yahoo_actions('BHP.AX', start, end)

        assert sum(actions['action'] == 'DIVIDEND') == 20
        assert sum(actions['action'] == 'SPLIT') == 1

        assert actions.ix['1995-05-11']['action'][0] == 'SPLIT'
        assert actions.ix['1995-05-11']['value'][0] == 1 / 1.1

        assert actions.ix['1993-05-10']['action'][0] == 'DIVIDEND'
        assert actions.ix['1993-05-10']['value'][0] == 0.3

    def test_get_data_yahoo_actions_invalid_symbol(self):
        start = datetime(1990, 1, 1)
        end = datetime(2000, 4, 5)

        with pytest.raises(IOError):
            web.get_data_yahoo_actions('UNKNOWN TICKER', start, end)

    def test_yahoo_reader_class(self):
        r = YahooDailyReader('GOOG')
        df = r.read()

        assert df.Volume.loc['JAN-02-2015'] == 1447500

        session = requests.Session()

        r = YahooDailyReader('GOOG', session=session)
        assert r.session is session

    def test_yahoo_DataReader(self):
        start = datetime(2010, 1, 1)
        end = datetime(2015, 5, 9)
        result = web.DataReader('AAPL', 'yahoo-actions', start, end)

        exp_idx = pd.DatetimeIndex(['2015-05-07', '2015-02-05',
                                    '2014-11-06', '2014-08-07',
                                    '2014-06-09', '2014-05-08',
                                    '2014-02-06', '2013-11-06',
                                    '2013-08-08', '2013-05-09',
                                    '2013-02-07', '2012-11-07',
                                    '2012-08-09'])
        exp = pd.DataFrame({'action': ['DIVIDEND', 'DIVIDEND', 'DIVIDEND',
                                       'DIVIDEND', 'SPLIT', 'DIVIDEND',
                                       'DIVIDEND', 'DIVIDEND',
                                       'DIVIDEND', 'DIVIDEND', 'DIVIDEND',
                                       'DIVIDEND', 'DIVIDEND'],
                            'value': [0.52, 0.47, 0.47, 0.47, 0.14285714,
                                      0.47, 0.43571, 0.43571, 0.43571,
                                      0.43571, 0.37857, 0.37857, 0.37857]},
                           index=exp_idx)
        tm.assert_frame_equal(result, exp)

    def test_yahoo_DataReader_multi(self):
        start = datetime(2010, 1, 1)
        end = datetime(2015, 5, 9)

        result = web.DataReader(['AAPL', 'F'], 'yahoo-actions', start, end)
        assert isinstance(result, pd.Panel)
