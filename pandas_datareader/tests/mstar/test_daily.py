from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import requests

import pandas_datareader.data as web
from pandas_datareader._testing import skip_on_exception
from pandas_datareader._utils import RemoteDataError
from pandas_datareader.data import MorningstarDailyReader


class TestMorningstarDaily(object):

    @skip_on_exception(RemoteDataError)
    def test_invalid_date(self):
        with pytest.raises(ValueError):
            web.DataReader("MSFT", 'morningstar', start="1990-03-A")
        with pytest.raises(ValueError):
            web.DataReader("MSFT", 'morningstar', start="2001-02-02",
                           end="1999-03-03")

    def test_invalid_partial_multi_symbols(self):
        df = web.DataReader(['MSFT', "21##", ""], "morningstar", retry_count=0)
        assert (len(df.index.levels[0]) == 1)

    def test_invalid_multi_symbols(self):
        with pytest.raises(ValueError):
            web.DataReader(["#$@", "21122"], "morningstar", retry_count=0)

    def test_invalid_symbol_type(self):
        with pytest.raises(TypeError):
            web.DataReader([12332], data_source='morningstar', retry_count=0)

    @skip_on_exception(RemoteDataError)
    def test_mstar(self):
        start = datetime(2014, 3, 5)
        end = datetime(2018, 1, 18)
        df = web.DataReader('MSFT', 'morningstar', start=start, end=end)
        assert (df['Open'][-1] == 89.8)

    @skip_on_exception(RemoteDataError)
    def test_get_data_single_symbol(self):
        # single symbol
        # just test that we succeed
        web.get_data_morningstar('GOOG')

    @skip_on_exception(RemoteDataError)
    def test_get_data_interval(self):
        # daily interval data
        pan = web.get_data_morningstar(symbols='XOM', start='2013-01-01',
                                       end='2013-12-31', interval='d')
        assert len(pan) == 261

        # weekly interval data
        pan = web.get_data_morningstar(symbols='XOM', start='2013-01-01',
                                       end='2013-12-31', interval='w')
        assert len(pan) == 54

        # monthly interval data
        pan = web.get_data_morningstar(symbols='XOM', start='2013-01-01',
                                       end='2013-12-31', interval='m')
        assert len(pan) == 13

        # test fail on invalid interval
        with pytest.raises(ValueError):
            web.get_data_morningstar('XOM', interval='NOT VALID')

    @skip_on_exception(RemoteDataError)
    def test_get_data_multiple_symbols(self):
        # just test that we succeed
        sl = ['AAPL', 'AMZN', 'GOOG']
        web.get_data_morningstar(sl, '2012')

    @skip_on_exception(RemoteDataError)
    def test_get_data_multiple_symbols_two_dates(self):
        df = web.get_data_morningstar(symbols=['XOM', 'MSFT'],
                                      start='2013-01-01',
                                      end='2013-03-04')

        assert len(df.index.levels[0]) == 2
        assert 'XOM' in df.index.levels[0]
        assert 'MSFT' in df.index.levels[0]

        # sanity checking
        assert df.dtypes['Close'] == np.float64
        assert df.dtypes['Open'] == np.float64
        assert df.dtypes['Low'] == np.float64
        assert df.dtypes['High'] == np.float64
        assert df.dtypes['Volume'] == np.int64

    def incl_dividend_column_multi(self):
        df = web.get_data_morningstar(symbols=['XOM', 'MSFT'],
                                      start='2013-01-01',
                                      end='2013-03-04', incl_dividends=True)

        assert ("isDividend" in df)

    def incl_splits_column_multi(self):
        df = web.get_data_morningstar(symbols=['XOM', 'MSFT'],
                                      start='2013-01-01',
                                      end='2013-03-04', incl_dividends=True)
        assert ("isSplit" in df)

    def excl_volume_column_multi(self):
        df = web.get_data_morningstar(symbols=["XOM", "MSFT"],
                                      start='2013-01-01',
                                      end='2013-03-04', incl_volume=False)
        assert ("Volume" not in df.keys())

    @skip_on_exception(RemoteDataError)
    def test_mstar_reader_class(self):
        dr = MorningstarDailyReader(symbols="GOOG", interval="d")
        df = dr.read()

        assert df.Close['GOOG']['2017-12-13'] == 1040.61

        session = requests.Session()

        dr = MorningstarDailyReader('GOOG', session=session)
        dr.read()
        assert dr.session is session

    @skip_on_exception(RemoteDataError)
    def test_mstar_DataReader_multi(self):
        start = datetime(2010, 1, 1)
        end = datetime(2015, 5, 9)

        result = web.DataReader(['AAPL', 'F'], 'morningstar', start, end)
        assert isinstance(result, pd.DataFrame)
