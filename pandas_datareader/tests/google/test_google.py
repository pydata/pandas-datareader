import pytest

import numpy as np
import pandas as pd
import pandas_datareader.data as web

from pandas import compat
from datetime import datetime
from pandas_datareader.data import GoogleDailyReader
from pandas_datareader._utils import RemoteDataError, SymbolWarning

import requests

import warnings
import pandas.util.testing as tm
from pandas.util.testing import assert_series_equal


def assert_n_failed_equals_n_null_columns(wngs, obj, cls=SymbolWarning):
    all_nan_cols_dict = {}

    for k, v in compat.iteritems(obj):
        is_null = pd.isnull(v)

        if hasattr(is_null, 'all'):
            is_null = is_null.all()

        all_nan_cols_dict[k] = is_null

    all_nan_cols = pd.Series(all_nan_cols_dict)

    n_all_nan_cols = all_nan_cols.sum()
    valid_warnings = pd.Series([wng for wng in wngs if wng.category == cls])

    assert len(valid_warnings) == n_all_nan_cols

    failed_symbols = all_nan_cols[all_nan_cols].index
    msgs = valid_warnings.map(lambda x: x.message)

    assert msgs.str.contains('|'.join(failed_symbols)).all()


class TestGoogle(object):

    @classmethod
    def setup_class(cls):
        cls.locales = tm.get_locales(prefix='en_US')
        if not cls.locales:  # pragma: no cover
            pytest.skip("US English locale not available for testing")

    @classmethod
    def teardown_class(cls):
        del cls.locales

    def test_google(self):
        # asserts that google is minimally working and that it throws
        # an exception when DataReader can't get a 200 response from
        # google
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)

        for locale in self.locales:
            with tm.set_locale(locale):
                panel = web.DataReader("NYSE:F", 'google', start, end)
            assert panel.Close[-1] == 13.68

        with pytest.raises(Exception):
            web.DataReader('NON EXISTENT TICKER', 'google', start, end)

    def assert_option_result(self, df):
        """
        Validate returned quote data has expected format.
        """
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

        exp_columns = pd.Index(['change_pct', 'last', 'time'])
        tm.assert_index_equal(df.columns, exp_columns)

        dtypes = [np.dtype(x) for x in ['float64', 'float64',
                                        'datetime64[ns]']]
        tm.assert_series_equal(df.dtypes, pd.Series(dtypes, index=exp_columns))

    @pytest.mark.xfail(reason="Google quote api is offline as of Oct 1, 2017")
    def test_get_quote_string(self):
        df = web.get_quote_google('GOOG')
        assert df.loc['GOOG', 'last'] > 0.0
        tm.assert_index_equal(df.index, pd.Index(['GOOG']))
        self.assert_option_result(df)

    @pytest.mark.xfail(reason="Google quote api is offline as of Oct 1, 2017")
    def test_get_quote_stringlist(self):
        df = web.get_quote_google(['GOOG', 'AMZN', 'GOOG'])
        assert_series_equal(df.iloc[0], df.iloc[2])
        tm.assert_index_equal(df.index, pd.Index(['GOOG', 'AMZN', 'GOOG']))
        self.assert_option_result(df)

    def test_get_goog_volume(self):
        for locale in self.locales:
            with tm.set_locale(locale):
                df = web.get_data_google('GOOG').sort_index()
            assert df.Volume.loc['JAN-02-2015'] == 1446662

    def test_get_multi1(self):
        for locale in self.locales:
            sl = ['AAPL', 'AMZN', 'GOOG']
            with tm.set_locale(locale):
                pan = web.get_data_google(sl, '2012')
            ts = pan.Close.GOOG.index[pan.Close.AAPL < pan.Close.GOOG]
            if (hasattr(pan, 'Close') and hasattr(pan.Close, 'GOOG') and
                    hasattr(pan.Close, 'AAPL')):
                assert ts[0].dayofyear == 3
            else:  # pragma: no cover
                with pytest.raises(AttributeError):
                    pan.Close()

    def test_get_multi_invalid(self):
        with warnings.catch_warnings(record=True):
            sl = ['AAPL', 'AMZN', 'INVALID']
            pan = web.get_data_google(sl, '2012')
            assert 'INVALID' in pan.minor_axis

    def test_get_multi_all_invalid(self):
        with warnings.catch_warnings(record=True):
            sl = ['INVALID', 'INVALID2', 'INVALID3']
            with pytest.raises(RemoteDataError):
                web.get_data_google(sl, '2012')

    def test_get_multi2(self):
        with warnings.catch_warnings(record=True) as w:
            for locale in self.locales:
                with tm.set_locale(locale):
                    pan = web.get_data_google(['GE', 'MSFT', 'INTC'],
                                              'JAN-01-12', 'JAN-31-12')
                result = pan.Close.loc['01-18-12']
                assert_n_failed_equals_n_null_columns(w, result)

                # sanity checking

                assert np.issubdtype(result.dtype, np.floating)
                result = pan.Open.loc['Jan-15-12':'Jan-20-12']

                assert result.shape == (4, 3)
                assert_n_failed_equals_n_null_columns(w, result)

    def test_dtypes(self):
        # see gh-3995, gh-8980
        data = web.get_data_google(
                'NYSE:F',
                start='JAN-01-10',
                end='JAN-27-13')
        assert np.issubdtype(data.Open.dtype, np.number)
        assert np.issubdtype(data.Close.dtype, np.number)
        assert np.issubdtype(data.Low.dtype, np.number)
        assert np.issubdtype(data.High.dtype, np.number)
        assert np.issubdtype(data.Volume.dtype, np.number)

    def test_unicode_date(self):
        # see gh-8967
        data = web.get_data_google(
                'NYSE:F',
                start='JAN-01-10',
                end='JAN-27-13')
        assert data.index.name == 'Date'

    def test_google_reader_class(self):
        r = GoogleDailyReader('GOOG')
        df = r.read()
        assert df.Volume.loc['JAN-02-2015'] == 1446662

        session = requests.Session()
        r = GoogleDailyReader('GOOG', session=session)
        assert r.session is session

    def test_bad_retry_count(self):

        with pytest.raises(ValueError):
            web.get_data_google('NYSE:F', retry_count=-1)
