from datetime import date

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest

import pandas_datareader.data as web
from pandas_datareader.exceptions import ImmediateDeprecationError


class TestGoogleOptions(object):

    @classmethod
    def setup_class(cls):
        cls.goog = None

    def test_deprecation(self):
        with pytest.raises(ImmediateDeprecationError):
            web.Options('GOOG', 'google')

    @pytest.mark.xfail(reason='Parsing error')
    def test_get_options_data(self):
        options = self.goog.get_options_data(expiry=self.goog.expiry_dates[0])

        assert isinstance(options, pd.DataFrame)
        assert len(options) > 10

        exp_columns = pd.Index(['Last', 'Bid', 'Ask', 'Chg',
                                'PctChg', 'Vol', 'Open_Int', 'Root',
                                'Underlying_Price', 'Quote_Time'])

        tm.assert_index_equal(options.columns, exp_columns)
        assert options.index.names == [u'Strike', u'Expiry',
                                       u'Type', u'Symbol']

        dtypes = ['float64'] * 7 + ['object', 'float64', 'datetime64[ns]']
        dtypes = [np.dtype(x) for x in dtypes]

        tm.assert_series_equal(options.dtypes, pd.Series(
            dtypes, index=exp_columns))

        for typ in options.index.levels[2]:
            assert typ in ['put', 'call']

    @pytest.mark.xfail(reason='Deprecated')
    def test_get_options_data_yearmonth(self):
        with pytest.raises(ImmediateDeprecationError):
            self.goog.get_options_data(month=1, year=2016)

    @pytest.mark.xfail(reason='Parsing error')
    def test_expiry_dates(self):
        dates = self.goog.expiry_dates

        assert len(dates) == 1
        assert isinstance(dates, list)
        assert all(isinstance(dt, date) for dt in dates)

    @pytest.mark.xfail(reason='Deprecated')
    def test_get_call_data(self):
        with pytest.raises(NotImplementedError):
            self.goog.get_call_data()

    @pytest.mark.xfail(reason='Deprecated')
    def test_get_put_data(self):
        with pytest.raises(NotImplementedError):
            self.goog.get_put_data()

    @pytest.mark.xfail(reason='Deprecated')
    def test_get_near_stock_price(self):
        with pytest.raises(NotImplementedError):
            self.goog.get_near_stock_price()

    @pytest.mark.xfail(reason='Deprecated')
    def test_get_forward_data(self):
        with pytest.raises(NotImplementedError):
            self.goog.get_forward_data([1, 2, 3])

    @pytest.mark.xfail(reason='Deprecated')
    def test_get_all_data(self):
        with pytest.raises(NotImplementedError):
            self.goog.get_all_data()

    @pytest.mark.xfail(reason='Deprecated')
    def test_get_options_data_with_year(self):
        with pytest.raises(NotImplementedError):
            self.goog.get_options_data(year=2016)
