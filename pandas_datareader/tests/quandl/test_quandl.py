import pytest

import pandas_datareader.data as web

from datetime import datetime

import pandas.util.testing as tm

class TestQuandl(object):

    @classmethod
    def setup_class(cls):
        cls.locales = tm.get_locales(prefix='en_US')
        if not cls.locales:  # pragma: no cover
            pytest.skip("US English locale not available for testing")

    @classmethod
    def teardown_class(cls):
        del cls.locales

    def test_quandl(self):
        # asserts that google is minimally working and that it throws
        # an exception when DataReader can't get a 200 response from
        # google
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)

        for locale in self.locales:
            with tm.set_locale(locale):
                panel = web.DataReader("F", 'quandl', start, end)
            assert panel.Close[-1] == 13.68

        with pytest.raises(Exception):
            web.DataReader('NON EXISTENT TICKER', 'quandl', start, end)
