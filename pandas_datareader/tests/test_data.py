import nose

from pandas import DataFrame
try:
    from pandas.util.testing import assert_produces_warning
except ImportError:  # pragma: no cover
    assert_produces_warning = None

import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader.data import DataReader


class TestOptionsWarnings(tm.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestOptionsWarnings, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(TestOptionsWarnings, cls).tearDownClass()

    def test_options_source_warning(self):
        if not assert_produces_warning:  # pragma: no cover
            raise nose.SkipTest("old version of pandas without "
                                "compat.assert_produces_warning")
        with assert_produces_warning():
            aapl = web.Options('aapl')  # noqa


class TestDataReader(tm.TestCase):
    def test_is_s3_url(self):
        from pandas.io.common import _is_s3_url
        self.assertTrue(_is_s3_url("s3://pandas/somethingelse.com"))

    def test_read_yahoo(self):
        gs = DataReader("GS", "yahoo")
        assert isinstance(gs, DataFrame)

    def test_read_yahoo_dividends(self):
        gs = DataReader("GS", "yahoo-dividends")
        assert isinstance(gs, DataFrame)

    def test_read_google(self):
        gs = DataReader("GS", "google")
        assert isinstance(gs, DataFrame)

    def test_read_fred(self):
        vix = DataReader("VIXCLS", "fred")
        assert isinstance(vix, DataFrame)

    def test_not_implemented(self):
        self.assertRaises(NotImplementedError, DataReader, "NA", "NA")


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)  # pragma: no cover
