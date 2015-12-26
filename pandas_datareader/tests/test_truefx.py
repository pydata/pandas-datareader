import os
import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web
from pandas_datareader.truefx import TrueFXReader

class TestTrueFX(tm.TestCase):
    def setUp(self):
        if os.getenv('USE_REQUESTS_CACHE', '0') == '1':
            import requests_cache
            from datetime import timedelta
            self.session = requests_cache.CachedSession(cache_name='cache', expire_after=timedelta(days=30))
        else:
            self.session = None
        self.dr = TrueFXReader(retry_count=3, pause=0.001, session=self.session)

    def test_url(self):
        expected = 'http://www.truefx.com/dev/data/2014/JANUARY-2014/AUDUSD-2014-01.zip'
        tm.assert_equal(self.dr.url('AUDUSD', 2014, 1), expected)

    def test_filename_csv(self):
        expected = 'AUDUSD-2014-01.csv'
        tm.assert_equal(self.dr._filename_csv('AUDUSD', 2014, 1), expected)

    def test_get_truefx_datareader(self):
        df = web.DataReader('AUD/USD', 'truefx', '2014-01-01', '2014-02-28', session=self.session)
        tm.assert_almost_equal(df.loc['2014-01-01 21:55:34.404', 'Ask'], 0.88922)
        tm.assert_almost_equal(df.loc['2014-02-03 00:03:38.169', 'Ask'], 0.87524)

if __name__ == '__main__':
    import nose
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'], exit=False)
