import nose
import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError


class TestEdgarIndex(tm.TestCase):

    def setUp(self):
        raise nose.SkipTest()

    def test_get_full_index(self):
        try:
            ed = web.DataReader('full', 'edgar-index')
            assert len(ed) > 1000
        except RemoteDataError as e:
            raise nose.SkipTest(e)

    def test_get_nonzip_index_and_low_date(self):
        try:
            ed = web.DataReader('daily', 'edgar-index', '1994-06-30',
                                '1994-07-02')
            assert len(ed) > 200
        except RemoteDataError as e:
            raise nose.SkipTest(e)

    def test_get_gz_index_and_no_date(self):
        try:
            ed = web.DataReader('daily', 'edgar-index')
            assert len(ed) > 2000
        except RemoteDataError as e:
            raise nose.SkipTest(e)

    def test_6_digit_date(self):
        try:
            ed = web.DataReader('daily', 'edgar-index', '1998-05-18',
                                '1998-05-18')
            assert len(ed) < 1200
        except RemoteDataError as e:
            raise nose.SkipTest(e)

if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
