import nose

import pandas as pd
import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError


class TestEdgarIndex(tm.TestCase):
    def test_get_full_index(self):
        try:
            ed = web.DataReader('full', 'edgar-index')
            assert len(ed) > 1000

            exp_columns = pd.Index(['cik', 'company_name', 'form_type',
                                    'date_filed', 'filename'], dtype='object')
            tm.assert_index_equal(ed.columns, exp_columns)

        except RemoteDataError as e:
            raise nose.SkipTest(e)

    def test_get_nonzip_index_and_low_date(self):
        try:
            ed = web.DataReader('daily', 'edgar-index', '1994-06-30',
                                '1994-07-02')
            assert len(ed) > 200
            exp_columns = pd.Index(['cik', 'company_name',
                                    'form_type', 'date_filed',
                                    'filename'], dtype='object')
            tm.assert_index_equal(ed.columns, exp_columns)

        except RemoteDataError as e:
            raise nose.SkipTest(e)

    def test_6_digit_date(self):
        try:
            ed = web.DataReader('daily', 'edgar-index', start='1998-05-18',
                                end='1998-05-18')
            assert len(ed) < 1200

            exp_columns = pd.Index(['cik', 'company_name',
                                    'form_type', 'date_filed',
                                    'filename'], dtype='object')
            tm.assert_index_equal(ed.columns, exp_columns)

        except RemoteDataError as e:
            raise nose.SkipTest(e)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
