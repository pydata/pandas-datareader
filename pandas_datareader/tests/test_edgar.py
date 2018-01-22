import pandas as pd
import pandas.util.testing as tm
import pytest

import pandas_datareader.data as web
from pandas_datareader.exceptions import ImmediateDeprecationError


class TestEdgarIndex(object):

    @classmethod
    def setup_class(cls):
        # As of December 31, SEC has disabled ftp access to EDGAR.
        # Disabling tests until re-write.
        pytest.skip("Disabling tests until re-write.")

    def test_raises(self):
        with pytest.raises(ImmediateDeprecationError):
            web.DataReader('full', 'edgar-index')

    def test_get_full_index(self):
        ed = web.DataReader('full', 'edgar-index')
        assert len(ed) > 1000

        exp_columns = pd.Index(['cik', 'company_name', 'form_type',
                                'date_filed', 'filename'], dtype='object')
        tm.assert_index_equal(ed.columns, exp_columns)

    def test_get_nonzip_index_and_low_date(self):
        ed = web.DataReader('daily', 'edgar-index', '1994-06-30',
                            '1994-07-02')
        assert len(ed) > 200
        assert ed.index.nlevels == 2

        dti = ed.index.get_level_values(0)
        assert isinstance(dti, pd.DatetimeIndex)
        exp_columns = pd.Index(['company_name', 'form_type',
                                'filename'], dtype='object')
        tm.assert_index_equal(ed.columns, exp_columns)

    def test_get_gz_index_and_no_date(self):
        # TODO: Rewrite, as this test causes Travis to timeout.

        ed = web.DataReader('daily', 'edgar-index')
        assert len(ed) > 2000

    def test_6_digit_date(self):
        ed = web.DataReader('daily', 'edgar-index', start='1998-05-18',
                            end='1998-05-18')
        assert len(ed) < 1200
        assert ed.index.nlevels == 2

        dti = ed.index.get_level_values(0)
        assert isinstance(dti, pd.DatetimeIndex)
        assert dti[0] == pd.Timestamp('1998-05-18')
        assert dti[-1] == pd.Timestamp('1998-05-18')

        exp_columns = pd.Index(['company_name', 'form_type',
                                'filename'], dtype='object')
        tm.assert_index_equal(ed.columns, exp_columns)
