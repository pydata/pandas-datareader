# pylint: disable-msg=E1101,W0613,W0603

import os
import sys

import numpy as np
import pandas as pd
import pandas.util.testing as tm

from pandas_datareader.io.sdmx import read_sdmx, _read_sdmx_dsd


class TestSDMX(tm.TestCase):

    def setUp(self):

        if sys.version_info < (2, 7, 0):
            import nose
            raise nose.SkipTest("Doesn't support Python 2.6 because of ElementTree incompat")

        self.dirpath = tm.get_data_path()

    def test_tourism(self):
        # Eurostat
        # Employed doctorate holders in non managerial and non professional
        # occupations by fields of science (%)
        dsd = _read_sdmx_dsd(os.path.join(self.dirpath, 'sdmx', 'DSD_cdh_e_fos.xml'))
        df = read_sdmx(os.path.join(self.dirpath, 'sdmx', 'cdh_e_fos.xml'), dsd=dsd)

        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEqual(df.shape, (2, 336))

        df = df['Percentage']['Total']['Natural sciences']
        df = df[['Norway', 'Poland', 'Portugal', 'Russia']]

        exp_col = pd.MultiIndex.from_product([['Norway', 'Poland', 'Portugal', 'Russia'],
                                              ['Annual']],
                                             names=['GEO', 'FREQ'])
        exp_idx = pd.DatetimeIndex(['2009', '2006'], name='TIME_PERIOD')

        values = np.array([[20.38, 25.1, 27.77, 38.1],
                           [25.49, np.nan, 39.05, np.nan]])
        expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
        tm.assert_frame_equal(df, expected)


if __name__ == '__main__':
    import nose
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'], exit=False)
