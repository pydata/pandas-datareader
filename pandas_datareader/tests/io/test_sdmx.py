# pylint: disable-msg=E1101,W0613,W0603

import os

import numpy as np
import pandas as pd
import pandas.util.testing as tm

from pandas_datareader.io.sdmx import read_sdmx, _read_sdmx_dsd


class TestSDMX(object):

    def setup_method(self, method):
        self.dirpath = tm.get_data_path()

    def test_tourism(self):
        # Eurostat
        # Employed doctorate holders in non managerial and non professional
        # occupations by fields of science (%)
        dsd = _read_sdmx_dsd(os.path.join(self.dirpath, 'sdmx',
                                          'DSD_cdh_e_fos.xml'))
        df = read_sdmx(os.path.join(self.dirpath, 'sdmx',
                                    'cdh_e_fos.xml'), dsd=dsd)

        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 336)

        df = df['Percentage']['Total']['Natural sciences']
        df = df[['Norway', 'Poland', 'Portugal', 'Russia']]

        exp_col = pd.MultiIndex.from_product([['Norway', 'Poland', 'Portugal',
                                               'Russia'], ['Annual']],
                                             names=['GEO', 'FREQ'])
        exp_idx = pd.DatetimeIndex(['2009', '2006'], name='TIME_PERIOD')

        values = np.array([[20.38, 25.1, 27.77, 38.1],
                           [25.49, np.nan, 39.05, np.nan]])
        expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
        tm.assert_frame_equal(df, expected)
