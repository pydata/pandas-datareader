import sys

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web


class TestEurostat(tm.TestCase):

    def setUp(self):

        if sys.version_info < (2, 7, 0):
            import nose
            raise nose.SkipTest("Doesn't support Python 2.6 because of ElementTree incompat")


    def test_get_cdh_e_fos(self):
        # Employed doctorate holders in non managerial and non professional
        # occupations by fields of science (%)
        df = web.DataReader('cdh_e_fos', 'eurostat', start=pd.Timestamp('2005-01-01'),
                            end=pd.Timestamp('2010-01-01'))

        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEqual(df.shape, (2, 336))

        df = df['Percentage']['Total']['Natural sciences']
        df = df[['Norway', 'Poland', 'Portugal', 'Russia']]

        exp_col = pd.MultiIndex.from_product([['Norway', 'Poland', 'Portugal', 'Russia'],
                                              ['Annual']],
                                             names=['GEO', 'FREQ'])
        exp_idx = pd.DatetimeIndex(['2006-01-01', '2009-01-01'], name='TIME_PERIOD')

        values = np.array([[25.49, np.nan, 39.05, np.nan],
                           [20.38, 25.1, 27.77, 38.1]])
        expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
        tm.assert_frame_equal(df, expected)

    def test_get_sts_cobp_a(self):
        # Building permits - annual data (2010 = 100)
        df = web.DataReader('sts_cobp_a', 'eurostat', start=pd.Timestamp('1992-01-01'),
                            end=pd.Timestamp('2013-01-01'))

        idx = pd.date_range('1992-01-01', '2013-01-01', freq='AS', name='TIME_PERIOD')
        ne = pd.Series([np.nan, np.nan, np.nan, 144.53, 136.97, 180.02, 198.36,
                        215.12, 200.05, 186.46, 127.33, 130.67, 143.26, 147.83,
                        176.69, 227.36, 199.45, 128.49, 100.0, 113.83, 89.33, 77.57],
                       name=('Building permits - m2 of useful floor area',
                             'Gross data',
                             'Non-residential buildings, except office buildings',
                             'Netherlands', 'Annual'),
                       index=idx)

        uk = pd.Series([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 120.37,
                        115.93, 112.53, 113.32, 110.18, 112.14, 119.06, 112.66,
                        113.05, 121.8, 113.97, 105.88, 100.0, 98.56, 103.69, 81.32],
                       name=('Building permits - m2 of useful floor area',
                             'Gross data',
                             'Non-residential buildings, except office buildings',
                             'United Kingdom', 'Annual'),
                       index=idx)

        for expected in [ne, uk]:
            result = df[expected.name]
            tm.assert_series_equal(result, expected)


if __name__ == '__main__':
    import nose
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'], exit=False)
