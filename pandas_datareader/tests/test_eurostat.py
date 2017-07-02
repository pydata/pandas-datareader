import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web

from pandas_datareader.compat import assert_raises_regex


class TestEurostat(object):

    def test_get_cdh_e_fos(self):
        # Employed doctorate holders in non managerial and non professional
        # occupations by fields of science (%)
        df = web.DataReader('cdh_e_fos', 'eurostat',
                            start=pd.Timestamp('2005-01-01'),
                            end=pd.Timestamp('2010-01-01'))

        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 336)

        df = df['Percentage']['Total']['Natural sciences']
        df = df[['Norway', 'Poland', 'Portugal', 'Russia']]

        exp_col = pd.MultiIndex.from_product([['Norway', 'Poland', 'Portugal',
                                             'Russia'], ['Annual']],
                                             names=['GEO', 'FREQ'])
        exp_idx = pd.DatetimeIndex(['2006-01-01', '2009-01-01'],
                                   name='TIME_PERIOD')

        values = np.array([[25.49, np.nan, 39.05, np.nan],
                           [20.38, 25.1, 27.77, 38.1]])
        expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
        tm.assert_frame_equal(df, expected)

    def test_get_sts_cobp_a(self):
        # Building permits - annual data (2010 = 100)
        df = web.DataReader('sts_cobp_a', 'eurostat',
                            start=pd.Timestamp('2000-01-01'),
                            end=pd.Timestamp('2013-01-01'))

        idx = pd.date_range('2000-01-01', '2013-01-01', freq='AS',
                            name='TIME_PERIOD')
        ne_name = ('Index, 2010=100',
                   'Building permits - m2 of useful floor area',
                   'Unadjusted data (i.e. neither seasonally adjusted nor '
                   'calendar adjusted data)',
                   'Non-residential buildings, except office buildings',
                   'Netherlands', 'Annual')
        ne_values = [200.0, 186.5, 127.3, 130.7, 143.3, 147.8, 176.7,
                     227.4, 199.4, 128.5, 100.0, 113.8, 89.3, 77.6]
        ne = pd.Series(ne_values, name=ne_name, index=idx)

        uk_name = ('Index, 2010=100',
                   'Building permits - m2 of useful floor area',
                   'Unadjusted data (i.e. neither seasonally adjusted nor '
                   'calendar adjusted data)',
                   'Non-residential buildings, except office buildings',
                   'United Kingdom', 'Annual')
        uk_values = [112.5, 113.3, 110.2, 112.1, 119.1, 112.7, 113.1,
                     121.8, 114.0, 105.9, 100.0, 98.6, 103.7, 81.3]
        uk = pd.Series(uk_values, name=uk_name, index=idx)

        for expected in [ne, uk]:
            result = df[expected.name]
            tm.assert_series_equal(result, expected)

    def test_get_nrg_pc_202(self):
        # see gh-149

        df = web.DataReader('nrg_pc_202', 'eurostat',
                            start=pd.Timestamp('2010-01-01'),
                            end=pd.Timestamp('2013-01-01'))

        name = ('All taxes and levies included',
                'Gigajoules (gross calorific value = GCV)',
                'Euro',
                'Band D1 : Consumption < 20 GJ',
                'Natural gas', 'Denmark', 'Semi-annual')

        exp_index = pd.Index(['2013-B2', '2013-B1', '2012-B2', '2012-B1',
                              '2011-B2', '2011-B1', '2010-B2', '2010-B1'],
                             name='TIME_PERIOD')
        exp = pd.Series([27.1403, 27.5854, 26.5285, 27.2187,
                         28.5862, 28.6448, 26.8147, 26.4979],
                        index=exp_index, name=name)

        tm.assert_series_equal(df[name], exp)

    def test_get_prc_hicp_manr_exceeds_limit(self):
        # see gh-149
        msg = 'Query size exceeds maximum limit'
        with assert_raises_regex(ValueError, msg):
            web.DataReader('prc_hicp_manr', 'eurostat',
                           start=pd.Timestamp('2000-01-01'),
                           end=pd.Timestamp('2013-01-01'))
