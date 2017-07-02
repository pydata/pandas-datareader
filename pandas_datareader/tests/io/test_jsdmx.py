# pylint: disable-msg=E1101,W0613,W0603

import os

import numpy as np
import pandas as pd
import pandas.util.testing as tm

from pandas_datareader.io import read_jsdmx


class TestJSDMX(object):

    def setup_method(self, method):
        self.dirpath = tm.get_data_path()

    def test_tourism(self):
        # OECD -> Industry and Services -> Inbound Tourism
        result = read_jsdmx(os.path.join(self.dirpath, 'jsdmx',
                                         'tourism.json'))
        assert isinstance(result, pd.DataFrame)

        exp_col = pd.MultiIndex.from_product(
            [['Japan'], ['China', 'Hong Kong, China',
                         'Total international arrivals',
                         'Total international receipts',
                         'International passenger transport receipts',
                         'International travel receipts',
                         'Korea', 'Chinese Taipei', 'United States']],
            names=['Country', 'Variable'])
        exp_idx = pd.DatetimeIndex(['2004', '2005', '2006', '2007',
                                    '2008', '2009', '2010', '2011',
                                    '2012'], name='Year')

        values = np.array([
            [616, 300, 6138, 1550, 330, 1220, 1588, 1081, 760],
            [653, 299, 6728, 1710, 340, 1370, 1747, 1275, 822],
            [812, 352, 7334, 1330, 350, 980, 2117, 1309, 817],
            [942, 432, 8347, 1460, 360, 1100, 2601, 1385, 816],
            [1000, 550, 8351, 1430, 310, 1120, 2382, 1390, 768],
            [1006, 450, 6790, 1170, 210, 960, 1587, 1024, 700],
            [1413, 509, 8611, 1350, 190, 1160, 2440, 1268, 727],
            [1043, 365, 6219, 1000, 100, 900, 1658, 994, 566],
            [1430, 482, 8368, 1300, 100, 1200, 2044, 1467, 717]])
        expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
        tm.assert_frame_equal(result, expected)

    def test_land_use(self):
        # OECD -> Environment -> Resources Land Use
        result = read_jsdmx(os.path.join(self.dirpath, 'jsdmx',
                                         'land_use.json'))
        assert isinstance(result, pd.DataFrame)
        result = result.loc['2010':'2011']

        exp_col = pd.MultiIndex.from_product([
            ['Japan', 'United States'],
            ['Arable land and permanent crops',
             'Arable and cropland % land area',
             'Total area', 'Forest', 'Forest % land area',
             'Land area', 'Permanent meadows and pastures',
             'Meadows and pastures % land area', 'Other areas',
             'Other % land area']], names=['Country', 'Variable'])
        exp_idx = pd.DatetimeIndex(['2010', '2011'], name='Year')
        values = np.array([[45930, 12.601, 377950, 249790, 68.529, 364500,
                            np.nan, np.nan, 68780, 18.87, 1624330, 17.757,
                            9831510, 3040220, 33.236, 9147420, 2485000,
                            27.166, 1997870, 21.841],
                           [45610, 12.513, 377955, 249878, 68.554, 364500,
                            np.nan, np.nan, 69012, 18.933, 1627625, 17.793,
                            9831510, 3044048, 33.278, 9147420, 2485000,
                            27.166, 1990747, 21.763]])
        expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
        tm.assert_frame_equal(result, expected)
