from datetime import datetime

import pytest

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web

from pandas import DataFrame
from pandas_datareader._utils import RemoteDataError


class TestFred(tm.TestCase):
    def test_fred(self):

        # Raises an exception when DataReader can't
        # get a 200 response from FRED.

        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 1)

        df = web.DataReader("GDP", "fred", start, end)
        ts = df['GDP']
        self.assertEqual(ts.index[0], pd.to_datetime("2010-01-01"))
        self.assertEqual(ts.index[-1], pd.to_datetime("2013-01-01"))
        self.assertEqual(ts.index.name, "DATE")
        self.assertEqual(ts.name, "GDP")

        received = ts.tail(1)[0]
        self.assertEqual(int(received), 16475)

        with tm.assertRaises(RemoteDataError):
            web.DataReader("NON EXISTENT SERIES", 'fred', start, end)

    def test_fred_nan(self):
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)
        df = web.DataReader("DFII5", "fred", start, end)
        assert pd.isnull(df.ix['2010-01-01'][0])

    @pytest.mark.skip(reason='Buggy as of 2/18/14; maybe a data revision?')
    def test_fred_parts(self):  # pragma: no cover
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)
        df = web.get_data_fred("CPIAUCSL", start, end)
        self.assertEqual(df.ix['2010-05-01'][0], 217.23)

        t = df.CPIAUCSL.values
        assert np.issubdtype(t.dtype, np.floating)
        self.assertEqual(t.shape, (37,))

    def test_fred_part2(self):
        expected = [[576.7],
                    [962.9],
                    [684.7],
                    [848.3],
                    [933.3]]
        result = web.get_data_fred("A09024USA144NNBR", start="1915").ix[:5]
        tm.assert_numpy_array_equal(result.values, np.array(expected))

    def test_invalid_series(self):
        name = "NOT A REAL SERIES"
        self.assertRaises(Exception, web.get_data_fred, name)

    @pytest.mark.skip(reason='Buggy as of 2/18/14; maybe a data revision?')
    def test_fred_multi(self):  # pragma: no cover
        names = ['CPIAUCSL', 'CPALTT01USQ661S', 'CPILFESL']
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)

        received = web.DataReader(names, "fred", start, end).head(1)
        expected = DataFrame([[217.478, 0.99701529, 220.544]], columns=names,
                             index=[pd.tslib.Timestamp('2010-01-01 00:00:00')])
        expected.index.rename('DATE', inplace=True)
        tm.assert_frame_equal(received, expected, check_less_precise=True)

    def test_fred_multi_bad_series(self):
        names = ['NOTAREALSERIES', 'CPIAUCSL', "ALSO FAKE"]
        with tm.assertRaises(RemoteDataError):
            web.DataReader(names, data_source="fred")
