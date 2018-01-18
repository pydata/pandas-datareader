from datetime import datetime

import pytest

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web

from pandas import DataFrame
from pandas_datareader._utils import RemoteDataError


class TestFred(object):
    def test_fred(self):

        # Raises an exception when DataReader can't
        # get a 200 response from FRED.

        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 1)

        df = web.DataReader("GDP", "fred", start, end)
        ts = df['GDP']

        assert ts.index[0] == pd.to_datetime("2010-01-01")
        assert ts.index[-1] == pd.to_datetime("2013-01-01")
        assert ts.index.name == "DATE"
        assert ts.name == "GDP"

        received = ts.tail(1)[0]
        assert int(received) == 16475

        with pytest.raises(RemoteDataError):
            web.DataReader("NON EXISTENT SERIES", 'fred', start, end)

    def test_fred_nan(self):
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)
        df = web.DataReader("DFII5", "fred", start, end)
        assert pd.isnull(df.loc['2010-01-01'][0])

    def test_fred_parts(self):  # pragma: no cover
        start = datetime(2010, 1, 1)
        end = datetime(2013, 1, 27)
        df = web.get_data_fred("CPIAUCSL", start, end)
        assert df.loc['2010-05-01'][0] == 217.29

        t = df.CPIAUCSL.values
        assert np.issubdtype(t.dtype, np.floating)
        assert t.shape == (37,)

    def test_fred_part2(self):
        expected = [[576.7],
                    [962.9],
                    [684.7],
                    [848.3],
                    [933.3]]
        result = web.get_data_fred("A09024USA144NNBR", start="1915").iloc[:5]
        tm.assert_numpy_array_equal(result.values, np.array(expected))

    def test_invalid_series(self):
        name = "NOT A REAL SERIES"
        with pytest.raises(Exception):
            web.get_data_fred(name)

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
        with pytest.raises(RemoteDataError):
            web.DataReader(names, data_source="fred")
