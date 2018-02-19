import pytest

from pandas_datareader.data import get_sector_performance_av

import pandas as pd
from pandas.testing import assert_index_equal


class TestAVSector(object):

    @classmethod
    def setup_class(cls):
        pytest.importorskip('lxml')

    def test_sector(self):
        df = get_sector_performance_av()

        cols = pd.Index(["RT", "1D", "5D", "1M", "3M", "YTD", "1Y", "3Y", "5Y",
                         "10Y"])
        assert_index_equal(df.columns, cols)
        assert "Energy" in df.index
        assert "Real Estate" in df.index
