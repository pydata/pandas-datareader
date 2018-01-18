import pytest

from pandas import DataFrame
from pandas_datareader.data import DataReader


class TestDataReader(object):

    def test_read_google(self):
        gs = DataReader("GS", "google")
        assert isinstance(gs, DataFrame)

    def test_read_iex(self):
        gs = DataReader("GS", "iex-last")
        assert isinstance(gs, DataFrame)

    def test_read_fred(self):
        vix = DataReader("VIXCLS", "fred")
        assert isinstance(vix, DataFrame)

    def test_not_implemented(self):
        with pytest.raises(NotImplementedError):
            DataReader("NA", "NA")
