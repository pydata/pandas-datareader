import pytest

from pandas import DataFrame
from pandas_datareader.data import DataReader
from pandas_datareader.exceptions import UnstableAPIWarning


class TestDataReader(object):

    @pytest.mark.xfail(reason="Deprecated")
    def test_read_google(self):
        with pytest.warns(UnstableAPIWarning):
            gs = DataReader("GS", "google")
            assert isinstance(gs, DataFrame)

    def test_read_iex(self):
        gs = DataReader("GS", "iex-last")
        assert isinstance(gs, DataFrame)

    def test_read_fred(self):
        vix = DataReader("VIXCLS", "fred")
        assert isinstance(vix, DataFrame)

    @pytest.mark.xfail(reason="Deprecated")
    def test_read_mstar(self):
        gs = DataReader("GS", data_source="morningstar")
        assert isinstance(gs, DataFrame)

    def test_not_implemented(self):
        with pytest.raises(NotImplementedError):
            DataReader("NA", "NA")
