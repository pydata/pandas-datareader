from pandas import DataFrame
import pytest

from pandas_datareader.data import DataReader

pytestmark = pytest.mark.stable


class TestDataReader:

    @pytest.mark.xfail(reason="Changes in API need fixes")
    def test_read_iex(self):
        gs = DataReader("GS", "iex-last")
        assert isinstance(gs, DataFrame)

    def test_read_fred(self):
        vix = DataReader("VIXCLS", "fred")
        assert isinstance(vix, DataFrame)

    def test_not_implemented(self):
        with pytest.raises(NotImplementedError):
            DataReader("NA", "NA")
