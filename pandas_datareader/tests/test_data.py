from pandas import DataFrame
import pytest

from pandas_datareader.base import _DailyBaseReader
from pandas_datareader.data import DataReader, register_custom_datareader

pytestmark = pytest.mark.stable


class TestDataReader(object):
    def test_read_iex(self):
        gs = DataReader("GS", "iex-last")
        assert isinstance(gs, DataFrame)

    def test_read_fred(self):
        vix = DataReader("VIXCLS", "fred")
        assert isinstance(vix, DataFrame)

    def test_not_implemented(self):
        with pytest.raises(NotImplementedError):
            DataReader("NA", "NA")

    def test_custom_reader_acc(self):
        class DemoReader(_DailyBaseReader):
            def __init__(
                self,
                symbols=None,
                start=None,
                end=None,
                retry_count=3,
                pause=0.1,
                session=None,
                api_key=None,
            ):
                super().__init__(
                    symbols=symbols,
                    start=start,
                    end=end,
                    retry_count=retry_count,
                    pause=pause,
                    session=session,
                )

            @property
            def url(self):
                return "https://stooq.com/q/d/l/"

            def _get_params(self, symbol):
                params = {"s": symbol, "i": "d"}
                return params

        register_custom_datareader("demo", DemoReader)
        result = DataReader("USDJPY", "demo")
        assert isinstance(result, DataFrame)

    def test_custom_reader_fail(self):
        with pytest.raises(NotImplementedError):
            DataReader("USDJPY", "demo1")
