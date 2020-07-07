import pytest

import pandas_datareader.data as web
from pandas_datareader.exceptions import ImmediateDeprecationError

pytestmark = [
    pytest.mark.alpha_vantage,
]


class TestAVQuotes(object):
    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    def test_invalid_symbol(self):
        with pytest.raises(ImmediateDeprecationError):
            web.get_quote_av("BADSYMBOL")

    def test_bad_multiple_symbol(self):
        with pytest.raises(ImmediateDeprecationError):
            web.get_quote_av(["AAPL", "BADSYMBOL"])

    def test_single_symbol(self):
        with pytest.raises(ImmediateDeprecationError):
            web.get_quote_av("AAPL", retry_count=6, pause=20.5)

    def test_multi_symbol(self):
        with pytest.raises(ImmediateDeprecationError):
            web.get_quote_av(["AAPL", "TSLA"], retry_count=6, pause=20.5)

    @pytest.mark.xfail(reason="May return NaN outside of market hours")
    def test_return_types(self):
        with pytest.raises(ImmediateDeprecationError):
            web.get_quote_av("AAPL", retry_count=6, pause=20.5)
