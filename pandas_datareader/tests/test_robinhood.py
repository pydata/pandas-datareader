import numpy as np
import pandas as pd
import pytest

from pandas_datareader.robinhood import RobinhoodHistoricalReader, RobinhoodQuoteReader

syms = ["GOOG", ["GOOG", "AAPL"]]
ids = list(map(str, syms))


@pytest.fixture(params=["GOOG", ["GOOG", "AAPL"]], ids=ids)
def symbols(request):
    return request.param


@pytest.mark.xfail(reason="Deprecated")
def test_robinhood_quote(symbols):
    df = RobinhoodQuoteReader(symbols=symbols).read()
    assert isinstance(df, pd.DataFrame)
    if isinstance(symbols, str):
        symbols = [symbols]
    assert df.shape[1] == len(symbols)


@pytest.mark.xfail(reason="Deprecated")
def test_robinhood_quote_too_many():
    syms = np.random.randint(65, 90, size=(10000, 4)).tolist()
    syms = list(map(lambda r: "".join(map(chr, r)), syms))
    syms = list(set(syms))
    with pytest.raises(ValueError):
        RobinhoodQuoteReader(symbols=syms)


@pytest.mark.xfail(reason="Deprecated")
def test_robinhood_historical_too_many():
    syms = np.random.randint(65, 90, size=(10000, 4)).tolist()
    syms = list(map(lambda r: "".join(map(chr, r)), syms))
    syms = list(set(syms))
    with pytest.raises(ValueError):
        RobinhoodHistoricalReader(symbols=syms)
    with pytest.raises(ValueError):
        RobinhoodHistoricalReader(symbols=syms[:76])


@pytest.mark.xfail(reason="Deprecated")
def test_robinhood_historical(symbols):
    df = RobinhoodHistoricalReader(symbols=symbols).read()
    assert isinstance(df, pd.DataFrame)
    if isinstance(symbols, str):
        symbols = [symbols]
    assert df.index.levels[0].shape[0] == len(symbols)
