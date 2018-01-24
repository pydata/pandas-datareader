import os

import pandas as pd
import pytest

from pandas_datareader.tiingo import TiingoDailyReader, TiingoMetaDataReader, \
    TiingoQuoteReader

TEST_API_KEY = os.getenv('TIINGO_API_KEY')

syms = ['GOOG', ['GOOG', 'XOM']]
ids = list(map(str, syms))


@pytest.fixture(params=syms, ids=ids)
def symbols(request):
    return request.param


@pytest.mark.skipif(TEST_API_KEY is None, reason="TIINGO_API_KEY not set")
def test_tiingo_quote(symbols):
    df = TiingoQuoteReader(symbols=symbols).read()
    assert isinstance(df, pd.DataFrame)
    if isinstance(symbols, str):
        symbols = [symbols]
    assert df.shape[0] == len(symbols)


@pytest.mark.skipif(TEST_API_KEY is None, reason="TIINGO_API_KEY not set")
def test_tiingo_historical(symbols):
    df = TiingoDailyReader(symbols=symbols).read()
    assert isinstance(df, pd.DataFrame)
    if isinstance(symbols, str):
        symbols = [symbols]
    assert df.index.levels[0].shape[0] == len(symbols)


@pytest.mark.skipif(TEST_API_KEY is None, reason="TIINGO_API_KEY not set")
def test_tiingo_metadata(symbols):
    df = TiingoMetaDataReader(symbols=symbols).read()
    assert isinstance(df, pd.DataFrame)
    if isinstance(symbols, str):
        symbols = [symbols]
    assert df.shape[1] == len(symbols)
