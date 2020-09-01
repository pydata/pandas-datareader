import os

import pandas as pd
import pytest

from pandas_datareader.tiingo import (
    TiingoDailyReader,
    TiingoIEXHistoricalReader,
    TiingoMetaDataReader,
    TiingoQuoteReader,
    get_tiingo_symbols,
)

pytestmark = pytest.mark.requires_api_key

TEST_API_KEY = os.getenv("TIINGO_API_KEY")
# Ensure blank TEST_API_KEY not used in pull request
TEST_API_KEY = None if not TEST_API_KEY else TEST_API_KEY

syms = ["GOOG", ["GOOG", "XOM"]]
ids = list(map(str, syms))


@pytest.fixture(params=syms, ids=ids)
def symbols(request):
    return request.param


formats = ["csv", "json"]
format_ids = list(map(str, formats))


@pytest.fixture(params=formats, ids=format_ids)
def formats(request):
    return request.param


@pytest.mark.skipif(TEST_API_KEY is None, reason="TIINGO_API_KEY not set")
def test_tiingo_quote(symbols, formats):
    df = TiingoQuoteReader(symbols=symbols, response_format=formats).read()
    assert isinstance(df, pd.DataFrame)
    if isinstance(symbols, str):
        symbols = [symbols]
    assert df.shape[0] == len(symbols)


@pytest.mark.skipif(TEST_API_KEY is None, reason="TIINGO_API_KEY not set")
def test_tiingo_historical(symbols, formats):
    df = TiingoDailyReader(symbols=symbols, response_format=formats).read()
    assert isinstance(df, pd.DataFrame)
    if isinstance(symbols, str):
        symbols = [symbols]
    assert df.index.levels[0].shape[0] == len(symbols)


@pytest.mark.skipif(TEST_API_KEY is None, reason="TIINGO_API_KEY not set")
def test_tiingo_iex_historical(symbols, formats):
    df = TiingoIEXHistoricalReader(symbols=symbols, response_format=formats).read()
    df.head()
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


def test_tiingo_no_api_key(symbols):
    from test.support import EnvironmentVarGuard

    env = EnvironmentVarGuard()
    env.unset("TIINGO_API_KEY")
    with env:
        with pytest.raises(ValueError):
            TiingoMetaDataReader(symbols=symbols)


@pytest.mark.skipif(
    pd.__version__ == "0.19.2",
    reason="pandas 0.19.2 does not like\
         this file format",
)
def test_tiingo_stock_symbols():
    sym = get_tiingo_symbols()
    assert isinstance(sym, pd.DataFrame)
