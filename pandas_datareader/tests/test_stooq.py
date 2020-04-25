import pytest

import pandas_datareader.data as web
from pandas_datareader.data import get_data_stooq

pytestmark = pytest.mark.stable


def test_stooq_dji():
    f = web.DataReader("GS", "stooq")
    assert f.shape[0] > 0


def test_get_data_stooq_dji():
    f = get_data_stooq("AMZN")
    assert f.shape[0] > 0


def test_get_data_stooq_dates():
    f = get_data_stooq("SPY", start="20180101", end="20180115")
    assert f.shape[0] == 9


def test_stooq_sp500():
    f = get_data_stooq("^SPX")
    assert f.shape[0] > 0


def test_stooq_clx19f():
    f = get_data_stooq("CLX26.F", start="20200101", end="20200115")
    assert f.shape[0] > 0


def test_get_data_stooq_dax():
    f = get_data_stooq("^DAX")
    assert f.shape[0] > 0


def test_stooq_googl():
    f = get_data_stooq("GOOGL.US")
    assert f.shape[0] > 0


def test_get_data_ibm():
    f = get_data_stooq("IBM.UK")
    assert f.shape[0] > 0
