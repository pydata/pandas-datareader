import pandas_datareader.data as web
from pandas_datareader.data import get_data_stooq


def test_stooq_dji():
    f = web.DataReader('GS', 'stooq')
    assert f.shape[0] > 0


def test_get_data_stooq_dji():
    f = get_data_stooq('AMZN')
    assert f.shape[0] > 0


def test_get_data_stooq_dates():
    f = get_data_stooq('SPY', start='20180101', end='20180115')
    assert f.shape[0] == 9
