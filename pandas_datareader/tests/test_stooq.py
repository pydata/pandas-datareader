import pandas_datareader.data as web
from pandas_datareader.data import get_data_stooq


def test_stooq_dji():
    f = web.DataReader('GS', 'stooq')
    assert f.shape[0] > 0


def test_get_data_stooq_dji():
    f = get_data_stooq('AMZN')
    assert f.shape[0] > 0
