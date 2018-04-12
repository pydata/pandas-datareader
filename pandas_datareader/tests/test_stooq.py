import pandas_datareader.data as web
from pandas_datareader.data import get_data_stooq


def test_stooq_dji():
    f = web.DataReader('GS.US', 'stooq')
    assert f.shape[0] > 0

def test_get_data_stooq_dji():
    f = get_data_stooq('AMZN.US')
    assert f.shape[0] > 0
