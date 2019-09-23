import datetime
import pandas as pd
import pytest

import pandas_datareader as pdr

pytestmark = pytest.mark.stable


class TestBinance(object):
    limit = 146
    binance_reader = pdr.get_data_binance(symbols="BNBBTC", limit=limit)
    columns_should_be_present = [
        "Open time",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Close time",
        "Quote asset volume",
        "Number of trades",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
    ]

    def test_object_type(self):
        assert isinstance(self.binance_reader, pd.DataFrame)

    def test_object_dimensions(self):
        assert self.binance_reader.shape[1] == len(
            self.columns_should_be_present
        ), "Number of columns in the returned data is higher than 11"
        assert (
            self.binance_reader.shape[0] <= self.limit
        ), "Number of rows in data returned is higher than the expected number"

    def test_object_columns(self):
        for col in self.binance_reader.columns:
            assert col in self.columns_should_be_present, (
                "Column" + col + "should not be present in the response"
            )
    
    def test_object_values(self):
        start = datetime.datetime(2018, 5, 17)
        end = datetime.datetime(2018, 5, 18)
        binance_reader = pdr.get_data_binance(symbols="BNBBTC", start=start, end=end)
        expected_values = [1526515200000,'0.00147300','0.00158900','0.00147100','0.00153670','1729643.47000000',1526601599999,'2630.46769190',68061,'968945.36000000','1472.76577070']
        actual_values = binance_reader.values.tolist()[0]
        assert expected_values == actual_values