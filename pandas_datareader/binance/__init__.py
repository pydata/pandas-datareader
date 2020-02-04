import time

import pandas as pd
import requests

from pandas_datareader.base import _BaseReader

BINANCE_BASE_URL = "https://api.binance.com"


class BinanceReader(_BaseReader):
    """Get data for the given name from Binance."""

    _format = None

    _intervals = {
        "ONEMINUTE": "1m",
        "THREEMINUTE": "3m",
        "FIVEMINUTE": "5m",
        "FIFTEENMINUTE": "15m",
        "THIRTYMINUTE": "30m",
        "ONEHOUR": "1h",
        "TWOHOUR": "2h",
        "FOURHOUR": "4h",
        "SIXHOUR": "6h",
        "EIGHTHOUR": "8h",
        "TWELVEHOUR": "12h",
        "ONEDAY": "1d",
        "THREEDAY": "3d",
        "ONEWEEK": "1w",
        "ONEMONTH": "1M",
    }

    def __init__(
        self,
        symbols=None,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
        interval="ONEDAY",
        limit=500,
    ):
        super(BinanceReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )
        self._interval = interval
        self._limit = limit

    @property
    def url(self):
        """API URL"""
        return BINANCE_BASE_URL + "/api/v1/klines"

    def _clean_data(self, data):
        dataFrame = pd.DataFrame(
            data.json(),
            columns=[
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
                "ignore",
            ],
        )
        dataFrame = dataFrame.drop(columns="ignore")
        return dataFrame

    def get_interval(self):
        return self._interval

    def _convert_time_to_miliseconds(self, dt):
        return int(time.mktime(dt.timetuple()) * 1000)

    @property
    def params(self):
        p = {
            "symbol": self.symbols,
            "interval": self._intervals[self._interval],
            "limit": self._limit,
        }
        if self.start is not None:
            p["startTime"] = self._convert_time_to_miliseconds(self.start)
        if self.end is not None:
            p["endTime"] = self._convert_time_to_miliseconds(self.end)

        return p

    def read(self):
        data = requests.get(url=self.url, params=self.params)
        data = self._clean_data(data)
        return data
