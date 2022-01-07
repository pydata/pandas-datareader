from datetime import datetime
from xml.etree import ElementTree

import numpy as np
from pandas import DataFrame, to_datetime
from six import string_types

from pandas_datareader.base import _DailyBaseReader


class NaverDailyReader(_DailyBaseReader):
    """Fetches daily historical data from Naver Finance.

    :param symbols: A single symbol; multiple symbols are not currently supported.
    :param adjust_price: Not implemented
    :param interval: Not implemented
    :param adjust_dividends: Not implemented
    """

    def __init__(
        self,
        symbols=None,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
        adjust_price=False,
        ret_index=False,
        chunksize=1,
        interval="d",
        get_actions=False,
        adjust_dividends=True,
    ):
        if not isinstance(symbols, string_types):
            raise NotImplementedError("Bulk-fetching is not implemented")

        super().__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            chunksize=chunksize,
        )

        self.headers = {
            "Sec-Fetch-Mode": "no-cors",
            "Referer": "https://finance.naver.com/item/fchart.nhn?code={}".format(
                symbols
            ),
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",  # noqa
        }

    @property
    def get_actions(self):
        return self._get_actions

    @property
    def url(self):
        return "https://fchart.stock.naver.com/sise.nhn"

    def _get_params(self, symbol):
        # NOTE: The server does not take start, end dates as inputs; it only
        # takes the number of trading days as an input. To circumvent this
        # pitfall, we calculate the number of business days between self.start
        # and the current date. And then we filter by self.end before returning
        # the final result (in _read_one_data()).
        days = np.busday_count(self.start.date(), datetime.now().date())
        params = {"symbol": symbol, "timeframe": "day", "count": days, "requestType": 0}
        return params

    def _read_one_data(self, url, params):
        """Read one data from specified symbol.

        :rtype: DataFrame
        """
        resp = self._get_response(url, params=params)
        parsed = self._parse_xml_response(resp.text)
        prices = DataFrame(
            parsed, columns=["Date", "Open", "High", "Low", "Close", "Volume"]
        )
        prices["Date"] = to_datetime(prices["Date"])
        prices = prices.set_index("Date")

        # NOTE: See _get_params() for explanations.
        return prices[(prices.index >= self.start) & (prices.index <= self.end)]

    def _parse_xml_response(self, xml_content):
        """Parses XML response from the server.

        An example of response:

            <?xml version="1.0" encoding="EUC-KR" ?>
            <protocol>
                <chartdata symbol="005930" name="Samsung Elctronics" count="500"
                        timeframe="day" precision="0" origintime="19900103">
                    <item data="20170918|218500|222000|217000|220500|72124" />
                    <item data="20170919|218000|221000|217500|219000|62753" />
                    ...
            </protocol>
        """
        root = ElementTree.fromstring(xml_content)
        items = root.findall("chartdata/item")

        for item in items:
            yield item.attrib["data"].split("|")
