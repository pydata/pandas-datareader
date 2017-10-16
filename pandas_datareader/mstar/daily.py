
from datetime import datetime, timedelta
from pandas import DataFrame
from pandas_datareader._utils import (SymbolWarning, _sanitize_dates)
import requests
import time
from warnings import warn

class MorningstarDailyReader(object):


    def __init__(self, start=None, end=datetime.today().strftime("%Y-%m-%d"), *args, **kwargs):

        self.start, self.end = _sanitize_dates(start, end)
        self.retry_count = kwargs.get("retry_count", 3)
        self.pause = kwargs.get("pause", 0.001)
        self.timeout = kwargs.get("timeout", 30)
        self.session = requests.session()
        symbols = kwargs.get("symbols")

        self.incl_splits = kwargs.get("incl_splits", True)
        self.incl_dividends = kwargs.get("incl_dividends", True)
        self.incl_vol = kwargs.get("incl_volume", True)
        self.incl_adjclose = kwargs.get("incl_adjclose", True)
        self.currency = kwargs.get("currency", "usd")

        if type(symbols) is str:
            self.symbols = [symbols]
        elif hasattr(symbols, "__iter__"):
            self.symbols = symbols
        else:
            raise TypeError("symbols must be string or iterable. not %s" % type(symbols))

        self._symbol_data_cache = []

    def _get_params(self):
        p = {"range": "|".join([self.start.strftime("%Y-%m-%d"), self.end.strftime("%Y-%m-%d")]), "f": "d",
             "curry": self.currency, "dtype": "his", "showVol": "true",
             "hasF": "true", "isD": "true", "isS": "true", "ProdCode": "DIRECT"}

        return p

    def _dl_mult_symbols(self, symbols):
        passed = []
        failed = []
        symbol_data = []
        for symbol in symbols:

            params = self._get_params()
            params.update({"ticker": symbol})
            _baseurl = "http://globalquote.morningstar.com/globalcomponent/RealtimeHistoricalStockData.ashx"

            try:
                resp = requests.get(_baseurl, params=params)
            except Exception:
                if self.retry_count == 0:
                    print("skipping symbol %s: number of retries exceeded." % symbol)
                    pass
                else:
                    if symbol in failed:
                        pass
                    else:
                        print("adding %s to retry list" % symbol)
                        failed.append(symbol)
            else:
                if resp.status_code == requests.codes.ok:
                    jsdata = self._restruct_json(symbol=symbol, jsondata=resp.json())
                    symbol_data.extend(jsdata)
                    passed.append(symbol)
            time.sleep(self.pause)
        if len(failed) > 0 and self.retry_count >0:
            self._dl_mult_symbols(symbols=failed)
            self.retry_count -= 1
        else:
            self.retry_count = 0

        if self.retry_count == 0 and len(failed) > 0:
            warn(SymbolWarning("The following symbols were excluded do to http request errors: \n %s" % failed))

        symbols_df = DataFrame(symbol_data)
        dfx = symbols_df.set_index(["symbol", "date"])
        return dfx.to_panel().swapaxes(axis1="items", axis2="minor")

    @staticmethod
    def _convert_index2date(enddate, indexvals):
        i = 0
        while i < len(indexvals):
            days = indexvals[len(indexvals) - 1] - indexvals[i]
            d = enddate - timedelta(days=days)
            i += 1
            yield d.strftime("%Y-%m-%d")

    # @staticmethod
    # def _adjust_close_price(price, event_type, event_value):
    #     if event_type is "split":
    #         e, s = event_value.split(":")
    #         adj=(price * int(s))/e
    #     elif event_type is "dividend":
    #         adj = price - float(event_value)
    #     else:
    #         raise ValueError("Invalid event_type")
    #     return adj

    def _restruct_json(self, symbol, jsondata):

        divdata = jsondata["DividendData"]

        pricedata = jsondata["PriceDataList"][0]["Datapoints"]
        dateidx = jsondata["PriceDataList"][0]["DateIndexs"]
        volumes = jsondata["VolumeList"]["Datapoints"]

        date_ = self._convert_index2date(enddate=self.end, indexvals=dateidx)
        barss = []
        for p in range(len(pricedata)):
            bar = pricedata[p]
            d = next(date_)
            bardict = {
                "symbol": symbol, "date": d, "open": bar[0], "high": bar[1], "low": bar[2],
                "close": bar[3]
            }
            if len(divdata) == 0:
                pass
            else:
                events = [x for x in divdata if
                          (datetime.strptime(x["Date"], "%Y-%m-%d") - datetime.strptime(d, "%Y-%m-%d")).days == 0]
                for e in events:
                    if e["Type"].find("Div") > -1 and self.incl_dividends is True:
                        bardict.update({"is_dividend": e["Desc"].replace(e["Type"], "")})
                    elif e["Type"].find("Split") > -1 and self.incl_splits is True:
                        bardict.update({"is_split": e["Desc"].replace(e["Type"], "")})
                    else:
                        pass
            if self.incl_vol is True:
                bardict.update({"volume": volumes[p]*1000000})
            else: pass


            barss.append(bardict)
        return barss

    def read(self):
        return self._dl_mult_symbols(symbols=self.symbols)

