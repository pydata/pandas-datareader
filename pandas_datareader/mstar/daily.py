
import time
from datetime import datetime, timedelta
from warnings import warn

import requests
from pandas import DataFrame

from pandas_datareader._utils import (SymbolWarning, _sanitize_dates)


class MorningstarDailyReader(object):


    def __init__(self, start=None, end=datetime.today().strftime("%Y-%m-%d"), *args, **kwargs):

        self.start, self.end = _sanitize_dates(start, end)

        self.retry_count = kwargs.get("retry_count", 3)
        self.pause = kwargs.get("pause", 0.001)
        self.timeout = kwargs.get("timeout", 30)
        self.session = kwargs.get("session", requests.session())

        self.incl_splits = kwargs.get("incl_splits", False)
        self.incl_dividends = kwargs.get("incl_dividends", False)
        self.incl_vol = kwargs.get("incl_volume", True)
        self.currency = kwargs.get("currency", "usd")
        self.interval = kwargs.get("interval", "d")

        self.symbols = kwargs.get("symbols")

        self._symbol_data_cache = []


    def _url_params(self):
        if self.interval not in ['d', 'wk', 'mo', 'm', 'w']:
            raise ValueError("Invalid interval: valid values are  'd', 'wk' and 'mo'. 'm' and 'w' have been implemented for "  # noqa
                             "backward compatibility")  # noqa
        elif self.interval in ['m', 'mo']:
            self.interval = 'm'
        elif self.interval in ['w', 'wk']:
            self.interval = 'w'

        if self.currency != "usd":
            warn("Caution! There is no explicit check for a valid currency acronym\n"
                 "If an error is encountered consider changing this value.")

        p = {"range": "|".join([self.start.strftime("%Y-%m-%d"), self.end.strftime("%Y-%m-%d")]),
             "f": self.interval, "curry": self.currency,
             "dtype": "his", "showVol": "true",
             "hasF": "true", "isD": "true", "isS": "true", "ProdCode": "DIRECT"}

        return p

    def _check_dates(self, *dates):
        if dates[0] > dates[1]:
            raise ValueError("Invalid start & end date! Start date cannot be later than end date.")
        else:
            return dates[0], dates[1]

    def _dl_mult_symbols(self, symbols):
        failed = []
        symbol_data = []
        for symbol in symbols:

            params = self._url_params()
            params.update({"ticker": symbol})
            _baseurl = "http://globalquote.morningstar.com/globalcomponent/RealtimeHistoricalStockData.ashx"

            try:
                resp = requests.get(_baseurl, params=params)
            except Exception:
                if symbol not in failed:
                    if self.retry_count == 0:
                        print("skipping symbol %s: number of retries exceeded." % symbol)
                        pass
                    else:
                        print("adding %s to retry list" % symbol)
                        failed.append(symbol)
            else:
                if resp.status_code == requests.codes.ok:
                    jsdata = self._restruct_json(symbol=symbol, jsondata=resp.json())
                    symbol_data.extend(jsdata)
                else:
                    raise Exception("Request Error!: %s : %s" % (resp.status_code, resp.reason))

            time.sleep(self.pause)

        if len(failed) > 0 and self.retry_count > 0:
            self._dl_mult_symbols(symbols=failed)
            self.retry_count -= 1
        else:
            self.retry_count = 0

        if self.retry_count == 0 and len(failed) > 0:
            warn(SymbolWarning("The following symbols were excluded do to http request errors: \n %s" % failed))

        symbols_df = DataFrame(data=symbol_data)
        dfx = symbols_df.set_index(["Symbol", "Date"])
        return dfx

    @staticmethod
    def _convert_index2date(enddate, indexvals):
        i = 0
        while i < len(indexvals):
            days = indexvals[len(indexvals) - 1] - indexvals[i]
            d = enddate - timedelta(days=days)
            i += 1
            yield d.strftime("%Y-%m-%d")

    #
    # def _adjust_close_price(price, event_type, event_value): #noqa
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
                "Symbol": symbol, "Date": d, "Open": bar[0], "High": bar[1], "Low": bar[2],
                "Close": bar[3]
            }
            if len(divdata) == 0:
                pass
            else:
                events = [x for x in divdata if
                          (datetime.strptime(x["Date"], "%Y-%m-%d") - datetime.strptime(d, "%Y-%m-%d")).days == 0]
                for e in events:
                    if e["Type"].find("Div") > -1 and self.incl_dividends is True:
                        bardict.update({"isDividend": e["Desc"].replace(e["Type"], "")})
                    elif e["Type"].find("Split") > -1 and self.incl_splits is True:
                        bardict.update({"isSplit": e["Desc"].replace(e["Type"], "")})
                    else:
                        pass
            if self.incl_vol is True:
                bardict.update({"Volume": int(volumes[p]*1000000)})
            else: pass


            barss.append(bardict)
        return barss

    def read(self):
        if type(self.symbols) is str:
            df = self._dl_mult_symbols(symbols=[self.symbols])
            if len(df.Close.keys()) == 0:
                raise IndexError("None of the provided symbols were valid")
            else:
                return df
        elif hasattr(self.symbols, "__iter__"):
            df = self._dl_mult_symbols(symbols=self.symbols)
            if len(df.Close.keys()) == 0:
                raise IndexError("None of the provided symbols were valid")
            else:
                return df
        else:
            raise TypeError("symbols must be iterable or string and not type %s" % type(self.symbols))




