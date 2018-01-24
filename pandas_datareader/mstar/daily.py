import time
from datetime import datetime
from warnings import warn

import requests
from pandas import DataFrame

from pandas_datareader._utils import SymbolWarning
from pandas_datareader.base import _BaseReader
import pandas as pd


class MorningstarDailyReader(_BaseReader):
    """
    Read daily data from Morningstar

    Parameters
    ----------
    symbols : {str, List[str]}
        String symbol of like of symbols
    start : string, (defaults to '1/1/2010')
        Starting date, timestamp. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : string, (defaults to today)
        Ending date, timestamp. Same format as starting date.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    freq : {str, None}
        Frequency to use in select readers
    incl_splits : bool, optional
        Include splits in data
    incl_dividends : bool,, optional
        Include divdends in data
    incl_volume : bool, optional
        Include volume in data
    currency : str, optional
        Currency to use for data
    interval : str, optional
        Sampling interval to use for downloaded data

    Notes
    -----
    See `Morningstar <http://www.morningstar.com>`__
    """

    def __init__(self, symbols, start=None, end=None, retry_count=3,
                 pause=0.1, timeout=30, session=None, freq=None,
                 incl_splits=False, incl_dividends=False, incl_volume=True,
                 currency='usd', interval='d'):
        super(MorningstarDailyReader, self).__init__(symbols, start, end,
                                                     retry_count, pause,
                                                     timeout, session, freq)

        self.incl_splits = incl_splits
        self.incl_dividends = incl_dividends
        self.incl_vol = incl_volume
        self.currency = currency
        self.interval = interval

        self._symbol_data_cache = []

    def _url_params(self):
        if self.interval not in ['d', 'wk', 'mo', 'm', 'w']:
            raise ValueError("Invalid interval: valid values are  'd', 'wk' "
                             "and 'mo'. 'm' and 'w' have been implemented for "
                             "backward compatibility")
        elif self.interval in ['m', 'mo']:
            self.interval = 'm'
        elif self.interval in ['w', 'wk']:
            self.interval = 'w'

        if self.currency != "usd":
            warn("Caution! There is no explicit check for a valid currency "
                 "acronym\nIf an error is encountered consider changing this "
                 "value.")

        p = {"range": "|".join(
            [self.start.strftime("%Y-%m-%d"), self.end.strftime("%Y-%m-%d")]),
            "f": self.interval, "curry": self.currency,
            "dtype": "his", "showVol": "true",
            "hasF": "true", "isD": "true", "isS": "true",
            "ProdCode": "DIRECT"}

        return p

    @property
    def url(self):
        """API URL"""
        return "http://globalquote.morningstar.com/globalcomponent/" \
               "RealtimeHistoricalStockData.ashx"

    def _get_crumb(self, *args):
        """Not required """
        pass

    def _dl_mult_symbols(self, symbols):
        failed = []
        symbol_data = []
        for symbol in symbols:

            params = self._url_params()
            params.update({"ticker": symbol})

            try:
                resp = requests.get(self.url, params=params)
            except Exception:
                if symbol not in failed:
                    if self.retry_count == 0:
                        warn("skipping symbol %s: number of retries "
                             "exceeded." % symbol)
                        pass
                    else:
                        print("adding %s to retry list" % symbol)
                        failed.append(symbol)
            else:
                if resp.status_code == requests.codes.ok:
                    jsondata = resp.json()
                    if jsondata is None:
                        failed.append(symbol)
                        continue
                    jsdata = self._restruct_json(symbol=symbol,
                                                 jsondata=jsondata)
                    symbol_data.extend(jsdata)
                else:
                    raise Exception("Request Error!: %s : %s" % (
                        resp.status_code, resp.reason))

            time.sleep(self.pause)

        if len(failed) > 0 and self.retry_count > 0:
            # TODO: This appears to do nothing since
            # TODO: successful symbols are not added to
            self._dl_mult_symbols(symbols=failed)
            self.retry_count -= 1
        else:
            self.retry_count = 0

        if not symbol_data:
            raise ValueError('All symbols were invalid')
        elif self.retry_count == 0 and len(failed) > 0:
            warn("The following symbols were excluded do to http "
                 "request errors: \n %s" % failed, SymbolWarning)

        symbols_df = DataFrame(data=symbol_data)
        dfx = symbols_df.set_index(["Symbol", "Date"])
        return dfx

    @staticmethod
    def _convert_index2date(indexvals):
        base = pd.to_datetime('1900-1-1')
        return [base + pd.to_timedelta(iv, unit='d') for iv in indexvals]

    def _restruct_json(self, symbol, jsondata):
        if jsondata is None:
            return
        divdata = jsondata["DividendData"]

        pricedata = jsondata["PriceDataList"][0]["Datapoints"]
        dateidx = jsondata["PriceDataList"][0]["DateIndexs"]
        volumes = jsondata["VolumeList"]["Datapoints"]

        dates = self._convert_index2date(indexvals=dateidx)
        barss = []
        for p in range(len(pricedata)):
            bar = pricedata[p]
            d = dates[p]
            bardict = {
                "Symbol": symbol, "Date": d, "Close": bar[0], "High": bar[1],
                "Low": bar[2], "Open": bar[3]
            }
            if len(divdata) == 0:
                pass
            else:
                events = []
                for x in divdata:
                    delta = (datetime.strptime(x["Date"], "%Y-%m-%d")
                             - d.to_pydatetime())
                    if delta.days == 0:
                        events.append(x)
                for e in events:
                    if self.incl_dividends and e["Type"].find("Div") > -1:
                        val = e["Desc"].replace(e["Type"], "")
                        bardict.update({"isDividend": val})
                    elif (self.incl_splits is True and
                          e["Type"].find("Split") > -1):
                        val = e["Desc"].replace(e["Type"], "")
                        bardict.update({"isSplit": val})
                    else:
                        pass
            if self.incl_vol is True:
                bardict.update({"Volume": int(volumes[p] * 1000000)})
            else:
                pass

            barss.append(bardict)
        return barss

    def read(self):
        """Read data"""
        if isinstance(self.symbols, str):
            symbols = [self.symbols]
        else:
            symbols = self.symbols

        is_str = False
        try:
            is_str = all(map(lambda v: isinstance(v, str), symbols))
        except Exception:
            pass

        if not is_str:
            raise TypeError("symbols must be iterable or string and not "
                            "type %s" % type(self.symbols))

        df = self._dl_mult_symbols(symbols=symbols)
        if len(df.index.levels[0]) == 0:
            raise ValueError("None of the provided symbols were valid")
        else:
            return df
