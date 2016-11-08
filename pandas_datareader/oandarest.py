import pandas as pd
import json as json
import datetime as datetime
import time
import re

from oandapyV20 import API
import oandapyV20.endpoints.instruments as instruments

from pandas.compat import StringIO, string_types
from ._utils import _init_session, _sanitize_dates
from pandas.tseries.offsets import DateOffset

from pandas_datareader.base import _BaseReader

class OANDARestHistoricalInstrumentReader(_BaseReader):
    """
        Historical Currency Pair Reader using  OANDA's REST API.
        See details at http://developer.oanda.com/rest-live-v20/instrument-ep/
        symbols : Dict of strings. 
            Each string is a currency pair with format BASE_QUOTE. Eg: ["EUR_USD", "JPY_USD"]
        symbolsTypes: Dict of strings.
            Each string represent the type of instrument to fetch data for. Eg: For symbols=["EUR_USD", "EUR_JPY"] then symbolsTypes=["currency", "currency"]
            Valid values: currency
        start: string
            Date to begin fetching curerncy pair, in RFC3339 ("%Y-%m-%dT%H:%M:%SZ)  # Eg: "2014-03-21T17:41:00Z"
        end: string
            Date to end fetching curerncy pair, in RFC3339 ("%Y-%m-%dT%H:%M:%SZ)  # Eg: "2014-03-21T17:41:00Z"
        freq: string or Pandas's DateOffset
            Frequency or periodicity of the candlesticks to be retrieved
            Valid values are the following Panda's Offset Aliases (http://pandas.pydata.org/pandas-docs/stable/timeseries.html):
                    5S  ->  5 second candlesticks, minute alignment
                    10S -> 10 second candlesticks, minute alignment
                    15S -> 15 second candlesticks, minute alignment
                    30S -> 30 second candlesticks, minute alignment
                    1T,1min  ->  1 minute candlesticks, minute alignment
                    2T,2min  ->  2 minute candlesticks, hour alignment
                    4T,4min  ->  4 minute candlesticks, hour alignment
                    5T,5min  ->  5 minute candlesticks, hour alignment
                    15T,15min -> 15 minute candlesticks, hour alignment
                    30T,30min -> 30 minute candlesticks, hour alignment
                    1H  ->  1 hour candlesticks, hour alignment
                    2H  ->  1 hour candlesticks, day alignment
                    3H  ->  3 hour candlesticks, day alignment
                    4H  ->  4 hour candlesticks, day alignment
                    6H  ->  6 hour candlesticks, day alignment
                    8H  ->  8 hour candlesticks, day alignment
                    12H -> 12 hour candlesticks, day alignment
                    1D  ->  1 day candlesticks, day alignment
                    1W  ->  1 week candlesticks, aligned to start of week
                    1M  ->  1 month candlesticks, aligned to first day of the month
                    See OANDA REST v20 for updated list
            Default: See DEFAULT_FREQUENCY
        candleFormat: string
            Candlesticks representation
            Valid values: M,B,A
                M for midpoint
                B for Bid
                A for Ask
            Default: See DEFAULT_CANDLE_FORMAT
        access_credential: Dict of strings
            Credential to query the api
            credential["accountType"]="practise". Mandatory. Valid values: practice, live
            credential["apiToken"]="Your OANDA API token". Mandatory. Valid value: See your OANDA Account's API Token
    """
    DEFAULT_FREQUENCY="5S"
    DEFAULT_CANDLE_FORMAT="M"
    SUPPORTED_OFFSET_ALIASES = {
            "5S":"S5",
            "10S":"S10" ,
            "15S":"S15",
            "30S":"S30",
            "T":"M1",
            "1T":"M1",
            "2T":"M2",
            "4T":"M4",
            "5T":"M5",
            "15T":"M15",
            "30T":"M30",
            "H":"H1",
            "1H":"H1",
            "2H":"H2",
            "3H":"H3",
            "4H":"H4",
            "6H":"H6",
            "8H":"H8",
            "12H":"H12",
            "1D":"D",
            "1W":"W",
            "1M":"M"
            }


    def __init__(self,symbols, symbolsTypes=None,
                start=None, end=None,
                freq=None, candleFormat=None,
                session=None,
                access_credential=None):
        _BaseReader.__init__(self, symbols, start=start, end=end, session=session)

        if symbols is None:
            self.symbols = ["EUR_USD"]

        self.symbolsTypes = symbolsTypes
        if symbolsTypes is None:
            self.symbolsTypes = ["currency"];

        self.freq = freq
        if freq is None:
            self.freq=OANDARestHistoricalInstrumentReader.DEFAULT_FREQUENCY

        self.candleFormat = candleFormat
        if candleFormat is None:
            self.candleFormat=OANDARestHistoricalInstrumentReader.DEFAULT_CANDLE_FORMAT

        self.access_credential=access_credential
        if access_credential is None:
            self.access_credential = {}

        if 'accountType' not in access_credential:
            self.access_credential['accountType'] = "practice"

        if 'apiToken' not in access_credential:
            self.access_credential['apiToken'] = ""


    def read(self):
        dfs = {}

        #print(str(self.symbols))

        for (index,symbol) in enumerate(self.symbols):
            symbolsType = self.symbolsTypes[index]
            if symbolsType is "currency":
                (base_currency, quote_currency) = self._split_currency_pair(symbol)
                df = self._read_historical_currencypair_rates(
                    self.start, self.end,
                    quote_currency=quote_currency, base_currency=base_currency,
                    freq=self.freq,
                    candleFormat=self.candleFormat,
                    access_credential=self.access_credential,
                    session=self.session
                    )
                df.name = symbol
                dfs[symbol] = df
            else:
                raise Exception("Symbol Type; %s not supported" % (symbolsType))
        
        return pd.Panel(dfs)

    def _read_historical_currencypair_rates(self, start, end, freq=None,
                                            quote_currency=None, base_currency=None, 
                                            candleFormat=None, reversed=False, 
                                            access_credential=None, session=None):
        session = _init_session(session)
        start, end = _sanitize_dates(start, end)

        if base_currency is None:
            base_currency = "EUR"
        if quote_currency is None:
            quote_currency = "USD"

        currencyPair = "%s_%s" % (base_currency, quote_currency)

        if access_credential is None:
            raise Exception('No access_crendtial provided. Historical data cannot be fetched')

        credential = access_credential
        #print(credential)

        oanda = API(access_token=credential['apiToken'],
                    environment=credential['accountType'])

        current_start = start
        current_end = end
        current_duration = current_end - current_start
        includeCandleOnStart = True
        dfs = []

        while current_start < end:
            current_end = current_start + current_duration

            if current_end > end:
                current_end = end

            current_start_timestamp = time.mktime(current_start.timetuple())
            current_end_timestamp = time.mktime(current_end.timetuple())
            rfc3339 = "%Y-%m-%dT%H:%M:%SZ"  # Eg: 2014-03-21T17:41:00Z
            current_start_rfc3339 = current_start.strftime(rfc3339)
            current_end_rfc3339 = current_end.strftime(rfc3339)

            if isinstance(freq, DateOffset):
                offsetString = freq.freqstr
            else:
                offsetString = freq

            if offsetString in OANDARestHistoricalInstrumentReader.SUPPORTED_OFFSET_ALIASES:
                granularity = OANDARestHistoricalInstrumentReader.SUPPORTED_OFFSET_ALIASES[offsetString]
            else:
                granularity = offsetString

            params = {
                "granularity": granularity,
                "from": current_start_rfc3339,
                "to": current_end_rfc3339,
                "price": candleFormat,
                "smooth": "false",
                "includeFirst": "true" if includeCandleOnStart else "false",
                "dailyAlignment" : "17",
                "alignmentTimezone" : "America/New_York",
                "weeklyAlignment" : "Friday"
            }

            #print(params)

            try:
                request = instruments.InstrumentsCandles(
                    instrument=currencyPair,
                    params=params)

                response = oanda.request(request)
                current_start = current_end
                includeCandleOnStart = False
            except Exception as error:
                isExceedResultsLimitError = re.findall("The results returned satisfying the query exceeds the maximum size", str(error))
                isExceedResultsLimitError = True if len(isExceedResultsLimitError) else False

                if isExceedResultsLimitError:
                    # Problem: OANDA caps returned range to 5000 results max
                    # Solution: Reduce requested date interval to return less than 5000 results
                    current_duration /= 2
                    continue
                else:
                    print("ERROR OANDA: "+ str(error))
                    print(str(error))
                    raise error

            #print(response)

            if not response:
                continue

            candles = response['candles']

            if not candles:
                continue

            #print(candles)

            ohlc = ['o','h','l','c']
            df = pd.io.json.json_normalize(
                    candles,
                    meta=[ 'time', 'volume', 'complete', ['mid', ohlc], ['ask', ohlc], ['bid', ohlc]]
                    )
            df['time'] = pd.to_datetime(df['time'])

            with pd.option_context('display.max_columns', None):
                pass #print(df)

            dfs.append(df)

        df = pd.concat(dfs);

        df.rename(  inplace=True,
                    index=str,
                    columns={
                        "time": "date",
                        "mid.o": "mid.open",
                        "mid.h":"mid.high",
                        "mid.l":"mid.low",
                        "mid.c":"mid.close",
                        "ask.o":"ask.open",
                        "ask.h":"ask.high",
                        "ask.l":"ask.low",
                        "ask.c":"ask.close",
                        "bid.o":"bid.open",
                        "bid.h":"bid.high",
                        "bid.l":"bid.low",
                        "bid.c":"bid.close",
                        })

        df = df.set_index('date')

        # Sort by date as OANDA REST v20 provides no guarantee
        # returned candles are sorted
        df.sort_index(axis=0, level='date', ascending=True, inplace=True)

        with pd.option_context('display.max_columns', None):
            pass #print(df)

        return df

    def _reverse_pair(s, sep="_"):
        lst = s.split(sep)
        return sep.join([lst[1], lst[0]])

    def _split_currency_pair(self, s, sep="_"):
        lst = s.split(sep)
        return (lst[0], lst[1])

