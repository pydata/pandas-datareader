import pandas as pd
import json as json
import datetime as datetime
import time
import re
import oandapy
from pandas.compat import StringIO, string_types
from ._utils import _init_session, _sanitize_dates

from pandas_datareader.base import _BaseReader

class OANDARestHistoricalInstrumentReader(_BaseReader):
    """
        Historical Currency Pair Reader using  OANDA's REST API.
        See details at http://developer.oanda.com/rest-live/rates/#retrieveInstrumentHistory
        symbols : Dict of strings. 
            Each string is a currency pair with format BASE_QUOTE. Eg: ["EUR_USD", "JPY_USD"]
        symbolsTypes: Dict of strings.
            Each string represent the type of instrument to fetch data for. Eg: For symbols=["EUR_USD", "EUR_JPY"] then symbolsTypes=["currency", "currency"]
            Valid values: currency
        start: string
            Date to begin fetching curerncy pair, in RFC3339 ("%Y-%m-%dT%H:%M:%SZ)  # Eg: "2014-03-21T17:41:00Z"
        end: string
            Date to end fetching curerncy pair, in RFC3339 ("%Y-%m-%dT%H:%M:%SZ)  # Eg: "2014-03-21T17:41:00Z"
        granularity: string
            Time range for each candlestick
            Default: See DEFAULT_GRANULARITY
        candleFormat: string
            Candlesticks representation
            Default: See DEFAULT_CANDLE_FORMAT
        access_credential: Dict of strings
            Credential to query the api
            credential["accountType"]="practise". Mandatory. Valid values: practice, live
            credential["apiToken"]="Your OANDA API token". Mandatory. Valid value: See your OANDA Account's API Token
    """
    DEFAULT_GRANULARITY="S5"
    DEFAULT_CANDLE_FORMAT="midpoint"

    def __init__(self,symbols, symbolsTypes=None,
                start=None, end=None,
                granularity=None, candleFormat=None, 
                session=None,
                access_credential=None):
        _BaseReader.__init__(self, symbols, start=start, end=end, session=session)

        if symbols is None:
            self.symbols = ["EUR_USD"]

        self.symbolsTypes = symbolsTypes
        if symbolsTypes is None:
            self.symbolsTypes = ["currency"];

        self.granularity = granularity
        if granularity is None:
            self.granularity=OANDARestHistoricalInstrumentReader.DEFAULT_GRANULARITY

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
                    granularity=self.granularity,
                    candleFormat=self.candleFormat,
                    access_credential=self.access_credential,
                    session=self.session
                    )
                df.name = symbol
                dfs[symbol] = df
            else:
                raise Exception("Symbol Type; %s not supported" % (symbolsType))
        
        return pd.Panel(dfs)

    def _read_historical_currencypair_rates(self, start, end, granularity=None, 
                                            quote_currency=None, base_currency=None, 
                                            candleFormat=None, reversed=False, 
                                            access_credential=None, session=None):
        """
        access_credential : dict of strings
            Format {
                "accountType": "practise OR live (mandatory)",
                "accountVersion": "0", // 0 for legacy REST API account (supported), v20 for REST-v20 (unsupported for now) 
                "apiToken"; "Private API token (mandatory)"}
        other parameter: * 
            See http://developer.oanda.com/rest-live/rates/#retrieveInstrumentHistory 
        """
        session = _init_session(session)
        start, end = _sanitize_dates(start, end)

        if base_currency is None:
            base_currency = "EUR"
        if quote_currency is None:
            quote_currency = "USD"

        currencyPair = "%s_%s" % (base_currency, quote_currency)

        if candleFormat is None:
            candleFormat = "midpoint"

        if access_credential is None:
            raise Exception('No access_crendtial provided. Historical data cannot be fetched')

        credential = access_credential
        #print(credential)

        oanda = oandapy.API(environment=credential['accountType'], 
                            access_token=credential['apiToken'])

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

            #print("%s(%s) -> %s(%s)" % (current_start, current_start_timestamp, current_end, current_end_timestamp))
            #print(type(current_start))

            params = {
                "instrument": currencyPair,
                "granularity": granularity,
                "start": current_start_rfc3339,
                "end": current_end_rfc3339,
                "candleFormat": candleFormat,
                "includeFirst": "true" if includeCandleOnStart else "false",
                "dailyAlignment" : "17",
                "alignmentTimezone" : "America/New_York",
                "weeklyAlignment" : "Friday"
            }

            #print(params)

            try:
                response = oanda.get_history(**params)
                current_start = current_end
                includeCandleOnStart = False
            except Exception as error:
                #print(str(error))
                error_code = re.findall("error code ([0-9]+)", str(error))
                if not error_code:
                    print(str(error))
                    raise error
                elif error_code[0] == "36": 
                    # Problem: OANDA caps returned range to 5000 results max
                    # Solution: Reduce requested date interval to return less than 5000 results
                    current_duration /= 2 
                    continue 
                else:
                    print(str(error))
                    raise error

            #print(response)

            if not response:
                continue

            candles = response['candles'] 
            candlesJSON = json.dumps(candles)

            #print(candlesJSON)

            df = pd.read_json(candlesJSON, typ='frame', orient='records', convert_dates=['time'])

            with pd.option_context('display.max_columns', None):
                pass #print(df)

            dfs.append(df)

        df = pd.concat(dfs);
        
        df = df.rename(columns={
            "time": "date",
        })
        df = df.set_index('date')

        # FIXME:Sort by date
        #df.sort(['date'], ascending=True)

        #print(df)

        #df = df[::-1]
        #if reversed:
        #    df.columns = pd.Index(df.columns.map(_reverse_pair))
        #    df = 1 / df
        #if len(base_currency) == 1:
        #    return df.iloc[:, 0]
        #else:
        #    return df

        return df

    def _reverse_pair(s, sep="_"):
        lst = s.split(sep)
        return sep.join([lst[1], lst[0]])

    def _split_currency_pair(self, s, sep="_"):
        lst = s.split(sep)
        return (lst[0], lst[1])
        

