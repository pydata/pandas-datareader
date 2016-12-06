import os

import pandas as pd
import re

from oandapyV20 import API
import oandapyV20.endpoints.instruments as instruments

from ._utils import _init_session, _sanitize_dates
from pandas.tseries.offsets import DateOffset
from pandas.compat import OrderedDict

from pandas_datareader.base import _BaseReader


class OANDARestHistoricalInstrumentReader(_BaseReader):
    """
        Historical Currency Pair Reader using  OANDA's REST v20 API.
        See details at http://developer.oanda.com/rest-live-v20/instrument-ep/
        symbols : string or Dict of strings.
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
                    T,1min  ->  1 minute candlesticks, minute alignment
                    2T,2min  ->  2 minute candlesticks, hour alignment
                    4T,4min  ->  4 minute candlesticks, hour alignment
                    5T,5min  ->  5 minute candlesticks, hour alignment
                    15T,15min -> 15 minute candlesticks, hour alignment
                    30T,30min -> 30 minute candlesticks, hour alignment
                    H  ->  1 hour candlesticks, hour alignment
                    2H  ->  1 hour candlesticks, day alignment
                    3H  ->  3 hour candlesticks, day alignment
                    4H  ->  4 hour candlesticks, day alignment
                    6H  ->  6 hour candlesticks, day alignment
                    8H  ->  8 hour candlesticks, day alignment
                    12H -> 12 hour candlesticks, day alignment
                    D  ->  1 day candlesticks, day alignment
                    W  ->  1 week candlesticks, aligned to start of week
                    M  ->  1 month candlesticks, aligned to first day of the month
                    See OANDA REST v20 for updated list
            Default: See DEFAULT_FREQUENCY
        candleFormat: string
            Candlesticks representations. Eg: BA to get bid and ask values
            Valid values:
                M for Midpoint
                B for Bid
                A for Ask
            Default: See DEFAULT_CANDLE_FORMAT
        access_credential: Dict of strings
            Credential to query the api
            credential["accountType"]="practise". Mandatory. Valid values: practice, live
            credential["apiToken"]="Your OANDA API token". Mandatory. Valid value: See your OANDA Account's API Token

        Returns:
        Panel with currency pairs requested. Each dataframe represents a currency pair and is MultiIndexed enabled.
        Each dataframe data point can be addressed as is (assuming Ask data were requested):

        Example:
        pn = OANDARestHistoricalInstrumentReader(
            symbols=["EUR_USD"],
            start="2014-03-19T09:00:00Z",
            end="2014-03-20T9:00:00Z",
            freq="5T",
            candleFormat="MBA",
            access_credential="See access credential for format"
        ).read()

        price = pn["Ask"]["Close"]["EUR_USD"]["2014-03-19 09:05:00"]
        price = pn["Ask"]["Close","2014-03-19 09:05:00","EUR_USD"]
        price = pn.loc[("Ask","Close"),"2014-03-19 09:05:00","EUR_USD"]
        price = pn["Ask"]["Close"]["EUR_USD"]["2014-03-19 09:05:00"]
    """

    DEFAULT_FREQUENCY = "5S"
    DEFAULT_CANDLE_FORMAT = "AMB"
    SUPPORTED_OFFSET_ALIASES = {
        "5S": "S5",
        "10S": "S10",
        "15S": "S15",
        "30S": "S30",
        "T": "M1",
        "2T": "M2",
        "4T": "M4",
        "5T": "M5",
        "15T": "M15",
        "30T": "M30",
        "H": "H1",
        "2H": "H2",
        "3H": "H3",
        "4H": "H4",
        "6H": "H6",
        "8H": "H8",
        "12H": "H12",
        "1M": "M"
    }

    def __init__(self, symbols, symbolsTypes=None,
                 start=None, end=None,
                 freq=None, candleFormat=None,
                 session=None,
                 access_credential=None,
                 reader_compatible=None):
        _BaseReader.__init__(self, symbols, start=start,
                             end=end, session=session)

        self.reader_compatible = reader_compatible
        if reader_compatible is None:
            self.reader_compatible = False

        self.symbols = symbols
        if symbols is None:
            self.symbols = ["EUR_USD"]

        if type(symbols) is str:
            self.symbols = [symbols]

        self.symbolsTypes = symbolsTypes
        if symbolsTypes is None:
            self.symbolsTypes = ["currency"]

        if len(self.symbols) != len(self.symbolsTypes):
            self.symbolsTypes = ["currency" for x in self.symbols]

        self.freq = freq
        if freq is None:
            self.freq = OANDARestHistoricalInstrumentReader.DEFAULT_FREQUENCY

        self.candleFormat = candleFormat
        if candleFormat is None:
            self.candleFormat = OANDARestHistoricalInstrumentReader.DEFAULT_CANDLE_FORMAT

        self.access_credential = access_credential
        if self.access_credential is None:
            self.access_credential = {}

        if 'accountType' not in self.access_credential:
            self.access_credential['accountType'] = "practice"

        if 'apiToken' not in self.access_credential:
            self.access_credential['apiToken'] = os.getenv('OANDA_API_TOKEN')
            if self.access_credential['apiToken'] is None:
                raise ValueError(
                    """Please provide an OANDA API token or set the OANDA_API_TOKEN environment variable\n
                    If you do not have an API key, you can get one here: http://developer.oanda.com/rest-live/authentication/""")

    def read(self):
        dfs = OrderedDict()

        # print(str(self.symbols))

        for (index, symbol) in enumerate(self.symbols):
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
                raise Exception("Symbol Type; %s not supported" %
                                (symbolsType))

        if self.reader_compatible and len(dfs) == 1:
            key = list(dfs.keys())[0]
            return dfs[key]

        pn = pd.Panel(dfs)
        pn.axes[0].name = "Currency"

        pn = pn.transpose(2, 1, 0)

        return pn

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
            raise Exception(
                'No access_crendtial provided. Historical data cannot be fetched')

        credential = access_credential
        # print(credential)

        oanda = API(access_token=credential['apiToken'],
                    environment=credential['accountType'])

        current_start = start
        current_end = end
        current_duration = current_end - current_start
        includeCandleOnStart = True

        df = None

        OANDA_OPEN = 'o'
        OANDA_HIGH = 'h'
        OANDA_LOW = 'l'
        OANDA_CLOSE = 'c'
        OANDA_MID = 'mid'
        OANDA_ASK = 'ask'
        OANDA_BID = 'bid'
        OANDA_OHLC = [OANDA_OPEN, OANDA_HIGH, OANDA_LOW, OANDA_CLOSE]
        OANDA_TIME = 'time'
        OANDA_VOLUME = 'volume'
        OANDA_COMPLETE = 'complete'
        OANDA_MID_OPEN = 'mid.o'
        OANDA_MID_VOLUME = 'mid.volume'
        OANDA_MID_COMPLETE = 'mid.complete'
        OANDA_ASK_OPEN = 'ask.o'
        OANDA_ASK_VOLUME = 'ask.volume'
        OANDA_ASK_COMPLETE = 'ask.complete'
        OANDA_BID_OPEN = 'bid.o'
        OANDA_BID_VOLUME = 'bid.volume'
        OANDA_BID_COMPLETE = 'bid.complete'

        DATAFRAME_DATE = 'Date'
        DATAFRAME_MID = 'Mid'
        DATAFRAME_ASK = 'Ask'
        DATAFRAME_BID = 'Bid'
        DATAFRAME_OPEN = 'Open'
        DATAFRAME_HIGH = 'High'
        DATAFRAME_LOW = 'Low'
        DATAFRAME_CLOSE = 'Close'
        DATAFRAME_VOLUME = 'Volume'
        DATAFRAME_COMPLETE = 'Complete'

        while current_start < end:
            current_end = current_start + current_duration

            if current_end > end:
                current_end = end

            # current_start_timestamp = time.mktime(current_start.timetuple())
            # current_end_timestamp = time.mktime(current_end.timetuple())
            rfc3339 = "%Y-%m-%dT%H:%M:%SZ"  # Eg: 2014-03-21T17:41:00Z
            current_start_rfc3339 = current_start.strftime(rfc3339)
            current_end_rfc3339 = current_end.strftime(rfc3339)

            if isinstance(freq, DateOffset):
                offsetString = freq.freqstr
            else:
                offsetString = freq

            if offsetString in OANDARestHistoricalInstrumentReader.SUPPORTED_OFFSET_ALIASES:
                granularity = OANDARestHistoricalInstrumentReader.SUPPORTED_OFFSET_ALIASES[
                    offsetString]
            else:
                granularity = offsetString

            params = {
                "granularity": granularity,
                "from": current_start_rfc3339,
                "to": current_end_rfc3339,
                "price": candleFormat,
                "smooth": "false",
                "includeFirst": "true" if includeCandleOnStart else "false",
                "dailyAlignment": "17",
                "alignmentTimezone": "America/New_York",
                "weeklyAlignment": "Friday"
            }

            # print(params)

            try:
                request = instruments.InstrumentsCandles(
                    instrument=currencyPair,
                    params=params)

                response = oanda.request(request)
                current_start = current_end
                includeCandleOnStart = False
            except Exception as error:
                isExceedResultsLimitError = re.findall(
                    "The results returned satisfying the query exceeds the maximum size", str(error))
                isExceedResultsLimitError = True if len(
                    isExceedResultsLimitError) else False

                if isExceedResultsLimitError:
                    # Problem: OANDA caps returned range to 5000 results max
                    # Solution: Reduce requested date interval to return less
                    # than 5000 results
                    current_duration /= 2
                    continue
                else:
                    print("ERROR OANDA: " + str(error))
                    raise error

            # print(response)

            if not response:
                continue

            candles = response['candles']

            if not candles:
                continue

            # print(candles)

            ndf = pd.io.json.json_normalize(
                candles,
                meta=[OANDA_TIME, 'volume', 'complete', [OANDA_MID, OANDA_OHLC], [OANDA_ASK, OANDA_OHLC], [OANDA_BID, OANDA_OHLC]]
            )

            if df is None:
                df = ndf
            else:
                df = pd.concat([df, ndf], ignore_index=True)

        # Set date as index
        df.rename(columns={OANDA_TIME: DATAFRAME_DATE}, inplace=True)
        df[DATAFRAME_DATE] = pd.to_datetime(df[DATAFRAME_DATE])
        datetimeIndex = pd.DatetimeIndex(df[DATAFRAME_DATE], name=DATAFRAME_DATE)
        df = df.drop([DATAFRAME_DATE], axis=1)
        df = df.set_index(datetimeIndex, verify_integrity=True)

        # Duplicate Volume/Complete column for easier MultiIndex creation
        if OANDA_MID_OPEN in df.columns:
            df[OANDA_MID_VOLUME] = df[OANDA_VOLUME]
            df[OANDA_MID_COMPLETE] = df[OANDA_COMPLETE]

        if OANDA_ASK_OPEN in df.columns:
            df[OANDA_ASK_VOLUME] = df[OANDA_VOLUME]
            df[OANDA_ASK_COMPLETE] = df[OANDA_COMPLETE]

        if OANDA_BID_OPEN in df.columns:
            df[OANDA_BID_VOLUME] = df[OANDA_VOLUME]
            df[OANDA_BID_COMPLETE] = df[OANDA_COMPLETE]

        df.drop(OANDA_VOLUME, axis=1, inplace=True)
        df.drop(OANDA_COMPLETE, axis=1, inplace=True)

        # Build MultiIndex based on data columns available
        df_columns = df.columns
        tuples = [tuple(c.split('.')) for c in df_columns]

        mapping = {
            OANDA_MID: DATAFRAME_MID,
            OANDA_ASK: DATAFRAME_ASK,
            OANDA_BID: DATAFRAME_BID,
            OANDA_VOLUME: DATAFRAME_VOLUME,
            OANDA_COMPLETE: DATAFRAME_COMPLETE,
            OANDA_OPEN: DATAFRAME_OPEN,
            OANDA_HIGH: DATAFRAME_HIGH,
            OANDA_LOW: DATAFRAME_LOW,
            OANDA_CLOSE: DATAFRAME_CLOSE
        }

        tuples = [(mapping[t[0]], mapping[t[1]]) for t in tuples]
        multiIndex = pd.MultiIndex.from_tuples(tuples)
        multiIndex.name = "Data"
        df.columns = multiIndex

        # Sort by date as OANDA REST v20 provides no guarantee
        # returned candles are sorted
        df.sort_index(axis=0, level=DATAFRAME_DATE, ascending=True, inplace=True)

        with pd.option_context('display.max_columns', None, 'display.multi_sparse', False):
            pass  # print(df)

        return df

    def _reverse_pair(s, sep="_"):
        lst = s.split(sep)
        return sep.join([lst[1], lst[0]])

    def _split_currency_pair(self, s, sep="_"):
        lst = s.split(sep)
        return (lst[0], lst[1])
