import os
import sys
import threading
import json

import logging
from logging import NullHandler

try:
    # Python 3.x
    from queue import PriorityQueue, Queue, Empty
except ImportError:
    # Python 2.x
    from Queue import PriorityQueue, Queue, Empty

import multiprocessing
from multiprocessing import Lock
import itertools

from datetime import timedelta
from datetime import datetime
from time import time

import pandas as pd
import re

from oandapyV20 import API, V20Error
import oandapyV20.endpoints.instruments as instruments
import oandapyV20.endpoints.accounts as accounts

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
                 reader_compatible=None,
                 max_concurrency=None):
        _BaseReader.__init__(self, symbols, start=start,
                             end=end, session=session)

        self.logger = logging.getLogger("pandas_datareader.oanda.OANDARestHistoricalInstrumentReader")
        if not len(self.logger.handlers):
            self.logger.addHandler(NullHandler())

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

        if isinstance(freq, DateOffset):
            offsetString = freq.freqstr
        else:
            offsetString = freq

        if offsetString in OANDARestHistoricalInstrumentReader.SUPPORTED_OFFSET_ALIASES:
            self.granularity = OANDARestHistoricalInstrumentReader.SUPPORTED_OFFSET_ALIASES[
                offsetString]
        else:
            self.granularity = offsetString

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

        self.max_concurrency = max_concurrency
        if max_concurrency is None or max_concurrency < 1:
            try:
                self.max_concurrency = multiprocessing.cpu_count()
            except NotImplementedError:
                self.max_concurrency = 1
        # 2 threads min required
        self.max_concurrency = max(2, self.max_concurrency)

        self.counter_lock = Lock()
        self.counter = itertools.count()

    def read(self):
        dfs = OrderedDict()

        self.logger.debug(str(self.symbols))

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

                if df is not None:
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

        self.OANDA_OPEN = 'o'
        self.OANDA_HIGH = 'h'
        self.OANDA_LOW = 'l'
        self.OANDA_CLOSE = 'c'
        self.OANDA_MID = 'mid'
        self.OANDA_ASK = 'ask'
        self.OANDA_BID = 'bid'
        self.OANDA_OHLC = [self.OANDA_OPEN, self.OANDA_HIGH, self.OANDA_LOW, self.OANDA_CLOSE]
        self.OANDA_TIME = 'time'
        self.OANDA_VOLUME = 'volume'
        self.OANDA_COMPLETE = 'complete'
        self.OANDA_MID_OPEN = 'mid.o'
        self.OANDA_MID_VOLUME = 'mid.volume'
        self.OANDA_MID_COMPLETE = 'mid.complete'
        self.OANDA_ASK_OPEN = 'ask.o'
        self.OANDA_ASK_VOLUME = 'ask.volume'
        self.OANDA_ASK_COMPLETE = 'ask.complete'
        self.OANDA_BID_OPEN = 'bid.o'
        self.OANDA_BID_VOLUME = 'bid.volume'
        self.OANDA_BID_COMPLETE = 'bid.complete'

        self.DATAFRAME_DATE = 'Date'
        self.DATAFRAME_MID = 'Mid'
        self.DATAFRAME_ASK = 'Ask'
        self.DATAFRAME_BID = 'Bid'
        self.DATAFRAME_OPEN = 'Open'
        self.DATAFRAME_HIGH = 'High'
        self.DATAFRAME_LOW = 'Low'
        self.DATAFRAME_CLOSE = 'Close'
        self.DATAFRAME_VOLUME = 'Volume'
        self.DATAFRAME_COMPLETE = 'Complete'

        df = self._concurrent_historical_currency_pair_download(start, end, currencyPair, access_credential)

        if df is not None:
            # Remove duplicates entry with similar time
            df.drop_duplicates([self.OANDA_TIME], keep="first", inplace=True)

            # Set date as index
            df.rename(columns={self.OANDA_TIME: self.DATAFRAME_DATE}, copy=False, inplace=True)
            df[self.DATAFRAME_DATE] = pd.to_datetime(df[self.DATAFRAME_DATE])
            df = df.set_index(self.DATAFRAME_DATE)

            # Duplicate Volume/Complete column for easier MultiIndex creation
            if self.OANDA_MID_OPEN in df.columns:
                df[self.OANDA_MID_VOLUME] = df[self.OANDA_VOLUME]
                df[self.OANDA_MID_COMPLETE] = df[self.OANDA_COMPLETE]

            if self.OANDA_ASK_OPEN in df.columns:
                df[self.OANDA_ASK_VOLUME] = df[self.OANDA_VOLUME]
                df[self.OANDA_ASK_COMPLETE] = df[self.OANDA_COMPLETE]

            if self.OANDA_BID_OPEN in df.columns:
                df[self.OANDA_BID_VOLUME] = df[self.OANDA_VOLUME]
                df[self.OANDA_BID_COMPLETE] = df[self.OANDA_COMPLETE]

            df.drop(self.OANDA_VOLUME, axis=1, inplace=True)
            df.drop(self.OANDA_COMPLETE, axis=1, inplace=True)

            # Build MultiIndex based on data columns available
            df_columns = df.columns
            tuples = [tuple(c.split('.')) for c in df_columns]

            mapping = {
                self.OANDA_MID: self.DATAFRAME_MID,
                self.OANDA_ASK: self.DATAFRAME_ASK,
                self.OANDA_BID: self.DATAFRAME_BID,
                self.OANDA_VOLUME: self.DATAFRAME_VOLUME,
                self.OANDA_COMPLETE: self.DATAFRAME_COMPLETE,
                self.OANDA_OPEN: self.DATAFRAME_OPEN,
                self.OANDA_HIGH: self.DATAFRAME_HIGH,
                self.OANDA_LOW: self.DATAFRAME_LOW,
                self.OANDA_CLOSE: self.DATAFRAME_CLOSE
            }

            tuples = [(mapping[t[0]], mapping[t[1]]) for t in tuples]

            multiIndex = pd.MultiIndex.from_tuples(tuples)
            multiIndex.name = "Data"
            df.columns = multiIndex

            # Convert some colums to specific datatypes
            tuples = [
                t for t in tuples
                if t[1] in [
                    self.DATAFRAME_OPEN,
                    self.DATAFRAME_HIGH,
                    self.DATAFRAME_LOW,
                    self.DATAFRAME_CLOSE
                ]
            ]
            df[tuples] = df[tuples].apply(pd.to_numeric)

            # Sort by date as OANDA REST v20 provides no guarantee
            # returned candles are sorted
            df.sort_index(axis=0, level=self.DATAFRAME_DATE, ascending=True, inplace=True)

            with pd.option_context('display.max_columns', 1000, 'display.width', 1000, 'display.multi_sparse', False):
                self.logger.debug("Final Currency DataFrame")
                self.logger.debug(df.head(3))
                pass

        return df

    def _concurrent_historical_currency_pair_download(self, start, end, symbol, credential):
        pending_jobs_queue = PriorityQueue()
        completed_jobs_queue = PriorityQueue()
        failed_job_queue = Queue()
        active_thread_queue = Queue()

        self.logger.debug(credential)

        consumer_threads = [
            threading.Thread(
                target=self._consume_download_job,
                args=(
                    pending_jobs_queue,
                    completed_jobs_queue,
                    failed_job_queue,
                    active_thread_queue,
                    API(access_token=credential['apiToken'], environment=credential['accountType'])
                )
            )
            for i in range(self.max_concurrency)
        ]
        producer_thread = threading.Thread(
            target=self._produce_download_jobs,
            args=(
                start,
                end,
                symbol,
                pending_jobs_queue,
                active_thread_queue
            ),
            name="Producer"
        )

        thread_pool = [producer_thread] + consumer_threads

        for thread in thread_pool:
            thread.start()
            active_thread_queue.put(None)

        df = self._merge_available_results(completed_jobs_queue, active_thread_queue)

        # thread_pool's threads stops by themselves

        return df

    def _merge_available_results(self, completed_jobs_queue, active_thread_queue):
        df = None

        done = False
        while not done:
            done = active_thread_queue.empty()

            self.logger.debug("done yet ?" + str(done))

            # Merge available results
            job = None

            try:
                priority, counter, job = completed_jobs_queue.get(block=True, timeout=1)
            except Empty:
                pass

            if job is not None:
                if df is None:
                    df = job.result
                else:
                    df = df.append(job.result, ignore_index=True)

                completed_jobs_queue.task_done()

        return df

    def _produce_download_jobs(self, start, end, symbol, pending_jobs_queue, active_thread_queue):
        try:
            for job in self._get_download_jobs(start, end, symbol):
                pending_jobs_queue.put((job.priority, self._increment_and_get_counter(), job))

            pending_jobs_queue.put((float(sys.maxsize), self._increment_and_get_counter(), None))
        finally:
            active_thread_queue.get()
            active_thread_queue.task_done()

    def _consume_download_job(self, pending_jobs_queue, completed_jobs_queue, failed_job_queue, active_thread_queue, oanda):
        done = False
        try:
            while not done:
                priority, counter, job = pending_jobs_queue.get()

                if job is not None:
                    self.logger.debug("consume job:" + str(job.priority) + "start:" + str(job.start) + "end:" + str(job.end))
                    self._download_historical_currency_pair(job, oanda)

                    if job.result is None:
                        if job.failed_count < 1:
                            job.failed_count = job.failed_count + 1
                            pending_jobs_queue.put((job.priority, self._increment_and_get_counter(), job))
                        else:
                            failed_job_queue.put(job)
                    else:
                        completed_jobs_queue.put((job.priority, self._increment_and_get_counter(), job))

                else:
                    done = True
                    pending_jobs_queue.put((float(sys.maxsize), self._increment_and_get_counter(), None))

                pending_jobs_queue.task_done()
        finally:
            active_thread_queue.get()
            active_thread_queue.task_done()

    def _get_download_jobs(self, start, end, symbol):
        class Job(object):
            def __lt__(self, other):
                return self.priority < other.priority

            def __le__(self, other):
                return self.priority <= other.priority

            def __eq__(self, other):
                return self.priority == other.priority

            def __ne__(self, other):
                return self.priority != other.priority

            def __gt__(self, other):
                return self.priority > other.priority

            def __ge__(self, other):
                return self.priority >= other.priority

        index = 0.0
        for current_start, current_end in self._get_periods(start, end, timedelta(days=1)):
            index = index + 1.0

            job = Job()
            job.priority = index
            job.start = current_start
            job.end = current_end
            job.symbol = symbol
            job.result = None
            job.failed_count = 0

            self.logger.debug("produce job:" + str(job.priority) + "start:" + str(job.start) + "end:" + str(job.end))

            yield job

    def _download_historical_currency_pair(self, job, oanda):
        request_period_start = datetime.now()
        request_period_end = datetime.now()
        request_period_duration = timedelta(seconds=1)
        request_count_per_second = 0
        # OANDA recommends to send no more than 15 reqs / s for an existing connection
        request_max_per_second = 15

        current_start = job.start
        current_end = job.end
        end = job.end
        currencyPair = job.symbol
        current_duration = current_end - current_start
        includeCandleOnStart = True

        df = None

        while current_start < end:
            current_end = current_start + current_duration

            if current_end > end:
                current_end = end

            # current_start_timestamp = time.mktime(current_start.timetuple())
            # current_end_timestamp = time.mktime(current_end.timetuple())
            rfc3339 = "%Y-%m-%dT%H:%M:%SZ"  # Eg: 2014-03-21T17:41:00Z
            current_start_rfc3339 = current_start.strftime(rfc3339)
            current_end_rfc3339 = current_end.strftime(rfc3339)

            params = {
                "granularity": self.granularity,
                "from": current_start_rfc3339,
                "to": current_end_rfc3339,
                "price": self.candleFormat,
                "smooth": "false",
                "includeFirst": "true" if includeCandleOnStart else "false",
                "dailyAlignment": "17",
                "alignmentTimezone": "America/New_York",
                "weeklyAlignment": "Friday"
            }

            self.logger.debug(params)

            # Limit number of requests per period
            now = datetime.now()
            if request_period_end < now:
                request_count_per_second = 0
                request_period_start = now
                request_period_end = request_period_start + request_period_duration

            request_count_per_second += 1

            self.logger.debug(str(request_count_per_second) + " reqs/s")

            if request_count_per_second > request_max_per_second:
                request_pause_duration = request_period_end - now
                time.sleep(request_pause_duration.total_seconds())

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
                    if type(error) is V20Error:
                        self.logger.exception("Request failed with code: " + str(error.code) + " and message: " + str(error.msg))
                    else:
                        self.logger.exception("ERROR OANDA: " + str(error))
                    raise error

            self.logger.debug(response)

            if not response:
                continue

            candles = response['candles']

            if not candles:
                continue

            ndf = pd.io.json.json_normalize(
                candles,
                meta=[self.OANDA_TIME, 'volume', 'complete', [self.OANDA_MID, self.OANDA_OHLC], [self.OANDA_ASK, self.OANDA_OHLC], [self.OANDA_BID, self.OANDA_OHLC]]
            )

            if df is None:
                df = ndf
            else:
                df = df.append(ndf, ignore_index=True)

        job.result = df

    def _get_periods(self, start, end, delta):
        current_start = start
        while current_start < end:
            if current_start > end:
                current_end = end
            else:
                current_end = current_start + delta

            yield current_start, current_end

            current_start = current_start + delta

    def _increment_and_get_counter(self):
        self.counter_lock.acquire()
        value = next(self.counter)
        self.counter_lock.release()
        return value

    def _reverse_pair(s, sep="_"):
        lst = s.split(sep)
        return sep.join([lst[1], lst[0]])

    def _split_currency_pair(self, s, sep="_"):
        lst = s.split(sep)
        return (lst[0], lst[1])


class OANDARestAccountInstrumentReader(_BaseReader):
    """
        Tradeable instruments for a given account using  OANDA's REST v20 API.
        See details at http://developer.oanda.com/rest-live-v20/account-ep/
        symbols : string or Dict of strings.
            Each string is a currency pair with format BASE_QUOTE. Eg: ["EUR_USD", "JPY_USD"]
        symbolsTypes: Dict of strings.
            Each string represent the type of instrument to fetch data for. Eg: For symbols=["EUR_USD", "EUR_JPY"] then symbolsTypes=["currency", "currency"]
            Valid values: currency
        use_default_symbols_if_account_not_authorized: Boolean
            True if fetching the instruments metadata fails due to HTTP 403
        access_credential: Dict of strings
            Credential to query the api
            credential["accountType"]="practise". Mandatory. Valid values: practice, live
            credential["apiToken"]="Your OANDA API token". Mandatory. Valid value: See your OANDA Account's API Token
            credential['accountId']="Your OANDA account Id to query instruments from"
    """

    def __init__(self, symbols, symbolsTypes=None,
                 session=None, use_default_symbols_if_account_not_authorized=True,
                 access_credential=None):

        self.logger = logging.getLogger("pandas_datareader.oandarest.OANDARestAccountInstrumentReader")
        if not len(self.logger.handlers):
            self.logger.addHandler(NullHandler())

        self.SYMBOL_ALL = "all_all"
        self.SYMBOL_TYPE_CURRENCY = "currency"

        self.symbols = symbols
        if symbols is None:
            self.symbols = [self.SYMBOL_ALL]
            self.symbolsTypes = [self.SYMBOL_TYPE_CURRENCY]

        if type(symbols) is str:
            self.symbols = [symbols]

        self.symbolsTypes = symbolsTypes
        if symbolsTypes is None:
            self.symbolsTypes = []

        if len(self.symbols) != len(self.symbolsTypes):
            self.symbolsTypes = ["currency" for x in self.symbols]

        self.access_credential = access_credential
        if access_credential is None:
            self.access_credential = {}

        if 'accountType' not in access_credential:
            self.access_credential['accountType'] = "practice"

        if 'apiToken' not in access_credential:
            self.access_credential['apiToken'] = os.getenv('OANDA_API_TOKEN')
            if self.access_credential['apiToken'] is None:
                raise ValueError(
                    """Please provide an OANDA API token or set the OANDA_API_TOKEN environment variable\n
                    If you do not have an API key, you can get one here: http://developer.oanda.com/rest-live/authentication/""")

        if 'accountId' not in access_credential:
            self.access_credential['accountId'] = os.getenv('OANDA_ACCOUNT_ID')
            if self.access_credential['accountId'] is None:
                raise ValueError(
                    """Please provide an OANDA ACCOUNT ID token or set the OANDA_ACCOUNT_ID environment variable""")

        self.use_default_symbols_if_account_not_authorized = use_default_symbols_if_account_not_authorized
        if use_default_symbols_if_account_not_authorized is None:
            self.use_default_symbols_if_account_not_authorized = True

    def read(self):
        dfs = {}
        df_currency = None

        for (index, symbol) in enumerate(self.symbols):
            symbolsType = self.symbolsTypes[index]
            if symbolsType is self.SYMBOL_TYPE_CURRENCY:
                requested_symbols = [] if symbol is self.SYMBOL_ALL else [symbol]

                df = self._read_available_currency_pairs(
                    symbols=requested_symbols,
                    access_credential=self.access_credential
                )
                if df_currency is None:
                    df_currency = df
                else:
                    df_currency.concat(df)
            else:
                raise Exception("Symbol Type; %s not supported" %
                                (symbolsType))

        dfs["Currencies"] = df_currency

        pn = pd.Panel(dfs)
        pn.axes[0].name = "Instruments"
        return pn

    def _read_available_currency_pairs(self, symbols=None, access_credential=None):
        if access_credential is None:
            raise Exception(
                'No access_crendtial provided. Instruments cannot be fetched')

        credential = access_credential

        oanda = API(access_token=credential['apiToken'],
                    environment=credential['accountType'])

        params = {
        }

        if symbols:
            params['instruments'] = ",".join(symbols)

        self.logger.debug(params)

        default_response = None
        response = None

        try:
            request = accounts.AccountInstruments(
                credential['accountId'],
                params=params)

            response = oanda.request(request)
        except Exception as error:
            if type(error) is V20Error:
                if error.code == 403:
                    default_response = self._get_default_account_instruments_metadata()
                else:
                    self.logger.exception("Request failed with code: " + str(error.code) + " and message: " + str(error.msg))
            else:
                self.logger.exception("ERROR OANDA: " + str(error))
                raise

        self.logger.error(response)

        default_response_used = default_response is not None and self.use_default_symbols_if_account_not_authorized

        if default_response_used:
            instruments = json.loads(default_response)['instruments']
        else:
            instruments = response['instruments']

        oanda_fields = [
            'displayName',
            'displayPrecision',
            'marginRate',
            'maximumOrderUnits',
            'maximumPositionSize',
            'maximumTrailingStopDistance',
            'minimumTradeSize',
            'minimumTrailingStopDistance',
            'name',
            'pipLocation',
            'tradeUnitsPrecision',
            'type',
            'start_date'
        ]

        mapping_fields = {key: key[0].upper() + key[1:] for key in oanda_fields}
        # Standard Instrument Metadata
        df = pd.io.json.json_normalize(
            instruments,
            meta=mapping_fields.keys()
        )

        df.rename(inplace=True,
                  index=str,
                  columns=mapping_fields)

        df = df.set_index(mapping_fields['name'])
        df['Provider'] = "default" if default_response_used else "real"

        # Extra Instrument Metadata
        instruments_extra = json.loads(self._get_default_account_instruments_extra_metadata())["instruments"]
        df_extra = pd.io.json.json_normalize(
            instruments_extra,
            meta=mapping_fields.keys()
        )

        df_extra.rename(
            inplace=True,
            index=str,
            columns=mapping_fields
        )

        df_extra = df_extra.set_index(mapping_fields['name'])

        # Merge all instruments metadata
        df = pd.concat([df, df_extra], axis=1)

        df.sort_index(axis=0, level=mapping_fields['name'], ascending=True, inplace=True)

        with pd.option_context('display.max_columns', 1000, 'display.width', 1000, 'display.multi_sparse', False):
            self.logger.debug(df)

        return df

    def _get_default_account_instruments_extra_metadata(self):
        # Last updated: 13 Dec 2016
        # start_date:
        #  OANDA does not provide this information.
        #  If you know the real start_date for a currency pair, please update it
        value = """
{
   "instruments":[
      {
         "name":"USD_THB",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_CHF",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_GBP",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_SGD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"CAD_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_ZAR",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_CHF",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"SGD_CHF",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"CHF_ZAR",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"SGD_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"NZD_USD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"NZD_CHF",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_HKD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_HKD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_DKK",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"AUD_HKD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_CZK",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_NOK",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"NZD_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"AUD_USD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"AUD_NZD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_CAD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_PLN",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"ZAR_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_SAR",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_CAD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"AUD_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_TRY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_NZD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_ZAR",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_USD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_MXN",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_PLN",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_DKK",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_NOK",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_CZK",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"NZD_SGD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_HUF",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_HKD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_SEK",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_SGD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"AUD_CHF",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"AUD_SGD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_CNH",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"TRY_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"AUD_CAD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"CAD_HKD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"SGD_HKD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_NZD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"NZD_CAD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_ZAR",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_SEK",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_SGD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"HKD_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_USD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"GBP_AUD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_PLN",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_CAD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"CAD_SGD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"CHF_HKD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_CHF",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"NZD_HKD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_AUD",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"CAD_CHF",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"CHF_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"USD_JPY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_TRY",
         "start_date":"2000-01-01T00:00:00.000Z"
      },
      {
         "name":"EUR_HUF",
         "start_date":"2000-01-01T00:00:00.000Z"
      }
   ],
   "lastTransactionID":"6356"
}
    """
        return value

    def _get_default_account_instruments_metadata(self):
        # Last updated: 13 Dec 2016
        value = """
{
  "instruments": [
    {
      "displayName": "USD/THB",
      "displayPrecision": 3,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "USD_THB",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/CHF",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_CHF",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/GBP",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_GBP",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/JPY",
      "displayPrecision": 3,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "EUR_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/SGD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_SGD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "CAD/JPY",
      "displayPrecision": 3,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "CAD_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/ZAR",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_ZAR",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/CHF",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_CHF",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "SGD/CHF",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "SGD_CHF",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "CHF/ZAR",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "CHF_ZAR",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "SGD/JPY",
      "displayPrecision": 3,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "SGD_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "NZD/USD",
      "displayPrecision": 5,
      "marginRate": "0.02",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "NZD_USD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "NZD/CHF",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "NZD_CHF",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/HKD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_HKD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/HKD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_HKD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/DKK",
      "displayPrecision": 5,
      "marginRate": "0.02",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_DKK",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "AUD/HKD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "AUD_HKD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/CZK",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_CZK",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/NOK",
      "displayPrecision": 5,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_NOK",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "NZD/JPY",
      "displayPrecision": 3,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "NZD_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "AUD/USD",
      "displayPrecision": 5,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "AUD_USD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "AUD/NZD",
      "displayPrecision": 5,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "AUD_NZD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/CAD",
      "displayPrecision": 5,
      "marginRate": "0.02",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_CAD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/PLN",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_PLN",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "ZAR/JPY",
      "displayPrecision": 3,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "ZAR_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/SAR",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_SAR",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/CAD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_CAD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "AUD/JPY",
      "displayPrecision": 3,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "AUD_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/JPY",
      "displayPrecision": 3,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "GBP_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/TRY",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_TRY",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/NZD",
      "displayPrecision": 5,
      "marginRate": "0.02",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_NZD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/ZAR",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_ZAR",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/USD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_USD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/MXN",
      "displayPrecision": 5,
      "marginRate": "0.06",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_MXN",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/PLN",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_PLN",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/DKK",
      "displayPrecision": 5,
      "marginRate": "0.02",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_DKK",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/NOK",
      "displayPrecision": 5,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_NOK",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/CZK",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_CZK",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "NZD/SGD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "NZD_SGD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/HUF",
      "displayPrecision": 3,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "USD_HUF",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/HKD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_HKD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/SEK",
      "displayPrecision": 5,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_SEK",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/SGD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_SGD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "AUD/CHF",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "AUD_CHF",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "AUD/SGD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "AUD_SGD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/CNH",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_CNH",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "TRY/JPY",
      "displayPrecision": 3,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "TRY_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "AUD/CAD",
      "displayPrecision": 5,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "AUD_CAD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "CAD/HKD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "CAD_HKD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "SGD/HKD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "SGD_HKD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/NZD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_NZD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "NZD/CAD",
      "displayPrecision": 5,
      "marginRate": "0.02",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "NZD_CAD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/ZAR",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_ZAR",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/SEK",
      "displayPrecision": 5,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_SEK",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/SGD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_SGD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "HKD/JPY",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "HKD_JPY",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/USD",
      "displayPrecision": 5,
      "marginRate": "0.02",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_USD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "GBP/AUD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "GBP_AUD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/PLN",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_PLN",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/CAD",
      "displayPrecision": 5,
      "marginRate": "0.02",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_CAD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "CAD/SGD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "CAD_SGD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "CHF/HKD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "CHF_HKD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/CHF",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "USD_CHF",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "NZD/HKD",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "NZD_HKD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/AUD",
      "displayPrecision": 5,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_AUD",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "CAD/CHF",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "CAD_CHF",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "CHF/JPY",
      "displayPrecision": 3,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "CHF_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "USD/JPY",
      "displayPrecision": 3,
      "marginRate": "0.03",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "USD_JPY",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/TRY",
      "displayPrecision": 5,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "1.00000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.00050",
      "name": "EUR_TRY",
      "pipLocation": -4,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    },
    {
      "displayName": "EUR/HUF",
      "displayPrecision": 3,
      "marginRate": "0.05",
      "maximumOrderUnits": "100000000",
      "maximumPositionSize": "0",
      "maximumTrailingStopDistance": "100.000",
      "minimumTradeSize": "1",
      "minimumTrailingStopDistance": "0.050",
      "name": "EUR_HUF",
      "pipLocation": -2,
      "tradeUnitsPrecision": 0,
      "type": "CURRENCY"
    }
  ],
  "lastTransactionID": "6356"
}
        """
        return value

    def _reverse_pair(s, sep="_"):
        lst = s.split(sep)
        return sep.join([lst[1], lst[0]])

    def _split_currency_pair(self, s, sep="_"):
        lst = s.split(sep)
        return (lst[0], lst[1])
