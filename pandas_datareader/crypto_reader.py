import sys
from typing import Dict

from sys import stdout
import pandas as pd
import time
import pytz

from pandas_datareader.crypto.exchange import Exchange
from pandas_datareader.crypto.utilities import split_str_to_list


class CryptoReader(Exchange):
    """ Class to request the data from a given exchange for a given currency-pair.
    The class inherits from Exchange to extract and format the request urls, as well as to
    extract and format the values from the response json. The requests are performed by
    the _BaseReader.
    """

    def __init__(self, exchange_name: str, symbols, interval: str = 'minutes', **kwargs):
        """ Constructor.

        @param exchange_name: String repr of the exchange name
        @param symbols: Currency pair to request (i.e. BTC-USD)
        @param interval: Candle interval (i.e. minutes, hours, days, weeks, months)
        @param kwargs: Additional arguments for the _BaseReader class.
        """

        super(CryptoReader, self).__init__(exchange_name, symbols, interval, **kwargs)

    def await_rate_limit(self):
        """ Sleep in order to not violate the rate limit."""

        time.sleep(self.rate_limit)

    @staticmethod
    def sort_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
        """ Sort columns in OHLCV order.

        @param dataframe: Requested data with unordered columns
        @return: pd.DataFrame with ordered columns
        """

        columns = {'open', 'high', 'low', 'close', 'volume', 'market_cap'}
        columns = list(columns.intersection(set(dataframe.columns)))
        return dataframe.loc[columns, :]

    @staticmethod
    def print_timestamp(timestamp):
        """ Prints the actual request timestamp.

        @param timestamp: The timestamp
        """
        stdout.write("Requesting from: \r{}".format(timestamp))
        stdout.flush()

    def index_and_cut_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """ Set index and cut data according to user specification.

        @param dataframe: Requested raw data
        @return: pd.DataFrame with specified length and proper index.
        """

        dataframe.set_index("time", inplace=True)
        dataframe.sort_index(inplace=True)
        dataframe = dataframe.loc[pytz.utc.localize(self.start): pytz.utc.localize(self.end)]

        return dataframe

    def _get_data(self) -> Dict:
        """ Requests the data and returns the response json.

        @return: Response json
        """
        # Ensure that the currency-pairs are seperated in a list
        if isinstance(self.symbols, str):
            self.symbols = split_str_to_list(self.symbols)
            self.symbols = dict.fromkeys(self.symbols, self.end)

        # Extract and format the url and parameters for the request
        param_dict = self.extract_request_urls(self.symbols)
        url, params = self.get_formatted_url_and_params(param_dict, *self.symbols.keys())

        # Perform the request
        self.print_timestamp(list(self.symbols.values())[0])
        resp = self._get_response(url, params=params, headers=None)

        # Await the rate-limit to avoid ip ban.
        self.await_rate_limit()
        return resp.json()

    def request(self, new_symbols: str = None) -> pd.DataFrame:
        """ Requests and extracts the data. Requests may be performed iteratively over time
        to collect the full time-series.

        @param new_symbols: New currency-pair to request, if they differ from the constructor.
        @return df: pd.DataFrame of the returned data.
        """

        if new_symbols:
            if isinstance(new_symbols, str):
                new_symbols = split_str_to_list(new_symbols)
            self.symbols = dict.fromkeys(new_symbols, self.end)

        result = list()
        # Repeat until no "older" timestamp is delivered. Cryptocurrency exchanges often restrict the amount of
        # data points returned by a single request, thus making it necessary to iterate backwards in time and merge
        # the retrieved data.
        while True:
            resp = self._get_data()
            data, mappings = self.format_data(resp)
            if not data:
                break
            # Append new data to the result list
            result = result + data

            # Find the place in the mapping list for the key "time".
            for counter, value in enumerate(mappings):
                if value == 'time':
                    break

            # Extract the minimum timestamp from the response to continue requesting with.
            new_time = min(item[counter] for item in data)

            # Break the requesting if condition is fulfilled
            if new_time.timestamp() <= self.start.timestamp():
                break
            # Or continue requesting from the new timestamp.
            else:
                self.symbols.update({list(self.symbols.keys())[0]: new_time})

        # Move cursor to the next line to ensure that new print statements are executed correctly.
        stdout.write("\n")
        if result:
            result = pd.DataFrame(result, columns=mappings)
            return self.index_and_cut_dataframe(result)
