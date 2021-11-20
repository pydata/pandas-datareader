#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Dict, List, Union, Optional

from abc import ABC
from sys import stdout
import pandas as pd
import time
from datetime import datetime
import pytz
import requests.exceptions

from pandas_datareader.crypto_utils.exchange import Exchange
from pandas_datareader.crypto_utils.utilities import split_str_to_list, get_exchange_names
from pandas_datareader.crypto_utils.utilities import sort_columns, print_timestamp


class CryptoReader(Exchange, ABC):
    """ Class to request the data from a given exchange for a given currency-pair.
    The class inherits from Exchange to extract and format the request urls, as well as to
    extract and format the values from the response json. The requests are performed by
    the _BaseReader.
    """

    def __init__(self,
                 exchange_name: str,
                 symbols: str,
                 start: Union[str, datetime] = None,
                 end: Union[str, datetime] = None,
                 interval: str = None,
                 **kwargs):
        """ Constructor. Inherits from the Exchange and _BaseReader class.

        @param exchange_name: String repr of the exchange name
        @param symbols: Currency pair to request (i.e. BTC-USD)
        @param start: The start time of the request, handed over to the BaseReader.
        @param end: The end time of the request, handed over to the BaseReader.
        @param interval: Candle interval (i.e. minutes, hours, days, weeks, months)
        @param kwargs: Additional arguments for the _BaseReader class.
        """

        super(CryptoReader, self).__init__(exchange_name, interval, symbols, start, end, **kwargs)

    @staticmethod
    def get_all_exchanges() -> List:
        """ Get all supported exchange names.

        @return List of exchange names.
        """

        return get_exchange_names()

    def get_currency_pairs(self) -> Optional[pd.DataFrame]:
        """ Requests all supported currency pairs from the exchange.

        @return: A list of all listed currency pairs.
        """

        param_dict = self.extract_request_urls(None, "currency_pairs")
        url = param_dict.get("currency_pairs").get("url", None)
        try:
            resp = self._get_response(url, params=None, headers=None)
            resp = self.format_currency_pairs(resp.json())
        except (requests.exceptions.MissingSchema, Exception):
            return None

        return pd.DataFrame(resp, columns=["Exchange", "Base", "Quote"])

    def _await_rate_limit(self):
        """ Sleep in order to not violate the rate limit, measured in requests per minute."""

        time.sleep(self.rate_limit)

    def _index_and_cut_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """ Set index and cut data according to user specification.

        @param dataframe: Requested raw data
        @return: pd.DataFrame with specified length and proper index.
        """

        dataframe.set_index("time", inplace=True)
        dataframe.sort_index(inplace=True)
        dataframe = dataframe.loc[pytz.utc.localize(self.start): pytz.utc.localize(self.end)]

        return sort_columns(dataframe)

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
        resp = self._get_response(url, params=params, headers=None)

        # Await the rate-limit to avoid ip ban.
        self._await_rate_limit()
        return resp.json()

    def read(self, new_symbols: str = None) -> pd.DataFrame:
        """ Requests and extracts the data. Requests may be performed iteratively over time
        to collect the full time-series.

        @param new_symbols: New currency-pair to request, if they differ from the constructor.
        @return df: pd.DataFrame of the returned data.
        """

        if new_symbols:
            if isinstance(new_symbols, str):
                new_symbols = split_str_to_list(new_symbols)
            # Create a new dict with new symbols as keys and the end timestamp as values.
            self.symbols = dict.fromkeys(new_symbols, self.end)

        result = list()
        # Repeat until no "older" timestamp is delivered. Cryptocurrency exchanges often restrict the amount of
        # data points returned by a single request, thus making it necessary to iterate backwards in time and merge
        # the retrieved data.
        while True:
            # perform request and extract data.
            resp = self._get_data()
            data, mappings = self.format_data(resp)
            # break if no data is returned
            if not data:
                break
            # or all returned data points already exist.
            elif result == data or all([datapoint in result for datapoint in data]):
                break

            print_timestamp(list(self.symbols.values())[0])

            # Append new data to the result list
            result = result + data

            # Find the place in the mapping list for the key "time".
            time_key = {v: k for k, v in enumerate(mappings)}
            time_key = time_key.get('time')

            # Extract the minimum timestamp from the response for further requests.
            new_time = min(item[time_key] for item in data)

            # Break the requesting if minimum timestamp is lower than initial start time.
            if new_time.timestamp() <= self.start.timestamp():
                break
            # Or continue requesting from the new timestamp.
            else:
                self.symbols.update({list(self.symbols.keys())[0]: new_time})

        # Move cursor to the next line to ensure that new print statements are executed correctly.
        stdout.write("\n")
        # If there is data put it into a pd.DataFrame, set index and cut it to fit the initial start/end time.
        if result:
            result = pd.DataFrame(result, columns=mappings)
            return self._index_and_cut_dataframe(result)
