#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Dict, List, Union, Optional

import warnings
from abc import ABC
from sys import stdout
import pandas as pd
import time
from datetime import datetime
import pytz
import requests.exceptions

from pandas_datareader.crypto_utils.exchange import Exchange
from pandas_datareader.crypto_utils.utilities import get_exchange_names
from pandas_datareader.crypto_utils.utilities import sort_columns, print_timestamp
from pandas_datareader.exceptions import EmptyResponseError


class CryptoReader(Exchange, ABC):
    """ Class to request the data from a given exchange for a given currency-pair.
    The class inherits from Exchange to extract and format the request urls,
    as well as to extract and format the values from the response json.
    The requests are performed by the _BaseReader.
    """

    def __init__(
        self,
        symbols: Union[str, dict] = None,
        exchange_name: str = None,
        start: Union[str, datetime] = None,
        end: Union[str, datetime] = None,
        interval: str = "days",
        **kwargs,
    ):
        """ Constructor. Inherits from the Exchange and _BaseReader class.

        @param symbols: Currency pair to request (i.e. BTC-USD)
        @param exchange_name: String repr of the exchange name
        @param start: The start time of the request, handed over to the BaseReader.
        @param end: The end time of the request, handed over to the BaseReader.
        @param interval: Candle interval (i.e. minutes, hours, days, weeks, months)
        @param **kwargs: Additional kw-arguments for the _BaseReader class.
        """

        if not start:
            start = datetime(2009, 1, 1)

        super(CryptoReader, self).__init__(
            exchange_name, interval, symbols, start, end, **kwargs
        )

    def _get_data(self) -> Dict:
        """ Requests the data and returns the response json.

        @return: Response json
        """

        # Extract and format the url and parameters for the request
        self.param_dict = "historic_rates"
        self.url_and_params = "historic_rates"

        # Perform the request
        resp = self._get_response(self.url, params=self.params, headers=None)

        # Await the rate-limit to avoid ip ban.
        self._await_rate_limit()

        return resp.json()

    def read(self, new_symbols: str = None) -> pd.DataFrame:
        """ Requests and extracts the data. Requests may be performed iteratively
        over time to collect the full time-series.

        @param new_symbols: New currency-pair to request, if they differ from
                            the constructor.
        @return: pd.DataFrame of the returned data.
        """

        if new_symbols:
            self.symbol_setter(new_symbols)

        # Check if the provided currency-pair is listed on the exchange.
        if not self._check_symbols:
            raise KeyError(
                "The provided currency-pair is not listed on "
                "'%s'. "
                "Call CryptoReader.get_currency_pairs() for an overview."
                % self.name.capitalize()
            )

        result = list()
        mappings = list()
        # Repeat until no "older" timestamp is delivered.
        # Cryptocurrency exchanges often restrict the amount of
        # data points returned by a single request, thus making it
        # necessary to iterate backwards in time and merge the retrieved data.
        while True:
            # perform request and extract data.
            resp = self._get_data()
            try:
                data, mappings = self.format_data(resp)
            # Break if no data is returned
            except EmptyResponseError:
                break

            # or all returned data points already exist.
            if result == data or all([datapoint in result for datapoint in data]):
                break

            if self.interval == "minutes":
                print_timestamp(list(self.symbols.values())[0])

            # Append new data to the result list
            result = result + data

            # Find the place in the mapping list for the key "time".
            time_key = {v: k for k, v in enumerate(mappings)}
            time_key = time_key.get("time")

            # Extract the minimum timestamp from the response for further requests.
            new_time = min(item[time_key] for item in data)

            # Break if min timestamp is lower than initial start time.
            if new_time.timestamp() <= self.start.timestamp():
                break
            # Or continue requesting from the new timestamp.
            else:
                self.symbols.update({list(self.symbols.keys())[0]: new_time})

        # Move cursor to the next line to ensure that
        # new print statements are executed correctly.
        stdout.write("\n")
        # If there is data put it into a pd.DataFrame,
        # set index and cut it to fit the initial start/end time.
        if result:
            result = pd.DataFrame(result, columns=mappings)
            result = self._index_and_cut_dataframe(result)

        # Reset the self.end date of the _BaseReader for further requesting.
        self.reset_request_start_date()

        return result

    @staticmethod
    def get_all_exchanges() -> List:
        """ Get all supported exchange names.

        @return: List of exchange names.
        """

        return get_exchange_names()

    def get_currency_pairs(
        self, raw_data: bool = False
    ) -> Optional[Union[pd.DataFrame, List]]:
        """ Requests all supported currency pairs from the exchange.

        @param raw_data: Return the raw data as a list of tuples.
        @return: A list of all listed currency pairs.
        """

        self.param_dict = "currency_pairs"
        self.url_and_params = "currency_pairs"
        try:
            resp = self._get_response(self.url, params=None, headers=None)
            resp = self.format_currency_pairs(resp.json())
        except (requests.exceptions.MissingSchema, Exception):
            return None

        if raw_data:
            return resp

        # create pd.DataFrame and apply upper case to values
        data = pd.DataFrame(resp, columns=["Exchange", "Base", "Quote"])
        data = data.apply(lambda x: x.str.upper(), axis=0)

        return data

    @property
    def _check_symbols(self) -> bool:
        """ Checks if the specified currency-pair is listed on the exchange"""

        currency_pairs = self.get_currency_pairs(raw_data=True)
        symbols = (
            self.symbols.keys() if isinstance(self.symbols, dict) else [self.symbols]
        )

        if currency_pairs is None:
            warnings.warn(
                "Currency-pair request is dysfunctional. "
                "Check for valid symbols is skipped."
            )
            return True

        return any(
            [
                all(
                    [
                        (self.name, *symbol.lower().split("/")) in currency_pairs
                        for symbol in symbols
                    ]
                ),
                all(
                    [
                        (self.name, *symbol.lower().split("-")) in currency_pairs
                        for symbol in symbols
                    ]
                ),
            ]
        )

    def _await_rate_limit(self):
        """ Sleep time in order to not violate the rate limit,
        measured in requests per minute."""

        time.sleep(self.rate_limit)

    def _index_and_cut_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """ Set index and cut data according to user specification.

        @param dataframe: Requested raw data
        @return: pd.DataFrame with specified length and proper index.
        """

        # Reindex dataframe and cut it to the specified start and end dates.
        dataframe.set_index("time", inplace=True)
        dataframe.sort_index(inplace=True)
        # Returned timestamps from the exchanges are converted into UTC
        # and therefore timezone aware. Make start and end dates
        # timezone aware in order to make them comparable.
        dataframe = dataframe.loc[
            pytz.utc.localize(self.start) : pytz.utc.localize(self.end)
        ]

        return sort_columns(dataframe)
