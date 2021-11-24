#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import datetime
import pandas as pd

from pandas_datareader.crypto import CryptoReader
from pandas_datareader.crypto_utils.mapping import extract_mappings
from pandas_datareader.crypto_utils.utilities import yaml_loader, sort_columns
from pandas_datareader.exceptions import EmptyResponseError


class TestCryptoReader:
    """ Unit tests for the CryptoReader."""

    exchange_name = 'coinbase'
    symbols = 'btc-usd'
    CryptoReader = CryptoReader(exchange_name, symbols)

    def test_get_all_exchanges(self):
        """ Test to return a list of all available exchanges."""

        result = self.CryptoReader.get_all_exchanges()

        assert isinstance(result, list)

    def test_read(self):
        """ Test the request from a particular exchange."""

        result = self.CryptoReader.read()

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_request_new_symbol(self):
        """ Test to request NEW symbols."""

        result = self.CryptoReader.read('eth-usd')

        assert 'eth-usd' in self.CryptoReader.symbols.keys()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_reset_request_start_date(self):
        """ Test resetting the request start date."""

        key = list(self.CryptoReader.symbols.keys())[0]
        self.CryptoReader.symbols.update({key: datetime.datetime(2020, 1, 1)})
        self.CryptoReader.reset_request_start_date()

        assert self.CryptoReader.symbols.get(key).date() == datetime.datetime.now().date()

    def test_check_symbols(self):
        """ Test checking if the provided currency-pair is listed on an exchange."""
        with pytest.raises(KeyError):
            self.CryptoReader.symbols = {" - ": datetime.datetime.today()}
            self.CryptoReader.read()

    def test_iterate_requests_until_end(self):
        """ Tests to iterate the request with updated timestamps until no more timestamp is collected
            or start time is reached."""
        # ToDo
        pass

    def test_sort_result_columns(self):
        """ Test to sort the columns of the response."""

        ordered_cols = ['open', 'high', 'low', 'close']
        response = pd.DataFrame({'high': range(0, 5), 'close':  range(0, 5),
                                 'open':  range(0, 5), 'low':  range(0, 5)})
        response = sort_columns(dataframe=response)

        assert all(ordered_cols == response.columns)

    def test_ensure_correct_column_names(self):
        """ Test to ensure specific column names."""

        response = pd.DataFrame({'High': range(0, 5), 'CLOSE':  range(0, 5),
                                 'oPen':  range(0, 5), 'low':  range(0, 5)})
        response = sort_columns(dataframe=response)

        assert response.columns == 'low'

    def test_cut_response_and_set_index(self):
        """ Test to cut the response to the initially defined start/end dates."""

        response = pd.DataFrame({'open': range(0, 100),
                                 'time': pd.period_range(end='2020-12-31', periods=100, freq="d")})

        self.CryptoReader.start = datetime.datetime(2020, 12, 1)
        self.CryptoReader.end = datetime.datetime(2020, 12, 20)

        response = self.CryptoReader._index_and_cut_dataframe(response)

        assert response.shape[0] == 20
        assert min(response.index).to_timestamp() == self.CryptoReader.start
        assert max(response.index).to_timestamp() == self.CryptoReader.end


class TestExchange:
    """ Unit tests for the Exchange class."""
    # ToDo: Create test-exchange yaml file and use it instead for this class.
    exchange_name = 'coinbase'
    symbols = 'btc-usd'
    CryptoReader = CryptoReader(exchange_name, symbols)

    def test_extract_mappings(self):
        """ Test to extract the mapping keys and values from the yaml files."""

        result = extract_mappings(self.CryptoReader.name,
                                  self.CryptoReader.yaml_file.get('requests')).get('historic_rates')

        assert isinstance(result, list)
        assert result

    def test_all_exchanges_have_mappings_and_necessary_values(self):
        """ Test if all exchange yaml-files have a specified mapping."""

        exchanges = self.CryptoReader.get_all_exchanges()
        assert isinstance(exchanges, list)
        assert exchanges

        for exchange in exchanges:
            file = yaml_loader(exchange)
            result = extract_mappings(exchange, file.get('requests')).get('historic_rates')
            assert isinstance(result, list)
            assert result

    def test_necessary_values_in_mappings(self):
        """ Test if all necessary values are in the mappings."""

        exchanges = self.CryptoReader.get_all_exchanges()

        for exchange in exchanges:
            file = yaml_loader(exchange)
            mappings = extract_mappings(exchange, file.get('requests')).get('historic_rates')

            for mapping in mappings:
                # Check if the object dict contains all necessary keys and not-None values.
                assert all([item in mapping.__dict__.keys() for item in ['key', 'path', 'types']])
                assert all([val is not None for _, val in mapping.__dict__.items()])

    def test_extract_param_dict(self):
        """ Test to extract the request url and parameters."""

        request_types = ["historic_rates", "currency_pairs"]

        for request_type in request_types:
            self.CryptoReader.param_dict = request_type

            assert isinstance(self.CryptoReader.param_dict, dict)
            assert request_type in self.CryptoReader.param_dict.keys()
            assert isinstance(self.CryptoReader.param_dict.get(request_type), dict)
            assert "url" in self.CryptoReader.param_dict.get(request_type).keys()

    def test_format_url_and_params(self):
        """ Test to correctly format the request url and parameters."""

        request_types = ["historic_rates", "currency_pairs"]

        for request_type in request_types:
            self.CryptoReader.param_dict = request_type
            self.CryptoReader.url_and_params = request_type

            assert isinstance(self.CryptoReader.url_and_params, dict)
            assert isinstance(self.CryptoReader.url, str)
            assert isinstance(self.CryptoReader.params, dict)

    def test_all_exchange_apis(self):
        """ Test if the API of every exchange is correctly implemented and functional."""
        # ToDo
        pass

    def test_extract_values_from_response(self):
        """ Tests to correctly extract the values from a specified response."""
        # ToDo
        pass

    def test_empty_response(self):
        """ Test the behavior for an valid but empty response."""
        resp = []
        with pytest.raises(EmptyResponseError):
            self.CryptoReader.format_data(resp)

    def test_format_data(self):
        """ Test to correctly extract the response values."""

        request_types = ["historic_rates", "currency_pairs"]

        for request_type in request_types:
            self.CryptoReader.param_dict = request_type
            self.CryptoReader.url_and_params = request_type

            resp = self.CryptoReader._get_data()
            data, mappings = self.CryptoReader.format_data(resp)

            assert isinstance(data, list)
            assert isinstance(mappings, list)
            assert all([len(item) == len(mappings) for item in data])

