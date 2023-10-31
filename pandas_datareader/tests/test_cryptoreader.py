#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test module for the classes:
    - CryptoReader,
    - Exchange,
    - Mapping.
"""
import itertools

import pytest
import datetime
import pandas as pd

from pandas_datareader.crypto import CryptoReader
from pandas_datareader.crypto_utils.mapping import Mapping
from pandas_datareader.crypto_utils.mapping import extract_mappings
from pandas_datareader.crypto_utils.utilities import (
    yaml_loader,
    sort_columns,
    replace_list_item,
    split_str_to_list,
)
from pandas_datareader.exceptions import EmptyResponseError


class TestCryptoReader:
    """ Unit tests for the CryptoReader."""

    exchange_name = "coinbase"
    symbols = "btc/usd"
    CryptoReader = CryptoReader(symbols=symbols, exchange_name=exchange_name)

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

        result = self.CryptoReader.read("eth/usd")

        assert "eth/usd" in self.CryptoReader.symbols.keys()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_reset_request_start_date(self):
        """ Test resetting the request start date."""

        key = list(self.CryptoReader.symbols.keys())[0]
        self.CryptoReader.symbols.update({key: datetime.datetime(2020, 1, 1)})
        self.CryptoReader.reset_request_start_date()

        assert (
            self.CryptoReader.symbols.get(key).date() == datetime.datetime.now().date()
        )

    def test_check_symbols(self):
        """ Test checking if the provided currency-pair is listed on an exchange."""

        with pytest.raises(KeyError):
            self.CryptoReader.symbols = {" / ": datetime.datetime.today()}
            self.CryptoReader.read()

    def test_check_wrong_splitting_symbol(self):
        """ Test checking if the provided currency-pair is listed on an exchange."""

        with pytest.raises(BaseException):
            self.CryptoReader.symbols = {" - ": datetime.datetime.today()}
            self.CryptoReader.read()

    def test_ensure_correct_column_names(self):
        """ Test to ensure specific column names."""

        response = pd.DataFrame(
            {
                "High": range(0, 5),
                "CLOSE": range(0, 5),
                "oPen": range(0, 5),
                "low": range(0, 5),
            }
        )
        response = sort_columns(dataframe=response)

        assert response.columns == "low"

    def test_index_and_cut_dataframe(self):
        """ Test to cut the response to the initially defined start/end dates."""

        response = pd.DataFrame(
            {
                "open": range(0, 100),
                "time": pd.period_range(end="2020-12-31", periods=100, freq="d"),
            }
        )

        self.CryptoReader.start = datetime.datetime(2020, 12, 1)
        self.CryptoReader.end = datetime.datetime(2020, 12, 20)

        response = self.CryptoReader._index_and_cut_dataframe(response)

        assert response.shape[0] == 20
        assert min(response.index).to_timestamp() == self.CryptoReader.start
        assert max(response.index).to_timestamp() == self.CryptoReader.end

    def test_get_currency_pairs(self):
        """ Test to retrieve all listed currency-pairs."""

        temp_reader = CryptoReader(exchange_name="coinbase")
        pairs = temp_reader.get_currency_pairs()

        assert isinstance(pairs, pd.DataFrame)
        assert pairs.empty is False


class TestExchange:
    """ Unit tests for the Exchange class."""

    exchange_name = "coinbase"
    symbols = "btc/usd"
    CryptoReader = CryptoReader(symbols=symbols, exchange_name=exchange_name)

    def test_correct_symbol_splitter(self):
        """ Test to check for the correct symbol splitter."""

        valid_symbols = ["btc-usd", "btc/usd", "btc-bitcoin/usd", "bitcoin/usd"]
        invalid_symbols = ["btc$usd", "btc_usd", "btc-bitcoin-usd", "btc/bitcoin/usd"]

        formatted_pairs = [
            self.CryptoReader.apply_currency_pair_format(cp) for cp in valid_symbols
        ]
        assert all(
            [isinstance(formatted_pair, str) for formatted_pair in formatted_pairs]
        )

        with pytest.raises(BaseException):
            [self.CryptoReader.apply_currency_pair_format(cp) for cp in invalid_symbols]

    def test_extract_mappings(self):
        """ Test to extract the mapping keys and values from the yaml files."""

        result = extract_mappings(
            self.CryptoReader.name, self.CryptoReader.yaml_file.get("requests")
        ).get("historic_rates")

        assert isinstance(result, list)
        assert result

    def test_all_exchanges_have_mappings_and_necessary_values(self):
        """ Test if all exchange yaml-files have a specified mapping."""

        exchanges = self.CryptoReader.get_all_exchanges()
        assert isinstance(exchanges, list)
        assert exchanges

        for exchange in exchanges:
            file = yaml_loader(exchange)
            result = extract_mappings(exchange, file.get("requests")).get(
                "historic_rates"
            )
            assert isinstance(result, list)
            assert result

    def test_necessary_values_in_mappings(self):
        """ Test if all necessary values are in the mappings."""

        exchanges = self.CryptoReader.get_all_exchanges()

        for exchange in exchanges:
            file = yaml_loader(exchange)
            mappings = extract_mappings(exchange, file.get("requests")).get(
                "historic_rates"
            )

            for mapping in mappings:
                # Check if the object dict contains all necessary keys
                # and not-None values.
                assert all(
                    [
                        item in mapping.__dict__.keys()
                        for item in ["key", "path", "types"]
                    ]
                )
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


class TestMapping:
    """ Test class for Mapping."""

    # pylint: disable=too-many-public-methods

    def test_extract_value_split_index_zero(self):
        """ Test of splitting a str and taking the index zero."""

        mapping = Mapping("exchange_name", ["product_id"], ["str", "split", "-", 0])
        result = mapping.extract_value({"product_id": "BTC-ETH"})
        assert result == "BTC"

    def test_extract_value_split_index_one(self):
        """ Test of splitting a str and taking the index one."""

        mapping = Mapping("exchange_name", ["product_id"], ["str", "split", "-", 1])
        result = mapping.extract_value({"product_id": "BTC-ETH"})
        assert result == "ETH"

    def test_extract_value_split_index_two(self):
        """ Test of splitting a str and taking the index two."""

        mapping = Mapping("exchange_name", ["product_id"], ["str", "split", "-", 2])
        result = mapping.extract_value({"product_id": "BTC-ETH-USD"})
        assert result == "USD"

    def test_extract_value_slice_first_half(self):
        """ Test of slicing a str and taking the first half."""

        mapping = Mapping("exchange_name", ["product_id"], ["str", "slice", 0, 3])
        result = mapping.extract_value({"product_id": "BTCETH"})
        assert result == "BTC"

    def test_extract_value_slice_second_half(self):
        """ Test of slicing a str and taking the second half."""

        mapping = Mapping("exchange_name", ["product_id"], ["str", "slice", 3, 6])
        result = mapping.extract_value({"product_id": "BTCETH"})
        assert result == "ETH"

    def test_extract_value_str_to_bool_true(self):
        """ Test of conversion from str to bool in case of True."""

        mapping = Mapping("active", ["active"], ["str", "bool"])

        result = mapping.extract_value({"active": "True"})

        assert isinstance(result, bool)
        assert result

    def test_extract_value_str_to_bool_true_lowercase(self):
        """ Test of conversion from str to bool in case of lowercase True."""

        mapping = Mapping("active", ["active"], ["str", "bool"])

        result = mapping.extract_value({"active": "true"})

        assert isinstance(result, bool)
        assert result

    def test_extract_value_str_to_bool_true_uppercase(self):
        """ Test of conversion from str to bool in case of uppercase True."""

        mapping = Mapping("active", ["active"], ["str", "bool"])

        result = mapping.extract_value({"active": "TRUE"})

        assert isinstance(result, bool)
        assert result

    def test_extract_value_str_to_bool_false(self):  #
        """ Test of conversion from str to bool in case of False."""

        mapping = Mapping("active", ["active"], ["str", "bool"])

        result = mapping.extract_value({"active": "False"})

        assert isinstance(result, bool)
        assert not result

    def test_extract_value_str_to_bool_false_lowercase(self):
        """ Test of conversion from str to bool in case of lowercase False."""

        mapping = Mapping("active", ["active"], ["str", "bool"])

        result = mapping.extract_value({"active": "false"})

        assert isinstance(result, bool)
        assert not result

    def test_extract_value_str_to_bool_false_uppercase(self):
        """ Test of conversion from str to bool in case of uppercase False."""

        mapping = Mapping("active", ["active"], ["str", "bool"])

        result = mapping.extract_value({"active": "FALSE"})

        assert isinstance(result, bool)
        assert not result

    def test_extract_value_str_to_bool_false_anything(self):
        """ Test of conversion from str to bool in case of anything."""

        mapping = Mapping("active", ["active"], ["str", "bool"])

        result = mapping.extract_value({"active": "anything"})

        assert isinstance(result, bool)
        assert not result

    def test_extract_value_bool_to_int_true(self):
        """ Test of conversion from bool to int in case of True."""

        mapping = Mapping("active", ["active"], ["bool", "int"])

        result = mapping.extract_value({"active": True})

        assert isinstance(result, int)
        assert result == 1

    def test_extract_value_bool_to_int_false(self):
        """ Test of conversion from bool to int in case of False."""

        mapping = Mapping("active", ["active"], ["bool", "int"])

        result = mapping.extract_value({"active": False})

        assert isinstance(result, int)
        assert result == 0

    def test_extract_value_int_to_bool_one(self):
        """ Test of conversion from int to bool in case of one."""

        mapping = Mapping("active", ["active"], ["int", "bool"])

        result = mapping.extract_value({"active": 1})

        assert isinstance(result, bool)
        assert result

    def test_extract_value_int_to_bool_zero(self):
        """ Test of conversion from int to bool in case of zero."""

        mapping = Mapping("active", ["active"], ["int", "bool"])

        result = mapping.extract_value({"active": 0})

        assert isinstance(result, bool)
        assert not result

    def test_extract_value_int_to_bool_two(self):
        """ Test of conversion from int to bool in case of two."""

        mapping = Mapping("active", ["active"], ["int", "bool"])

        result = mapping.extract_value({"active": 2})

        assert isinstance(result, bool)
        assert result

    def test_extract_value_int_to_bool_neg_two(self):
        """ Test of conversion from int to bool in case of negative two."""

        mapping = Mapping("active", ["active"], ["int", "bool"])

        result = mapping.extract_value({"active": -2})

        assert isinstance(result, bool)
        assert result

    def test_extract_value_int_fromtimestamp(self):
        """ Test of conversion from an int timestamp to datetime."""

        mapping = Mapping("time", ["time"], ["float", "from_timestamp", "0"])

        result = mapping.extract_value({"time": 1538122622})

        assert isinstance(result, datetime.datetime)
        # Different results depending on timezone
        # self.assertEqual(result, datetime.datetime(2018, 9, 28, 8, 17, 2))

    def test_extract_value_int_utcfromtimestamp(self):
        """ Test of conversion from an int UTC timestamp to datetime."""

        mapping = Mapping("time", ["time"], ["float", "from_timestamp", "1"])

        result = mapping.extract_value({"time": 1538122622})

        assert isinstance(result, datetime.datetime)

    def test_extract_value_int_fromtimestampms(self):
        """ Test of conversion from an int timestamp with ms to datetime."""

        mapping = Mapping("time", ["time"], ["float", "from_timestamp", "1"])

        result = mapping.extract_value({"time": 1538122622123})

        assert isinstance(result, datetime.datetime)
        assert result.microsecond == 123000

    def test_extract_value_float_fromtimestamp(self):
        """ Test of conversion from a float timestamp to datetime."""

        mapping = Mapping("time", ["time"], ["float", "from_timestamp", "0"])

        result = mapping.extract_value({"time": 1538122622.123})

        assert isinstance(result, datetime.datetime)
        assert result.microsecond == 123000

    def test_extract_value_float_utcfromtimestamp(self):
        """ Test of conversion from a float UTC timestamp to datetime."""

        mapping = Mapping("time", ["time"], ["float", "from_timestamp", "0"])

        result = mapping.extract_value({"time": 1538122622.123})

        assert isinstance(result, datetime.datetime)
        assert result.microsecond == 123000

    def test_extract_value_str_to_int_zero(self):
        """ Test of conversion from str to int in case of zero."""

        mapping = Mapping("number", ["number"], ["str", "int"])

        result = mapping.extract_value({"number": "0"})

        assert isinstance(result, int)
        assert result == 0

    def test_extract_value_str_to_int_one(self):
        """ Test of conversion from str to int in case of one."""

        mapping = Mapping("number", ["number"], ["str", "int"])

        result = mapping.extract_value({"number": "1"})

        assert isinstance(result, int)
        assert result == 1

    def test_extract_value_str_to_int_two(self):
        """ Test of conversion from str to int in case of two."""

        mapping = Mapping("number", ["number"], ["str", "int"])

        result = mapping.extract_value({"number": "2"})

        assert isinstance(result, int)
        assert result == 2

    def test_extract_value_str_to_int_twelve(self):
        """ Test of conversion from str to int in case of twelve."""

        mapping = Mapping("number", ["number"], ["str", "int"])

        result = mapping.extract_value({"number": "12"})

        assert isinstance(result, int)
        assert result == 12

    def test_extract_value_str_to_int_neg_one(self):
        """ Test of conversion from str to int in case negative one."""

        mapping = Mapping("number", ["number"], ["str", "int"])

        result = mapping.extract_value({"number": "-1"})

        assert isinstance(result, int)
        assert result == -1

    def test_extract_value_str_to_float_zero(self):
        """ Test of conversion from str to float in case of zero."""

        mapping = Mapping("number", ["number"], ["str", "float"])

        result = mapping.extract_value({"number": "0.0"})

        assert isinstance(result, float)
        assert result == 0.0

    def test_extract_value_str_to_float_one(self):
        """ Test of conversion from str to float in case of one."""

        mapping = Mapping("number", ["number"], ["str", "float"])

        result = mapping.extract_value({"number": "1.0"})

        assert isinstance(result, float)
        assert result == 1.0

    def test_extract_value_str_to_float_two(self):
        """ Test of conversion from str to float in case of two."""

        mapping = Mapping("number", ["number"], ["str", "float"])

        result = mapping.extract_value({"number": "2.0"})

        assert isinstance(result, float)
        assert result == 2.0

    def test_extract_value_str_to_float_twelve(self):
        """ Test of conversion from str to float in case of twelve."""

        mapping = Mapping("number", ["number"], ["str", "float"])

        result = mapping.extract_value({"number": "12.0"})

        assert isinstance(result, float)
        assert result == 12.0

    def test_extract_value_str_to_float_pi(self):
        """ Test of conversion from str to float in case of Pi."""

        mapping = Mapping("number", ["number"], ["str", "float"])

        result = mapping.extract_value({"number": "3.141592654"})

        assert isinstance(result, float)
        assert result == 3.141592654

    def test_extract_value_str_to_float_neg_one(self):
        """ Test of conversion from str to float in case of negative one."""

        mapping = Mapping("number", ["number"], ["str", "float"])

        result = mapping.extract_value({"number": "-1.0"})

        assert isinstance(result, float)
        assert result == -1.0

    def test_extract_value_str_to_float_neg_pi(self):
        """ Test of conversion from str to float in case of negative Pi."""

        mapping = Mapping("number", ["number"], ["str", "float"])

        result = mapping.extract_value({"number": "-3.141592654"})

        assert isinstance(result, float)
        assert result == -3.141592654

    def test_extract_value_datetime_totimestamp(self):
        """ Test of conversion from datetome to timestamp."""

        mapping = Mapping("date", ["date"], ["datetime", "totimestamp"])

        result = mapping.extract_value(
            {"date": datetime.datetime(2018, 10, 11, 11, 20)}
        )

        assert isinstance(result, int)
        # Different results depending on timezone
        # self.assertEqual(result, 1539249600)

    def test_extract_value_datetime_totimestampms(self):
        """ Test of conversion from datetome to timestamp with ms."""

        mapping = Mapping("date", ["date"], ["datetime", "totimestampms"])

        result = mapping.extract_value(
            {"date": datetime.datetime(2018, 10, 11, 11, 20, 0, 123000)}
        )

        assert isinstance(result, int)
        # Different results depending on timezone
        # self.assertEqual(result, 1539249600123)

    def test_extract_value_datetime_utctotimestamp(self):
        """ Test of conversion from datetome to UTC timestamp."""

        mapping = Mapping("date", ["date"], ["datetime", "utctotimestamp"])

        result = mapping.extract_value(
            {"date": datetime.datetime(2018, 10, 11, 11, 20, 0)}
        )

        assert isinstance(result, int)
        # Different results depending on timezone

    def test_extract_value_str_strptime_date(self):
        """ Test of conversion timestring via strptime in case of a date."""

        mapping = Mapping("date", ["date"], ["str", "strptime", "%Y-%m-%d"])

        result = mapping.extract_value({"date": "2018-10-11"})

        assert isinstance(result, datetime.datetime)
        assert result == datetime.datetime(2018, 10, 11)

    def test_extract_value_str_strptime_datetime(self):
        """ Test of conversion timestring via strptime in case of a datetime."""

        mapping = Mapping("date", ["date"], ["str", "strptime", "%Y-%m-%d %H:%M"])

        result = mapping.extract_value({"date": "2018-10-11 12:06"})

        assert isinstance(result, datetime.datetime)
        assert result == datetime.datetime(2018, 10, 11, 12, 6)

    def test_extract_value_datetime_strftime_date(self):
        """ Test of conversion from datetime via strftime in case of a date."""

        mapping = Mapping("date", ["date"], ["datetime", "strftime", "%Y-%m-%d"])

        result = mapping.extract_value({"date": datetime.datetime(2018, 10, 11)})

        assert isinstance(result, str)
        assert result == "2018-10-11"

    def test_extract_value_datetime_strftime_datetime(self):
        """ Test of conversion from datetime via strftime in case of a dt."""

        mapping = Mapping("date", ["date"], ["datetime", "strftime", "%Y-%m-%d %H:%M"])

        result = mapping.extract_value({"date": datetime.datetime(2018, 10, 11, 12, 6)})

        assert isinstance(result, str)
        assert result == "2018-10-11 12:06"

    def test_extract_value_dict_key(self):
        """ Test of extract value for dict_keys without further processing."""

        mapping = Mapping("pair", ["dict_key"], ["str"])

        result = mapping.extract_value({"BTC_USD": "value", "ETH_EUR": "value"})

        assert isinstance(result, list)
        assert result == ["BTC_USD", "ETH_EUR"]

    def test_extract_value_dict_key_split_index_zero(self):
        """ Test of extract value for dict_keys with split and index 0."""

        mapping = Mapping("pair", ["dict_key"], ["str", "split", "_", 0])

        result = mapping.extract_value({"BTC_USD": "value", "ETH_EUR": "value"})

        assert isinstance(result, list)
        assert result == ["BTC", "ETH"]

    def test_extract_value_dict_key_split_index_one(self):
        """ Test of extract value for dict_keys with split and index 1."""

        mapping = Mapping("pair", ["dict_key"], ["str", "split", "_", 1])

        result = mapping.extract_value({"BTC_USD": "value", "ETH_EUR": "value"})

        assert isinstance(result, list)
        assert result == ["USD", "EUR"]

    def test_extract_value_where_pair_can_not_be_split(self):
        """ Test extract value for case where value is currency pair but cannot
        be extracted since there is no delimiter for splitting the pair."""
        mapping = Mapping("first", [], ["first_currency"])
        result = mapping.extract_value(
            ["ETHBTC", "XRPBTC"], currency_pair_info=("XRP", "BTC", "XRPBTC")
        )
        assert result == "XRP"

        mapping = Mapping("second", [], ["second_currency"])
        result = mapping.extract_value(
            ["ETHBTC", "XRPBTC"], currency_pair_info=("ETH", "BTC", "ETHBTC")
        )
        assert result == "BTC"

    def test_extract_value_list(self):
        """Test of extracting all elements in a list."""
        mapping = Mapping("number_as_string", [[]], ["str"])
        value_list = ["1.0", "1.1", "1.2", "1.3", "2.4", "2.5", "3.6", "3.7"]
        result = mapping.extract_value(value_list)

        assert value_list == result

    def test_extract_value_list_containing_dict_with_key(self):
        """ Test of extract value for a list that contains a dict for each
        index with a known key."""

        # list element directly holds value behind key
        mapping = Mapping("first_currency", ["first"], ["str"])

        extract_list = [
            {"first": "BTC"},
            {"first": "ETH"},
            {"first": "XRP"},
            {"first": "DOGE"},
            {"first": "XSA"},
            {"first": "TEST"},
        ]
        result = mapping.extract_value(extract_list)
        value_list = ["BTC", "ETH", "XRP", "DOGE", "XSA", "TEST"]
        assert value_list == result

        # list element holds dict that holds another dict which holds the
        # value behind key
        mapping = Mapping(
            "first_currency",
            [
                "a",
                "first",
            ],  # ['dict_values', 'first'] works also since it's only one level
            ["str"],
        )

        extract_list = [
            {"a": {"first": "BTC", "second": "should not matter"}},
            {"a": {"first": "ETH", "second": "should not matter"}},
            {"a": {"first": "XRP", "second": "should not matter"}},
            {"a": {"first": "DOGE", "second": "should not matter"}},
            {"a": {"first": "XSA", "second": "should not matter"}},
            {"a": {"first": "TEST", "second": "should not matter"}},
        ]
        result = mapping.extract_value(extract_list)
        assert value_list == result

        # list element with few more levels of dictionaries
        mapping = Mapping("first_currency", ["a", "b", "c", "d", "first"], ["str"])

        extract_list = [
            {
                "a": {
                    "b": {
                        "c": {
                            "d": {"first": "BTC", "second": "should not matter"},
                            "dist": "other_value",
                        }
                    }
                }
            },
            {
                "a": {
                    "b": {
                        "c": {"d": {"first": "ETH", "second": "should not matter"}},
                        "dist": "other_value",
                    }
                }
            },
            {
                "a": {
                    "b": {"c": {"d": {"first": "XRP", "second": "should not matter"}}},
                    "dist": "other_value",
                }
            },
            {
                "a": {
                    "b": {"c": {"d": {"first": "DOGE", "second": "should not matter"}}}
                },
                "dist": "other_value",
            },
            {
                "a": {
                    "b": {"c": {"d": {"first": "XSA", "second": "should not matter"}}},
                    "dist": "other_value",
                }
            },
            {
                "a": {
                    "b": {
                        "c": {"d": {"first": "TEST", "second": "should not matter"}},
                        "dist": "other_value",
                    }
                }
            },
        ]
        result = mapping.extract_value(extract_list)
        assert value_list == result

    def test_extract_values_dict_values(self):
        """ Test of extract values for the special case that each key of a
        dictionary holds the values."""
        mapping = Mapping("first_currency", ["dict_values"], ["str"])

        extract_dict = {"1": "XRP", 2: "ETH", "3": "BTC", 4: "DOGE"}
        result = mapping.extract_value(extract_dict)
        value_list = ["XRP", "ETH", "BTC", "DOGE"]
        assert value_list == result

        # extracting dicts with dict_values -> aka flattening the json
        mapping = Mapping("first_currency", ["dict_values", "first"], ["str"])

        extract_dict = {
            "1": {"first": "XRP", "second": "Whatever"},
            2: {"first": "ETH", "second": "Whatever"},
            "3": {"first": "BTC", "second": "Whatever"},
            4: {"first": "DOGE", "second": "Whatever"},
        }
        result = mapping.extract_value(extract_dict)
        value_list = ["XRP", "ETH", "BTC", "DOGE"]
        assert value_list == result

        # How to not use dict_values, since other values will be filtered also
        mapping = Mapping(
            "first_currency",
            ["dict_values", "dict_values", "dict_values", "dict_values", "first"],
            ["str"],
        )
        extract_list = [
            {
                "a": {
                    "b": {
                        "c": {
                            "d": {"first": "BTC", "second": "should not matter"},
                            "dist": "other_value",
                        }
                    }
                }
            },
            {
                "a": {
                    "b": {
                        "c": {"d": {"first": "ETH", "second": "should not matter"}},
                        "dist": "other_value",
                    }
                }
            },
            {
                "a": {
                    "b": {"c": {"d": {"first": "XRP", "second": "should not matter"}}},
                    "dist": "other_value",
                }
            },
            {
                "a": {
                    "b": {"c": {"d": {"first": "DOGE", "second": "should not matter"}}}
                },
                "dist": "other_value",
            },
            {
                "a": {
                    "b": {"c": {"d": {"first": "XSA", "second": "should not matter"}}},
                    "dist": "other_value",
                }
            },
            {
                "a": {
                    "b": {
                        "c": {"d": {"first": "TEST", "second": "should not matter"}},
                        "dist": "other_value",
                    }
                }
            },
        ]

        with pytest.raises(TypeError):
            mapping.extract_value(extract_list)

    def test_extract_value_list_containing_dict_where_key_is_value(self):
        """ Test of extract value for a list that contains a dict that contains a dict.
        The first dict only has one key, which is the value we search for."""
        mapping = Mapping("second_currency", ["dict_key"], ["str", "split", "_", 1])
        extract_list = [
            {"btc_eth": {"other": "values"}},
            {"btc_eur": {"other": "values"}},
            {"btc_usd": {"other": "values"}},
            {"btc_usdt": {"other": "values"}},
        ]
        value_list = ["eth", "eur", "usd", "usdt"]
        result = mapping.extract_value(extract_list)
        assert value_list == result

    def test_extract_value_list_containing_dict_where_pair_is_key_to_values(self):
        """ Test of extract value for a list that contains dicts that hold
        a single dict. The key is the formatted currency pair that is needed
        to gain access to the value."""
        mapping = Mapping("value", ["currency_pair", 1], ["str", "float"])
        extract_dict = {
            "btc_eth": {1: "123.456"},
            "btc_xrp": {1: "789.101"},
            "xrp_eth": {1: "112.131"},
            "doge_btc": {1: "415.161"},
        }
        value_list = [415.161, 112.131, 789.101, 123.456]

        # note that currency pair infos are from bottom to top
        # if compared to extract_dict
        currency_pair_infos = (
            ("doge", "BTC", "doge_btc"),
            ("xrp", "eth", "xrp_eth"),
            ("btc", "xrp", "btc_xrp"),
            ("btc", "eth", "btc_eth"),
        )

        result = []
        for currency_pair_info in currency_pair_infos:
            result.append(
                mapping.extract_value(
                    extract_dict, currency_pair_info=currency_pair_info
                )
            )
        assert value_list == result

    def test_extract_value_dict_containing_list(self):
        """ Test of extract value where the response is a dict and the
        known key contains a list of the values."""
        mapping = Mapping("first_currency", ["data"], ["str", "split", "_", 0])
        extract_dict = {
            "data": ["eth_btc", "usd_btc", "xrp_eth", "eth_xrp"],
            "other_stuff": "that does not matter",
        }
        value_list = ["eth", "usd", "xrp", "eth"]
        result = mapping.extract_value(extract_dict)
        assert value_list == result

    def test_extract_value_dict_containing_list_containing_dict_with_value(self):
        """ Test of extract value where the response is a dict and
         a known key contains a list of dicts that hold the value."""

        mapping = Mapping("value", ["data", "value"], ["str", "float"])
        extract_dict = {
            "data": [
                {"value": "1.12"},
                {"value": "1.44"},
                {"value": "1.34"},
                {"value": "1.89"},
            ]
        }
        value_list = [1.12, 1.44, 1.34, 1.89]
        result = mapping.extract_value(extract_dict)
        assert value_list == result

    def test_extract_value_dict_containing_list_containing_dict_where_key_is_value(
        self,
    ):
        """ Test of extract value where the response is a dict and a known key
        contains a list of dicts with a single key that is the value."""
        mapping = Mapping(
            "second_currency", ["data", "dict_key"], ["str", "split", "_", 1]
        )
        extract_dict = {
            "data": [
                {"eth_btc": {"other": "values"}},
                {"btc_xrp": {"other": "values"}},
                {"tsla_usd": {"other": "values"}},
                {"usdt_eth": {"other": "values"}},
            ]
        }
        value_list = ["btc", "xrp", "usd", "eth"]
        result = mapping.extract_value(extract_dict)
        assert value_list == result


class TestUtilities:
    """ Test class for the utility functions."""

    def test_yaml_loader(self):
        """ Test the yaml-file loader. """

        with pytest.raises(ValueError):
            # Should except the FileNotFoundError and raise a ValueError instead.
            yaml_loader("some_unsupported_exchange")
            # Raise a ValueError directly.
            yaml_loader()

    def test_replace_list_item(self):
        """ Test to replace a specific value form a list."""

        condition = 5
        replace_list = [1, 5, 6, 7, 8, 9]
        new_list = replace_list_item(replace_list, condition, 999)

        assert new_list == [1, 999, 6, 7, 8, 9]

    def test_split_str_to_list(self):
        """ Test split a string into a list"""

        string = "btc- usd, btc-usd,btc - usd,  btc-u s d  ,  btc-usd"
        list_values = split_str_to_list(string)

        assert list_values == list(itertools.repeat("btc-usd", 5))

    def test_sort_columns(self):
        """ Test to sort the columns of the response."""

        ordered_cols = ["open", "high", "low", "close"]
        response = pd.DataFrame(
            {
                "high": range(0, 5),
                "close": range(0, 5),
                "open": range(0, 5),
                "low": range(0, 5),
            }
        )
        response = sort_columns(dataframe=response)

        assert all(ordered_cols == response.columns)
