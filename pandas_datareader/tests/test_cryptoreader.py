import pytest
import pandas as pd

from pandas_datareader.crypto import CryptoReader
from pandas_datareader.crypto_utils.mapping import extract_mappings
from pandas_datareader.crypto_utils.utilities import yaml_loader


class TestCryptoReader:
    """Test class for the CryptoReader."""

    exchange_name = "coinbase"
    symbols = "btc-usd"
    kwargs = {"interval": "days"}
    CryptoReader = CryptoReader(exchange_name, symbols, **kwargs)

    def test_get_all_exchanges(self):
        """ Test to return a list of all available exchanges."""

        result = self.CryptoReader.get_all_exchanges()
        assert isinstance(result, list)

    def test_read(self):
        """ Test the request from a particular exchange"""

        result = self.CryptoReader.read()

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_request_new_symbol(self):
        """ Test to request NEW symbols"""

        result = self.CryptoReader.read('eth-usd')

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_extract_mappings(self):
        """ Test to extract the mapping keys and values from the yaml files"""

        result = extract_mappings(self.CryptoReader.name,
                                  self.CryptoReader.yaml_file.get('requests')).get('historic_rates')

        assert isinstance(result, list)
        assert result
        # ToDo: Check for necessary values in mappings.

    def test_all_exchanges_have_mappings(self):
        """ Test if all exchange yaml-files have a specified mapping."""

        exchanges = self.CryptoReader.get_all_exchanges()
        assert isinstance(exchanges, list)
        assert exchanges

        for exchange in exchanges:
            file = yaml_loader(exchange)
            result = extract_mappings(exchange, file.get('requests')).get('historic_rates')

            assert isinstance(result, list)
            assert result

    def test_extract_values_from_response(self):
        """ Tests to correctly extract the values from a specified response"""

        pass

    def test_iterate_requests_until_no_further_timestamp(self):
        """ Tests to iterate the request with updated timestamps until no more timestamp is collected
            or start time is reached.
        """

        pass

    def test_empty_response(self):
        """ Test the behavior for an valid but empty response"""

        pass
