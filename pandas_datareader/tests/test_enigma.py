import pytest
from requests.exceptions import HTTPError

import pandas_datareader as pdr
import pandas_datareader.data as web
from pandas_datareader.exceptions import ImmediateDeprecationError

pytestmark = pytest.mark.requires_api_key

TEST_API_KEY = "DEPRECATED"


class TestEnigma(object):
    @property
    def dataset_id(self):
        """
        USDA Food Recall Archive - 1996
        Selected for being a relatively small dataset.
        https://public.enigma.com/datasets/292129b0-1275-44c8-a6a3-2a0881f24fe1
        """
        return "292129b0-1275-44c8-a6a3-2a0881f24fe1"

    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    def test_enigma_datareader(self):
        try:
            with pytest.raises(ImmediateDeprecationError):
                web.DataReader(self.dataset_id, "enigma", api_key=TEST_API_KEY)
        except HTTPError as e:
            pytest.skip(e)

    def test_enigma_get_data_enigma(self):
        try:
            with pytest.raises(ImmediateDeprecationError):
                pdr.get_data_enigma(self.dataset_id, TEST_API_KEY)
        except HTTPError as e:
            pytest.skip(e)

    def test_bad_key(self):
        with pytest.raises(ImmediateDeprecationError):
            web.DataReader(self.dataset_id, "enigma", api_key=TEST_API_KEY + "xxx")

    def test_bad_dataset_id(self):
        with pytest.raises(ImmediateDeprecationError):
            web.DataReader(
                "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzz", "enigma", api_key=TEST_API_KEY
            )
