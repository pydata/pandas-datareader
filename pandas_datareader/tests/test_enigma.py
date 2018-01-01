import os
import pytest

from requests.exceptions import HTTPError
import pandas_datareader as pdr
import pandas_datareader.data as web

TEST_API_KEY = os.getenv('ENIGMA_API_KEY')


@pytest.mark.skipif(TEST_API_KEY is None, reason="no enigma_api_key")
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
            df = web.DataReader(self.dataset_id,
                                'enigma', access_key=TEST_API_KEY)
            assert 'case_number' in df.columns
        except HTTPError as e:
            pytest.skip(e)

    def test_enigma_get_data_enigma(self):
        try:
            df = pdr.get_data_enigma(self.dataset_id, TEST_API_KEY)
            assert 'case_number' in df.columns
        except HTTPError as e:
            pytest.skip(e)

    def test_bad_key(self):
        with pytest.raises(HTTPError):
            web.DataReader(self.dataset_id,
                           'enigma', access_key=TEST_API_KEY + 'xxx')

    def test_bad_dataset_id(self):
        with pytest.raises(HTTPError):
            web.DataReader('zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzz',
                           'enigma', access_key=TEST_API_KEY)
