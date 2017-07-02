import os
import pytest

from requests.exceptions import HTTPError
import pandas_datareader as pdr
import pandas_datareader.data as web

TEST_API_KEY = os.getenv('ENIGMA_API_KEY')


@pytest.mark.skipif(TEST_API_KEY is None, reason="no enigma_api_key")
class TestEnigma(object):

    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    def test_enigma_datareader(self):
        try:
            df = web.DataReader('enigma.inspections.restaurants.fl',
                                'enigma', access_key=TEST_API_KEY)
            assert 'serialid' in df.columns
        except HTTPError as e:
            pytest.skip(e)

    def test_enigma_get_data_enigma(self):
        try:
            df = pdr.get_data_enigma(
                'enigma.inspections.restaurants.fl', TEST_API_KEY)
            assert 'serialid' in df.columns
        except HTTPError as e:
            pytest.skip(e)

    def test_bad_key(self):
        with pytest.raises(HTTPError):
            web.DataReader('enigma.inspections.restaurants.fl',
                           'enigma', access_key=TEST_API_KEY + 'xxx')

    def test_bad_url(self):
        with pytest.raises(HTTPError):
            web.DataReader('enigma.inspections.restaurants.fllzzy',
                           'enigma', access_key=TEST_API_KEY)
