import os
import pytest

from requests.exceptions import HTTPError

import pandas_datareader as pdr
import pandas_datareader.data as web
from pandas_datareader._testing import skip_on_exception

TEST_API_KEY = os.getenv('ENIGMA_API_KEY')


class TestEnigma(object):

    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    @skip_on_exception(HTTPError)
    def test_enigma_datareader(self):
        df = web.DataReader('enigma.inspections.restaurants.fl',
                            'enigma', access_key=TEST_API_KEY)
        assert 'serialid' in df.columns

    @skip_on_exception(HTTPError)
    def test_enigma_get_data_enigma(self):
        df = pdr.get_data_enigma(
            'enigma.inspections.restaurants.fl', TEST_API_KEY)
        assert 'serialid' in df.columns

    def test_bad_key(self):
        with pytest.raises(HTTPError):
            web.DataReader('enigma.inspections.restaurants.fl',
                           'enigma', access_key=TEST_API_KEY + 'xxx')

    def test_bad_url(self):
        with pytest.raises(HTTPError):
            web.DataReader('enigma.inspections.restaurants.fllzzy',
                           'enigma', access_key=TEST_API_KEY)
