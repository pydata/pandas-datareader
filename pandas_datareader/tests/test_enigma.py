import os

import requests
from requests.exceptions import HTTPError

import nose
import pandas.util.testing as tm
from pandas.util.testing import (assert_series_equal, assert_frame_equal)
from pandas_datareader.tests._utils import _skip_if_no_lxml

import pandas_datareader.data as web
import pandas_datareader as pdr

TEST_API_KEY = os.getenv('ENIGMA_API_KEY')


class TestEnigma(tm.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestEnigma, cls).setUpClass()
        _skip_if_no_lxml()

    def test_enigma(self):
        self.assertTrue('serialid' in list(
            web.DataReader('enigma.inspections.restaurants.fl',
                           'enigma',
                           access_key=TEST_API_KEY).columns))
        self.assertTrue('serialid' in list(pdr.get_data_enigma(
            'enigma.inspections.restaurants.fl', TEST_API_KEY)))

    def test_bad_key(self):
        _exception = None
        try:
            web.DataReader('enigma.inspections.restaurants.fl',
                           'enigma',
                           access_key=TEST_API_KEY + 'xxx')
        except HTTPError as e:
            _exception = e
        assert isinstance(_exception, HTTPError)

    def test_bad_url(self):
        _exception = None
        try:
            web.DataReader('enigma.inspections.restaurants.fllzzy',
                           'enigma',
                           access_key=TEST_API_KEY)
        except Exception as e:
            _exception = e
        assert isinstance(_exception, HTTPError)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
