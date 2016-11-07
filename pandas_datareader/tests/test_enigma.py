import os

from requests.exceptions import HTTPError

import nose
import pandas.util.testing as tm
from pandas_datareader.tests._utils import _skip_if_no_lxml

import pandas_datareader.data as web
import pandas_datareader as pdr

TEST_API_KEY = os.getenv('ENIGMA_API_KEY')


class TestEnigma(tm.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestEnigma, cls).setUpClass()
        _skip_if_no_lxml()

    def test_enigma_datareader(self):
        try:
            df = web.DataReader('enigma.inspections.restaurants.fl',
                                'enigma', access_key=TEST_API_KEY)
            self.assertTrue('serialid' in df.columns)
        except HTTPError as e:  # pragma: no cover
            raise nose.SkipTest(e)

    def test_enigma_get_data_enigma(self):
        try:
            df = pdr.get_data_enigma(
                'enigma.inspections.restaurants.fl', TEST_API_KEY)
            self.assertTrue('serialid' in df.columns)
        except HTTPError as e:  # pragma: no cover
            raise nose.SkipTest(e)

    def test_bad_key(self):
        with tm.assertRaises(HTTPError):
            web.DataReader('enigma.inspections.restaurants.fl',
                           'enigma',
                           access_key=TEST_API_KEY + 'xxx')

    def test_bad_url(self):
        with tm.assertRaises(HTTPError):
            web.DataReader('enigma.inspections.restaurants.fllzzy',
                           'enigma',
                           access_key=TEST_API_KEY)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
