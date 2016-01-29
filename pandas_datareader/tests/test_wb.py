import nose
import time

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import requests

from pandas_datareader.wb import (search, download, get_countries,
                                  get_indicators, WorldBankReader)
from pandas_datareader._utils import PANDAS_0170, PANDAS_0160, PANDAS_0140


class TestWB(tm.TestCase):

    def test_wdi_search(self):

        # Test that a name column exists, and that some results were returned
        # ...without being too strict about what the actual contents of the
        # results actually are.  The fact that there are some, is good enough.

        result = search('gdp.*capita.*constant')
        self.assertTrue(result.name.str.contains('GDP').any())

        # check cache returns the results within 0.5 sec
        current_time = time.time()
        result = search('gdp.*capita.*constant')
        self.assertTrue(result.name.str.contains('GDP').any())
        self.assertTrue(time.time() - current_time < 0.5)

        result2 = WorldBankReader().search('gdp.*capita.*constant')
        session = requests.Session()
        result3 = search('gdp.*capita.*constant', session=session)
        result4 = WorldBankReader(session=session).search('gdp.*capita.*constant')
        for result in [result2, result3, result4]:
            self.assertTrue(result.name.str.contains('GDP').any())

    def test_wdi_download(self):

        # Test a bad indicator with double (US), triple (USA),
        # standard (CA, MX), non standard (KSV),
        # duplicated (US, US, USA), and unknown (BLA) country codes

        # ...but NOT a crash inducing country code (World bank strips pandas
        #    users of the luxury of laziness, because they create their
        #    own exceptions, and don't clean up legacy country codes.
        # ...but NOT a retired indicator (User should want it to error.)

        cntry_codes = ['CA', 'MX', 'USA', 'US', 'US', 'KSV', 'BLA']
        inds = ['NY.GDP.PCAP.CD', 'BAD.INDICATOR']

        expected = {'NY.GDP.PCAP.CD': {('Canada', '2004'): 31829.522562759001, ('Canada', '2003'): 28026.006013044702,
                                       ('Kosovo', '2004'): 2135.3328465238301, ('Kosovo', '2003'): 1969.56271307405,
                                       ('Mexico', '2004'): 7042.0247834044303, ('Mexico', '2003'): 6601.0420648056606,
                                       ('United States', '2004'): 41928.886136479705, ('United States', '2003'): 39682.472247320402}}
        expected = pd.DataFrame(expected)
        # Round, to ignore revisions to data.
        expected = np.round(expected, decimals=-3)
        if PANDAS_0170:
            expected = expected.sort_index()
        else:
            expected = expected.sort()

        result = download(country=cntry_codes, indicator=inds,
                          start=2003, end=2004, errors='ignore')
        if PANDAS_0170:
            result = result.sort_index()
        else:
            result = result.sort()
        # Round, to ignore revisions to data.
        result = np.round(result, decimals=-3)

        if PANDAS_0140:
            expected.index.names = ['country', 'year']
        else:
            # prior versions doesn't allow to set multiple names to MultiIndex
            # Thus overwrite it with the result
            expected.index = result.index
        tm.assert_frame_equal(result, expected)

        # pass start and end as string
        result = download(country=cntry_codes, indicator=inds,
                          start='2003', end='2004', errors='ignore')
        if PANDAS_0170:
            result = result.sort_index()
        else:
            result = result.sort()
        # Round, to ignore revisions to data.
        result = np.round(result, decimals=-3)
        tm.assert_frame_equal(result, expected)

    def test_wdi_download_str(self):

        expected = {'NY.GDP.PCAP.CD': {('Japan', '2004'): 36441.50449394,
                                       ('Japan', '2003'): 33690.93772972,
                                       ('Japan', '2002'): 31235.58818439,
                                       ('Japan', '2001'): 32716.41867489,
                                       ('Japan', '2000'): 37299.64412913}}
        expected = pd.DataFrame(expected)
        # Round, to ignore revisions to data.
        expected = np.round(expected, decimals=-3)
        if PANDAS_0170:
            expected = expected.sort_index()
        else:
            expected = expected.sort()

        cntry_codes = 'JP'
        inds = 'NY.GDP.PCAP.CD'
        result = download(country=cntry_codes, indicator=inds,
                          start=2000, end=2004, errors='ignore')
        if PANDAS_0170:
            result = result.sort_index()
        else:
            result = result.sort()
        result = np.round(result, decimals=-3)

        if PANDAS_0140:
            expected.index.names = ['country', 'year']
        else:
            # prior versions doesn't allow to set multiple names to MultiIndex
            # Thus overwrite it with the result
            expected.index = result.index

        tm.assert_frame_equal(result, expected)

        result = WorldBankReader(inds, countries=cntry_codes,
                                 start=2000, end=2004, errors='ignore').read()
        if PANDAS_0170:
            result = result.sort_index()
        else:
            result = result.sort()
        result = np.round(result, decimals=-3)
        tm.assert_frame_equal(result, expected)

    def test_wdi_download_error_handling(self):
        cntry_codes = ['USA', 'XX']
        inds = 'NY.GDP.PCAP.CD'

        with tm.assertRaisesRegexp(ValueError, "Invalid Country Code\\(s\\): XX"):
            result = download(country=cntry_codes, indicator=inds,
                              start=2003, end=2004, errors='raise')

        if PANDAS_0160:
            # assert_produces_warning doesn't exists in prior versions
            with self.assert_produces_warning():
                result = download(country=cntry_codes, indicator=inds,
                                  start=2003, end=2004, errors='warn')
                self.assertTrue(isinstance(result, pd.DataFrame))
                self.assertEqual(len(result), 2)

        cntry_codes = ['USA']
        inds = ['NY.GDP.PCAP.CD', 'BAD_INDICATOR']

        with tm.assertRaisesRegexp(ValueError, "The provided parameter value is not valid\\. Indicator: BAD_INDICATOR"):
            result = download(country=cntry_codes, indicator=inds,
                              start=2003, end=2004, errors='raise')

        if PANDAS_0160:
            with self.assert_produces_warning():
                result = download(country=cntry_codes, indicator=inds,
                                  start=2003, end=2004, errors='warn')
                self.assertTrue(isinstance(result, pd.DataFrame))
                self.assertEqual(len(result), 2)

    def test_wdi_download_w_retired_indicator(self):

        cntry_codes = ['CA', 'MX', 'US']
        # Despite showing up in the search feature, and being listed online,
        # the api calls to GDPPCKD don't work in their own query builder, nor
        # pandas module.  GDPPCKD used to be a common symbol.
        # This test is written to ensure that error messages to pandas users
        # continue to make sense, rather than a user getting some missing
        # key error, cause their JSON message format changed.  If
        # World bank ever finishes the deprecation of this symbol,
        # this nose test should still pass.

        inds = ['GDPPCKD']

        try:
            result = download(country=cntry_codes, indicator=inds,
                              start=2003, end=2004, errors='ignore')
        # If for some reason result actually ever has data, it's cause WB
        # fixed the issue with this ticker.  Find another bad one.
        except ValueError as e:
            raise nose.SkipTest("No indicators returned data: {0}".format(e))

        # if it ever gets here, it means WB unretired the indicator.
        # even if they dropped it completely, it would still get caught above
        # or the WB API changed somehow in a really unexpected way.
        if len(result) > 0:  # pragma: no cover
            raise nose.SkipTest("Invalid results")

    def test_wdi_download_w_crash_inducing_countrycode(self):

        cntry_codes = ['CA', 'MX', 'US', 'XXX']
        inds = ['NY.GDP.PCAP.CD']

        try:
            result = download(country=cntry_codes, indicator=inds,
                              start=2003, end=2004, errors='ignore')
        except ValueError as e:
            raise nose.SkipTest("No indicators returned data: {0}".format(e))

        # if it ever gets here, it means the country code XXX got used by WB
        # or the WB API changed somehow in a really unexpected way.
        if len(result) > 0:  # pragma: no cover
            raise nose.SkipTest("Invalid results")

    def test_wdi_get_countries(self):
        result1 = get_countries()
        result2 = WorldBankReader().get_countries()

        session = requests.Session()
        result3 = get_countries(session=session)
        result4 = WorldBankReader(session=session).get_countries()

        for result in [result1, result2, result3, result4]:
            self.assertTrue('Zimbabwe' in list(result['name']))
            self.assertTrue(len(result) > 100)
            self.assertTrue(pd.notnull(result.latitude.mean()))
            self.assertTrue(pd.notnull(result.longitude.mean()))

    def test_wdi_get_indicators(self):
        result1 = get_indicators()
        result2 = WorldBankReader().get_indicators()

        session = requests.Session()
        result3 = get_indicators(session=session)
        result4 = WorldBankReader(session=session).get_indicators()

        for result in [result1, result2, result3, result4]:
            exp_col = pd.Index(['id', 'name', 'source', 'sourceNote', 'sourceOrganization', 'topics'])
            # assert_index_equal doesn't exists
            self.assertTrue(result.columns.equals(exp_col))
            self.assertTrue(len(result) > 10000)


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)  # pragma: no cover
