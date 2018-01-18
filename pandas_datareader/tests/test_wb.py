import time

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest
import requests

from pandas_datareader.compat import assert_raises_regex
from pandas_datareader.wb import (search, download, get_countries,
                                  get_indicators, WorldBankReader)


class TestWB(object):

    def test_wdi_search(self):

        # Test that a name column exists, and that some results were returned
        # ...without being too strict about what the actual contents of the
        # results actually are.  The fact that there are some, is good enough.

        result = search('gdp.*capita.*constant')
        assert result.name.str.contains('GDP').any()

        # check cache returns the results within 0.5 sec
        current_time = time.time()
        result = search('gdp.*capita.*constant')
        assert result.name.str.contains('GDP').any()
        assert time.time() - current_time < 0.5

        result2 = WorldBankReader().search('gdp.*capita.*constant')
        session = requests.Session()

        result3 = search('gdp.*capita.*constant', session=session)
        result4 = WorldBankReader(session=session).search(
            'gdp.*capita.*constant')

        for result in [result2, result3, result4]:
            assert result.name.str.contains('GDP').any()

    def test_wdi_download(self):

        # Test a bad indicator with double (US), triple (USA),
        # standard (CA, MX), non standard (KSV),
        # duplicated (US, US, USA), and unknown (BLA) country codes

        # ...but NOT a crash inducing country code (World Bank strips pandas
        #    users of the luxury of laziness because they create their
        #    own exceptions, and don't clean up legacy country codes.
        # ...but NOT a retired indicator (user should want it to error).

        cntry_codes = ['CA', 'MX', 'USA', 'US', 'US', 'KSV', 'BLA']
        inds = ['NY.GDP.PCAP.CD', 'BAD.INDICATOR']

        # These are the expected results, rounded (robust against
        # data revisions in the future).
        expected = {'NY.GDP.PCAP.CD': {('Canada', '2004'): 32000.0,
                                       ('Canada', '2003'): 28000.0,
                                       ('Kosovo', '2004'): 2000.0,
                                       ('Kosovo', '2003'): 2000.0,
                                       ('Mexico', '2004'): 7000.0,
                                       ('Mexico', '2003'): 7000.0,
                                       ('United States', '2004'): 42000.0,
                                       ('United States', '2003'): 40000.0}}
        expected = pd.DataFrame(expected)
        expected = expected.sort_index()

        result = download(country=cntry_codes, indicator=inds,
                          start=2003, end=2004, errors='ignore')
        result = result.sort_index()

        # Round, to ignore revisions to data.
        result = np.round(result, decimals=-3)

        expected.index.names = ['country', 'year']
        tm.assert_frame_equal(result, expected)

        # pass start and end as string
        result = download(country=cntry_codes, indicator=inds,
                          start='2003', end='2004', errors='ignore')
        result = result.sort_index()

        # Round, to ignore revisions to data.
        result = np.round(result, decimals=-3)
        tm.assert_frame_equal(result, expected)

    def test_wdi_download_str(self):

        # These are the expected results, rounded (robust against
        # data revisions in the future).
        expected = {'NY.GDP.PCAP.CD': {('Japan', '2004'): 38000.0,
                                       ('Japan', '2003'): 35000.0,
                                       ('Japan', '2002'): 32000.0,
                                       ('Japan', '2001'): 34000.0,
                                       ('Japan', '2000'): 39000.0}}
        expected = pd.DataFrame(expected)
        expected = expected.sort_index()

        cntry_codes = 'JP'
        inds = 'NY.GDP.PCAP.CD'
        result = download(country=cntry_codes, indicator=inds,
                          start=2000, end=2004, errors='ignore')
        result = result.sort_index()
        result = np.round(result, decimals=-3)

        expected.index.names = ['country', 'year']
        tm.assert_frame_equal(result, expected)

        result = WorldBankReader(inds, countries=cntry_codes,
                                 start=2000, end=2004, errors='ignore').read()
        result = result.sort_index()
        result = np.round(result, decimals=-3)
        tm.assert_frame_equal(result, expected)

    def test_wdi_download_error_handling(self):
        cntry_codes = ['USA', 'XX']
        inds = 'NY.GDP.PCAP.CD'

        msg = "Invalid Country Code\\(s\\): XX"
        with assert_raises_regex(ValueError, msg):
            download(country=cntry_codes, indicator=inds,
                     start=2003, end=2004, errors='raise')

        with tm.assert_produces_warning():
            result = download(country=cntry_codes, indicator=inds,
                              start=2003, end=2004, errors='warn')
            assert isinstance(result, pd.DataFrame)
            assert len(result), 2

        cntry_codes = ['USA']
        inds = ['NY.GDP.PCAP.CD', 'BAD_INDICATOR']

        msg = ("The provided parameter value is not valid\\. "
               "Indicator: BAD_INDICATOR")
        with assert_raises_regex(ValueError, msg):
            download(country=cntry_codes, indicator=inds,
                     start=2003, end=2004, errors='raise')

        with tm.assert_produces_warning():
            result = download(country=cntry_codes, indicator=inds,
                              start=2003, end=2004, errors='warn')
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2

    def test_wdi_download_w_retired_indicator(self):

        cntry_codes = ['CA', 'MX', 'US']
        # Despite showing up in the search feature, and being listed online,
        # the api calls to GDPPCKD don't work in their own query builder, nor
        # pandas module.  GDPPCKD used to be a common symbol.
        #
        # This test is written to ensure that error messages to pandas users
        # continue to make sense, rather than a user getting some missing
        # key error, cause their JSON message format changed.  If
        #
        # World bank ever finishes the deprecation of this symbol,
        # this test should still pass.

        inds = ['GDPPCKD']

        with pytest.raises(ValueError):
            result = download(country=cntry_codes, indicator=inds,
                              start=2003, end=2004, errors='ignore')

            # If it ever gets here, it means WB unretired the indicator.
            # even if they dropped it completely, it would still
            # get caught above
            # or the WB API changed somehow in a really
            # unexpected way.
            if len(result) > 0:  # pragma: no cover
                pytest.skip("Invalid results")

    def test_wdi_download_w_crash_inducing_countrycode(self):

        cntry_codes = ['CA', 'MX', 'US', 'XXX']
        inds = ['NY.GDP.PCAP.CD']

        with pytest.raises(ValueError):
            result = download(country=cntry_codes, indicator=inds,
                              start=2003, end=2004, errors='ignore')

            # If it ever gets here, it means the country code XXX
            # got used by WB
            # or the WB API changed somehow in a really
            # unexpected way.
            if len(result) > 0:  # pragma: no cover
                pytest.skip("Invalid results")

    def test_wdi_get_countries(self):
        result1 = get_countries()
        result2 = WorldBankReader().get_countries()

        session = requests.Session()
        result3 = get_countries(session=session)
        result4 = WorldBankReader(session=session).get_countries()

        for result in [result1, result2, result3, result4]:
            assert 'Zimbabwe' in list(result['name'])
            assert len(result) > 100
            assert pd.notnull(result.latitude.mean())
            assert pd.notnull(result.longitude.mean())

    def test_wdi_get_indicators(self):
        result1 = get_indicators()
        result2 = WorldBankReader().get_indicators()

        session = requests.Session()
        result3 = get_indicators(session=session)
        result4 = WorldBankReader(session=session).get_indicators()

        for result in [result1, result2, result3, result4]:
            exp_col = pd.Index(['id', 'name', 'source', 'sourceNote',
                                'sourceOrganization', 'topics', 'unit'])
            # assert_index_equal doesn't exists
            assert result.columns.equals(exp_col)
            assert len(result) > 10000

    def test_wdi_download_monthly(self):
        expected = {'COPPER': {('World', '2012M01'): 8040.47,
                               ('World', '2011M12'): 7565.48,
                               ('World', '2011M11'): 7581.02,
                               ('World', '2011M10'): 7394.19,
                               ('World', '2011M09'): 8300.14,
                               ('World', '2011M08'): 9000.76,
                               ('World', '2011M07'): 9650.46,
                               ('World', '2011M06'): 9066.85,
                               ('World', '2011M05'): 8959.90,
                               ('World', '2011M04'): 9492.79,
                               ('World', '2011M03'): 9503.36,
                               ('World', '2011M02'): 9867.60,
                               ('World', '2011M01'): 9555.70}}
        expected = pd.DataFrame(expected)
        # Round, to ignore revisions to data.
        expected = np.round(expected, decimals=-3)
        expected = expected.sort_index()
        cntry_codes = 'ALL'
        inds = 'COPPER'
        result = download(country=cntry_codes, indicator=inds,
                          start=2011, end=2012, freq='M', errors='ignore')
        result = result.sort_index()
        result = np.round(result, decimals=-3)

        expected.index.names = ['country', 'year']
        tm.assert_frame_equal(result, expected)

        result = WorldBankReader(inds, countries=cntry_codes, start=2011,
                                 end=2012, freq='M', errors='ignore').read()
        result = result.sort_index()
        result = np.round(result, decimals=-3)
        tm.assert_frame_equal(result, expected)

    def test_wdi_download_quarterly(self):
        code = 'DT.DOD.PUBS.CD.US'
        expected = {code: {('Albania', '2012Q1'): 3240539817.18,
                           ('Albania', '2011Q4'): 3213979715.15,
                           ('Albania', '2011Q3'): 3187681048.95,
                           ('Albania', '2011Q2'): 3248041513.86,
                           ('Albania', '2011Q1'): 3137210567.92}}
        expected = pd.DataFrame(expected)
        # Round, to ignore revisions to data.
        expected = np.round(expected, decimals=-3)
        expected = expected.sort_index()
        cntry_codes = 'ALB'
        inds = 'DT.DOD.PUBS.CD.US'
        result = download(country=cntry_codes, indicator=inds,
                          start=2011, end=2012, freq='Q', errors='ignore')
        result = result.sort_index()
        result = np.round(result, decimals=-3)

        expected.index.names = ['country', 'year']
        tm.assert_frame_equal(result, expected)

        result = WorldBankReader(inds, countries=cntry_codes, start=2011,
                                 end=2012, freq='Q', errors='ignore').read()
        result = result.sort_index()
        result = np.round(result, decimals=-1)
        tm.assert_frame_equal(result, expected)
