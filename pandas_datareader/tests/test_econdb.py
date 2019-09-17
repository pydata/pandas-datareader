import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pytest

import pandas_datareader.data as web

pytestmark = pytest.mark.stable


class TestEcondb(object):
    def test_get_cdh_e_fos(self):
        # EUROSTAT
        # Employed doctorate holders in non managerial and non professional
        # occupations by fields of science (%)
        df = web.DataReader(
            "dataset=CDH_E_FOS&GEO=NO,PL,PT,RU&FOS07=FOS1&Y_GRAD=TOTAL",
            "econdb",
            start=pd.Timestamp("2005-01-01"),
            end=pd.Timestamp("2010-01-01"),
        )
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 4)

        # the levels and not returned consistently for econdb
        names = list(df.columns.names)
        levels = [lvl.values.tolist() for lvl in list(df.columns.levels)]

        exp_col = pd.MultiIndex.from_product(levels, names=names)
        exp_idx = pd.DatetimeIndex(["2006-01-01", "2009-01-01"], name="TIME_PERIOD")

        values = np.array([[25.49, np.nan, 39.05, np.nan], [20.38, 25.1, 27.77, 38.1]])
        expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
        tm.assert_frame_equal(df, expected)

    def test_get_tourism(self):
        # OECD
        # TOURISM_INBOUND

        df = web.DataReader(
            "dataset=OE_TOURISM_INBOUND&COUNTRY=JPN,USA&" "VARIABLE=INB_ARRIVALS_TOTAL",
            "econdb",
            start=pd.Timestamp("2008-01-01"),
            end=pd.Timestamp("2012-01-01"),
        )
        df = df.astype(np.float)
        jp = np.array([8351000, 6790000, 8611000, 6219000, 8368000], dtype=float)
        us = np.array(
            [175702304, 160507424, 164079728, 167600272, 171320416], dtype=float
        )
        index = pd.date_range("2008-01-01", "2012-01-01", freq="AS", name="TIME_PERIOD")

        # sometimes the country and variable columns are swapped
        lvl1 = df.columns.levels[0][0]
        if lvl1 == "Total international arrivals":
            df = df.swaplevel(0, 1, axis=1)
        for label, values in [("Japan", jp), ("United States", us)]:
            expected = pd.Series(
                values, index=index, name="Total international arrivals"
            )
            tm.assert_series_equal(df[label]["Total international arrivals"], expected)

    def test_bls(self):
        # BLS
        # CPI
        df = web.DataReader(
            "ticker=BLS_CU.CUSR0000SA0.M.US",
            "econdb",
            start=pd.Timestamp("2010-01-01"),
            end=pd.Timestamp("2013-01-27"),
        )

        assert df.loc["2010-05-01"][0] == 217.3
