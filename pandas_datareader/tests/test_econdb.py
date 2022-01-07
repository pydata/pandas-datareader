import numpy as np
import pandas as pd
from pandas import testing as tm
import pytest

from pandas_datareader import data as web

pytestmark = pytest.mark.stable


class TestEcondb(object):
    def test_infer_start_end_from_symbols(self):
        df = web.DataReader(
            (
                "dataset=NAMQ_10_GDP&v=Geopolitical entity (reporting)"
                "&h=TIME&from=2010-02-01&to=2018-10-01&GEO=[AL,AT,BE,BA,"
                "BG,HR,CY,CZ,DK,EE,EA19,FI,FR,DE,EL,HU,IS,IE,IT,XK,LV,LT,"
                "LU,MT,ME,NL,MK,NO,PL,PT,RO,RS,SK,SI,ES,SE,CH,TR,UK]"
                "&NA_ITEM=[B1GQ]&S_ADJ=[SCA]&UNIT=[CLV10_MNAC]"
            ),
            "econdb",
        )
        assert df.index[0].year == 2010
        assert df.index[-1].year == 2018

    @pytest.mark.xfail(reason="Dataset does not exist on Econdb")
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
            "dataset=OE_TOURISM_INBOUND&COUNTRY=JPN,USA&VARIABLE=INB_ARRIVALS_TOTAL",
            "econdb",
            start=pd.Timestamp("2008-01-01"),
            end=pd.Timestamp("2012-01-01"),
        )
        df = df.astype(float)
        jp = np.array([8351000, 6790000, 8611000, 6219000, 8368000], dtype=float)
        us = np.array(
            [175702304, 160507424, 164079728, 167600272, 171320416], dtype=float
        )
        index = pd.date_range("2008-01-01", "2012-01-01", freq="AS", name="TIME_PERIOD")

        # check the values coming back are equal
        np.testing.assert_array_equal(df.values[:, 0], jp)
        np.testing.assert_array_equal(df.values[:, 1], us)

        # sometimes the country and variable columns are swapped
        df = df.swaplevel(2, 1, axis=1)
        for label, values in [("Japan", jp), ("United States", us)]:
            expected = pd.Series(
                values, index=index, name="Total international arrivals"
            )
            expected.index.freq = None
            tm.assert_series_equal(
                df[label]["Tourism demand surveys"]["Total international arrivals"],
                expected,
            )

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

    def test_australia_gdp(self):
        df = web.DataReader(
            "dataset=ABS_GDP&to=2019-09-01&from=1959-09-01&h=TIME&v=Indicator", "econdb"
        )
        assert (
            df.loc[
                "2017-10-01",
                (
                    "GDP per capita: Current prices - National Accounts",
                    "Seasonally Adjusted",
                    "AUD",
                ),
            ]
            == 18329
        )
