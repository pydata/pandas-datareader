import numpy as np
import pandas as pd
from pandas import testing as tm

from pandas_datareader import data as web
from pandas_datareader._testing import skip_on_exception
from pandas_datareader._utils import RemoteDataError


class TestEurostat:
    def test_get_ert_h_eur_a(self):
        # Former euro area national currencies vs. euro/ECU
        # annual data (ert_h_eur_a)
        df = web.DataReader(
            "ert_h_eur_a",
            "eurostat",
            start=pd.Timestamp("2009-01-01"),
            end=pd.Timestamp("2010-01-01"),
        )
        assert isinstance(df, pd.DataFrame)

        currencies = ["Italian lira", "Lithuanian litas"]
        # cols = [(currency, "Average") for currency in currencies]
        df_currency = df[currencies]
        df_currency = df_currency.xs("Average", axis=1, level=1)
        df_currency.columns = df_currency.columns.droplevel(1)

        exp_col = pd.MultiIndex.from_product(
            [currencies, ["Annual"]], names=["CURRENCY", "FREQ"]
        )
        exp_idx = pd.DatetimeIndex(["2009-01-01", "2010-01-01"], name="TIME_PERIOD")
        values = np.array([[1936.27, 3.4528], [1936.27, 3.4528]])
        expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
        tm.assert_frame_equal(df_currency, expected)

    def test_get_sts_cobp_a(self):
        # Building permits - annual data (2010 = 100)
        df = web.DataReader(
            "sts_cobp_a",
            "eurostat",
            start=pd.Timestamp("2000-01-01"),
            end=pd.Timestamp("2013-01-01"),
        )

        idx = pd.date_range("2000-01-01", "2013-01-01", freq="YS", name="TIME_PERIOD")
        ne_name = (
            "Building permits - m2 of useful floor area",
            "Index, 2015=100",
            "Unadjusted data (i.e. neither seasonally adjusted nor "
            "calendar adjusted data)",
            "Non-residential buildings, except office buildings",
            "Netherlands",
            "Annual",
        )
        ne_values = [
            258.4,
            240.9,
            164.5,
            168.8,
            185.1,
            190.9,
            228.3,
            293.7,
            257.7,
            166.0,
            129.2,
            147.0,
            118.4,
            98.0,
        ]
        ne = pd.Series(ne_values, name=ne_name, index=idx)

        uk_name = (
            "Building permits - m2 of useful floor area",
            "Index, 2015=100",
            "Unadjusted data (i.e. neither seasonally adjusted nor "
            "calendar adjusted data)",
            "Non-residential buildings, except office buildings",
            "United Kingdom",
            "Annual",
        )
        uk_values = [
            137.0,
            137.9,
            134.1,
            136.5,
            144.9,
            137.1,
            137.6,
            148.3,
            138.7,
            128.9,
            121.7,
            120.0,
            126.2,
            99.0,
        ]
        uk = pd.Series(uk_values, name=uk_name, index=idx)

        for expected in [ne, uk]:
            expected.index.freq = None
            result = df[expected.name]
            tm.assert_series_equal(result, expected, check_freq=False)

    def test_get_nrg_pc_202(self):
        # see gh-149

        df = web.DataReader(
            "nrg_pc_202",
            "eurostat",
            start=pd.Timestamp("2010-01-01"),
            end=pd.Timestamp("2013-01-01"),
        )

        name = (
            "Euro",
            "Gigajoule (gross calorific value - GCV)",
            "Natural gas",
            "All taxes and levies included",
            "Consumption less than 20 GJ - band D1",
            "Denmark",
            "Half-yearly, semesterly",
        )

        exp_index = pd.Index(
            [
                "2010-S1",
                "2010-S2",
                "2011-S1",
                "2011-S2",
                "2012-S1",
                "2012-S2",
                "2013-S1",
                "2013-S2",
            ],
            name="TIME_PERIOD",
        )
        exp = pd.Series(
            [26.4979, 26.8147, 28.6448, 28.5862, 27.2187, 26.5285, 27.5854, 27.1403],
            index=exp_index,
            name=name,
        )

        tm.assert_series_equal(df[name], exp)

    @skip_on_exception(RemoteDataError)
    def test_get_prc_hicp_manr_large_query(self):
        df = web.DataReader(
            "prc_hicp_manr",
            "eurostat",
            start=pd.Timestamp("2000-01-01"),
            end=pd.Timestamp("2013-01-01"),
        )
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.min() >= pd.Timestamp("2000-01-01")
        assert df.index.max() <= pd.Timestamp("2013-12-31")
        assert "FREQ" in df.columns.names
        assert "UNIT" in df.columns.names
        assert "COICOP" in df.columns.names
        assert "GEO" in df.columns.names
