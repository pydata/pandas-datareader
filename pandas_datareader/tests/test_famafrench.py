import pandas as pd
import pandas.testing as tm
import pytest

import pandas_datareader.data as web
from pandas_datareader.famafrench import get_available_datasets

pytestmark = pytest.mark.stable


class TestFamaFrench(object):
    def test_get_data(self):
        keys = [
            "F-F_Research_Data_Factors",
            "F-F_ST_Reversal_Factor",
            "6_Portfolios_2x3",
            "Portfolios_Formed_on_ME",
            "Prior_2-12_Breakpoints",
            "ME_Breakpoints",
        ]

        for name in keys:
            ff = web.DataReader(name, "famafrench")
            assert "DESCR" in ff
            assert len(ff) > 1

    def test_get_available_datasets(self):
        pytest.importorskip("lxml")
        avail = get_available_datasets()
        assert len(avail) > 100

    def test_index(self):
        ff = web.DataReader("F-F_Research_Data_Factors", "famafrench")
        assert ff[0].index.freq == "M"
        assert ff[1].index.freq == "A-DEC"

    def test_f_f_research(self):
        results = web.DataReader(
            "F-F_Research_Data_Factors",
            "famafrench",
            start="2010-01-01",
            end="2010-12-01",
        )
        assert isinstance(results, dict)
        assert len(results) == 3

        exp = pd.DataFrame(
            {
                "Mkt-RF": [
                    -3.36,
                    3.4,
                    6.31,
                    2.0,
                    -7.89,
                    -5.56,
                    6.93,
                    -4.77,
                    9.54,
                    3.88,
                    0.6,
                    6.82,
                ],
                "SMB": [
                    0.38,
                    1.2,
                    1.42,
                    4.98,
                    0.05,
                    -1.97,
                    0.16,
                    -3.00,
                    3.92,
                    1.15,
                    3.70,
                    0.7,
                ],
                "HML": [
                    0.31,
                    3.16,
                    2.1,
                    2.81,
                    -2.38,
                    -4.5,
                    -0.27,
                    -1.95,
                    -3.12,
                    -2.59,
                    -0.9,
                    3.81,
                ],
                "RF": [
                    0.0,
                    0.0,
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                ],
            },
            index=pd.period_range("2010-01-01", "2010-12-01", freq="M", name="Date"),
            columns=["Mkt-RF", "SMB", "HML", "RF"],
        )
        tm.assert_frame_equal(results[0], exp, check_less_precise=0)

    def test_me_breakpoints(self):
        results = web.DataReader(
            "ME_Breakpoints", "famafrench", start="2010-01-01", end="2010-12-01"
        )
        assert isinstance(results, dict)
        assert len(results) == 2
        assert results[0].shape == (12, 21)

        exp_columns = pd.Index(
            [
                "Count",
                (0, 5),
                (5, 10),
                (10, 15),
                (15, 20),
                (20, 25),
                (25, 30),
                (30, 35),
                (35, 40),
                (40, 45),
                (45, 50),
                (50, 55),
                (55, 60),
                (60, 65),
                (65, 70),
                (70, 75),
                (75, 80),
                (80, 85),
                (85, 90),
                (90, 95),
                (95, 100),
            ],
            dtype="object",
        )
        tm.assert_index_equal(results[0].columns, exp_columns)

        exp_index = pd.period_range("2010-01-01", "2010-12-01", freq="M", name="Date")
        tm.assert_index_equal(results[0].index, exp_index)

    def test_prior_2_12_breakpoints(self):
        results = web.DataReader(
            "Prior_2-12_Breakpoints", "famafrench", start="2010-01-01", end="2010-12-01"
        )
        assert isinstance(results, dict)
        assert len(results) == 2
        assert results[0].shape == (12, 22)

        exp_columns = pd.Index(
            [
                "<=0",
                ">0",
                (0, 5),
                (5, 10),
                (10, 15),
                (15, 20),
                (20, 25),
                (25, 30),
                (30, 35),
                (35, 40),
                (40, 45),
                (45, 50),
                (50, 55),
                (55, 60),
                (60, 65),
                (65, 70),
                (70, 75),
                (75, 80),
                (80, 85),
                (85, 90),
                (90, 95),
                (95, 100),
            ],
            dtype="object",
        )
        tm.assert_index_equal(results[0].columns, exp_columns)

        exp_index = pd.period_range("2010-01-01", "2010-12-01", freq="M", name="Date")
        tm.assert_index_equal(results[0].index, exp_index)
