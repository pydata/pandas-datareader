import numpy as np
import pandas as pd
from pandas import testing as tm
import pytest

from pandas_datareader import data as web
from pandas_datareader.famafrench import get_available_datasets

pytestmark = pytest.mark.stable


class TestFamaFrench:
    def test_get_data_sample(self):
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
        # M is for legacy pandas < 2
        assert ff[0].index.freq.name in ("ME", "M")
        # A-DEC is for legacy pandas < 2
        assert ff[1].index.freq.name in ("YE-DEC", "A-DEC")

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
                    -3.35,
                    3.39,
                    6.30,
                    1.99,
                    -7.90,
                    -5.56,
                    6.92,
                    -4.78,
                    9.55,
                    3.87,
                    0.59,
                    6.82,
                ],
                "SMB": [
                    0.43,
                    1.18,
                    1.46,
                    4.84,
                    0.13,
                    -1.79,
                    0.22,
                    -3.01,
                    3.82,
                    1.08,
                    3.67,
                    0.72,
                ],
                "HML": [
                    0.33,
                    3.18,
                    2.19,
                    2.96,
                    -2.48,
                    -4.73,
                    -0.50,
                    -1.73,
                    -3.02,
                    -2.46,
                    -0.90,
                    3.56,
                ],
                "RF": [
                    0.00,
                    0.00,
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
        received = results[0]
        np.testing.assert_allclose(received, exp)
        tm.assert_index_equal(received.index, exp.index)
        tm.assert_index_equal(received.columns, exp.columns)

    def test_me_breakpoints(self):
        results = web.DataReader(
            "ME_Breakpoints", "famafrench", start="2010-01-01", end="2010-12-31"
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

    def test_all_datasets(self) -> None:
        for dataset in get_available_datasets():
            data = web.DataReader(dataset, "famafrench")

            assert tuple(data) == (*range(len(data) - 1), "DESCR")
