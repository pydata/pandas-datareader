from datetime import datetime
import os

import pandas as pd
import pytest

from pandas_datareader import data as web
from pandas_datareader._utils import RemoteDataError

TEST_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
TEST_API_KEY = None if not TEST_API_KEY else TEST_API_KEY

pytestmark = [
    pytest.mark.requires_api_key,
    pytest.mark.alpha_vantage,
    pytest.mark.skipif(TEST_API_KEY is None, reason="ALPHAVANTAGE_API_KEY not set"),
]


class TestAVTimeSeries(object):
    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")
        cls.col_index_adj = pd.Index(
            [
                "open",
                "high",
                "low",
                "close",
                "adjusted close",
                "volume",
                "dividend amount",
            ]
        )
        cls.col_index = pd.Index(["open", "high", "low", "close", "volume"])

    @property
    def start(self):
        return datetime(2015, 2, 9)

    @property
    def end(self):
        return datetime(2017, 5, 24)

    def test_av_bad_symbol(self):
        with pytest.raises((ValueError, RemoteDataError)):
            web.DataReader(
                "BADTICKER",
                "av-daily",
                start=self.start,
                end=self.end,
                retry_count=6,
                pause=20.5,
            )

    def test_av_daily(self):
        df = web.DataReader(
            "AAPL",
            "av-daily",
            start=self.start,
            end=self.end,
            retry_count=6,
            pause=20.5,
        )
        assert df.columns.equals(self.col_index)
        assert len(df) == 578
        assert df["volume"][-1] == 19178000

        expected1 = df.loc["2017-02-09"]
        assert expected1["close"] == 132.42
        assert expected1["high"] == 132.445

        expected2 = df.loc["2017-05-24"]
        assert expected2["close"] == 153.34
        assert expected2["high"] == 154.17

    def test_av_daily_adjusted(self):
        df = web.DataReader(
            "AAPL",
            "av-daily-adjusted",
            start=self.start,
            end=self.end,
            retry_count=6,
            pause=20.5,
        )
        assert df.columns.equals(
            pd.Index(
                [
                    "open",
                    "high",
                    "low",
                    "close",
                    "adjusted close",
                    "volume",
                    "dividend amount",
                    "split coefficient",
                ]
            )
        )
        assert len(df) == 578
        assert df["volume"][-1] == 19178000

        expected1 = df.loc["2017-02-09"]
        assert expected1["close"] == 132.42
        assert expected1["high"] == 132.445
        assert expected1["dividend amount"] == 0.57
        assert expected1["split coefficient"] == 1.0

        expected2 = df.loc["2017-05-24"]
        assert expected2["close"] == 153.34
        assert expected2["high"] == 154.17
        assert expected2["dividend amount"] == 0.00
        assert expected2["split coefficient"] == 1.0

    @staticmethod
    def _helper_df_weekly_monthly(df, adj=False):

        expected1 = df.loc["2015-02-27"]
        assert expected1["close"] == 128.46
        assert expected1["high"] == 133.60

        expected2 = df.loc["2017-03-31"]
        assert expected2["close"] == 143.66
        assert expected2["high"] == 144.5

    def test_av_weekly(self):
        df = web.DataReader(
            "AAPL",
            "av-weekly",
            start=self.start,
            end=self.end,
            retry_count=6,
            pause=20.5,
        )

        assert len(df) == 119
        assert df.iloc[0].name == "2015-02-13"
        assert df.iloc[-1].name == "2017-05-19"
        assert df.columns.equals(self.col_index)
        self._helper_df_weekly_monthly(df, adj=False)

    def test_av_weekly_adjusted(self):
        df = web.DataReader(
            "AAPL",
            "av-weekly-adjusted",
            start=self.start,
            end=self.end,
            retry_count=6,
            pause=20.5,
        )

        assert len(df) == 119
        assert df.iloc[0].name == "2015-02-13"
        assert df.iloc[-1].name == "2017-05-19"
        assert df.columns.equals(self.col_index_adj)
        self._helper_df_weekly_monthly(df, adj=True)

    def test_av_monthly(self):
        df = web.DataReader(
            "AAPL",
            "av-monthly",
            start=self.start,
            end=self.end,
            retry_count=6,
            pause=20.5,
        )

        assert len(df) == 27
        assert df.iloc[0].name == "2015-02-27"
        assert df.iloc[-1].name == "2017-04-28"
        assert df.columns.equals(self.col_index)
        self._helper_df_weekly_monthly(df, adj=False)

    def test_av_monthly_adjusted(self):
        df = web.DataReader(
            "AAPL",
            "av-monthly-adjusted",
            start=self.start,
            end=self.end,
            retry_count=6,
            pause=20.5,
        )

        assert df.columns.equals(self.col_index_adj)
        assert len(df) == 27
        assert df.iloc[0].name == "2015-02-27"
        assert df.iloc[-1].name == "2017-04-28"
        self._helper_df_weekly_monthly(df, adj=True)

    def test_av_intraday(self):
        # Not much available to test, but ensure close in length
        df = web.DataReader("AAPL", "av-intraday", retry_count=6, pause=20.5)

        assert len(df) > 1000
        assert "open" in df.columns
        assert "close" in df.columns
