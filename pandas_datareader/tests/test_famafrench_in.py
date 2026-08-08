import pandas as pd

from pandas_datareader import data as web


class TestFamaFrenchIndia:
    def test_mkt_rf_rename(self):
        ff = web.DataReader(
            "2025-12_FourFactors_and_Market_Returns_Daily", "famafrench_in"
        )
        # assert that the col exists in the dataframe
        assert ff["Mkt-RF"] is not None

    def test_download_works_for_date_range(self):
        ff = web.DataReader(
            "2025-12_FourFactors_and_Market_Returns_Daily",
            "famafrench_in",
            start="2025-01-01",
            end="2025-12-31",
        )

        assert ff.iloc[0].name.date() == pd.to_datetime("2025-01-01").date()
        assert ff.iloc[-1].name.date() == pd.to_datetime("2025-12-31").date()

    def test_download_monthly(self):
        ff = web.DataReader(
            "2025-12_FourFactors_and_Market_Returns_Monthly", "famafrench_in"
        )
        assert ff is not None

    def test_download_yearly(self):
        ff = web.DataReader(
            "2025-12_FourFactors_and_Market_Returns_Yearly", "famafrench_in"
        )

        assert ff is not None
