import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web

from pandas_datareader.oanda import get_oanda_currency_historical_rates


class TestOandaCurrencyHistoricalRate(tm.TestCase):
    def test_oanda_currency_historical_rate(self):
        start = "2016-01-01"
        end = "2016-06-01"
        quote_currency = "USD"
        base_currency = None
        reversed = True
        df_rates = get_oanda_currency_historical_rates(
            start, end,
            quote_currency=quote_currency,
            base_currency=base_currency, reversed=reversed
        )
        self.assertEqual(df_rates.index[0], pd.to_datetime("2016-01-01"))
        self.assertEqual(df_rates.index[-1], pd.to_datetime("2016-06-01"))

    def test_oanda_currency_historical_rate_datareader(self):
        start = "2016-01-01"
        end = "2016-06-01"
        df_rates = web.DataReader(["EUR", "GBP", "JPY"], "oanda", start, end)
        self.assertEqual(df_rates.index[0], pd.to_datetime("2016-01-01"))
        self.assertEqual(df_rates.index[-1], pd.to_datetime("2016-06-01"))
