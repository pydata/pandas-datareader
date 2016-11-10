import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web
from pandas.tseries.frequencies import to_offset
import nose

from pandas_datareader.oandarest import OANDARestHistoricalInstrumentReader


class TestOandaHistoricalInstrumentReader(tm.TestCase):

    def get_credential(self):
        return {'accountType': "practice",
                'apiToken': "8106b9109c7c6b17d45ff215f6da9d42-1446c148ee0f4ac9274eb6c2be0784b7"
                }

    def test_oanda_historical_currencypair(self):
        start = "2014-03-19T09:00:00Z"
        end = "2014-03-21T9:00:00Z"
        symbols = ["EUR_USD"]

        try:
            pn = OANDARestHistoricalInstrumentReader(
                symbols=symbols,
                start=start, end=end,
                freq="5T",
                access_credential=self.get_credential()
            ).read()
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        df_rates = pn[symbols[0]]

        self.assertTrue(pd.to_datetime(start) <= df_rates.index[0])
        self.assertTrue(df_rates.index[-1] <= pd.to_datetime(end))

    def test_oanda_historical_currencypair2(self):
        start = "2014-03-19T09:00:00Z"
        end = "2014-03-21T09:00:00Z"
        symbols = ["EUR_USD"]

        try:
            pn = web.DataReader(
                ["EUR_USD"], data_source="oanda_historical_currency",
                start=start, end=end,
                access_key=self.get_credential()
            )
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        df_rates = pn[symbols[0]]

        self.assertTrue(pd.to_datetime(start) <= df_rates.index[0])
        self.assertTrue(df_rates.index[-1] <= pd.to_datetime(end))

    def test_oanda_historical_currencypair3(self):
        start = "2014-03-19T09:00:00Z"
        end = "2014-03-21T9:00:00Z"
        symbols = ["EUR_USD"]

        try:
            pn = OANDARestHistoricalInstrumentReader(
                symbols=symbols,
                start=start, end=end,
                freq=to_offset("5T"),
                access_credential=self.get_credential()
            ).read()
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        df_rates = pn[symbols[0]]

        self.assertTrue(pd.to_datetime(start) <= df_rates.index[0])
        self.assertTrue(df_rates.index[-1] <= pd.to_datetime(end))