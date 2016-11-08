import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web

from pandas_datareader.oandarest import OANDARestHistoricalInstrumentReader


class TestOandaHistoricalInstrumentReader(tm.TestCase):
    def get_credential(self):
        return {'accountType':"practice",
                'accountVersion':"0"
                }

    def test_oanda_historical_currencypair(self):
        start = "2014-03-19T09:00:00Z"
        end = "2014-03-21T9:00:00Z"
        symbols = ["EUR_USD"]

        pn = OANDARestHistoricalInstrumentReader(
            symbols=symbols,
            start=start, end=end,
            granularity="S5",
            access_credential=self.get_credential()
        ).read()

        df_rates = pn[symbols[0]]  

        self.assertTrue(pd.to_datetime(start) <= df_rates.index[0])
        self.assertTrue(df_rates.index[-1] <= pd.to_datetime(end))

    def test_oanda_historical_currencypair2(self):
        start = "2014-03-19T09:00:00Z"
        end = "2014-03-21T09:00:00Z"
        symbols = ["EUR_USD"]

        pn = web.DataReader(
                ["EUR_USD"], data_source="oanda_historical_currency", 
                start=start, end=end,
                custom=self.get_credential()
        )

        df_rates = pn[symbols[0]]  

        self.assertTrue(pd.to_datetime(start) <= df_rates.index[0])
        self.assertTrue(df_rates.index[-1] <= pd.to_datetime(end))

