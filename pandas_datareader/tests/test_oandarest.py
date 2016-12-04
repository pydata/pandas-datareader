import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web
from pandas.tseries.frequencies import to_offset
import nose

from pandas_datareader.oandarest import OANDARestHistoricalInstrumentReader


class TestOandaHistoricalInstrumentReader(tm.TestCase):

    def get_credential(self):
        return {'accountType': "practice"}

    def assertPanel(self, pn, start, end, symbols):
        self.assertTrue(pn is not None)

        # Check all symbols have been downloaded
        self.assertTrue(set(pn.minor_axis) == set(symbols))

        # Data can be access using the following notations
        # price = pn["Ask"]["Close"]["EUR_USD"]["2014-03-19 09:05:00"]
        # price = pn["Ask"]["Close","2014-03-19 09:05:00","EUR_USD"]
        # price = pn.loc[("Ask","Close"),"2014-03-19 09:05:00","EUR_USD"]
        # price = pn["Ask"]["Close"]["EUR_USD"]["2014-03-19 09:05:00"]

        # For all prices types available
        for item in pn.items:
            for itemValue in item:
                itemValue = pn[item[0]]
                typeValue = itemValue[item[1]]
                for currency in typeValue.columns.values:
                    currencyValue = typeValue[currency]
                    price = currencyValue[0]

                    # Check non empty time series is available
                    self.assertTrue(pd.to_datetime(start) <= currencyValue.index[0])
                    self.assertTrue(currencyValue.index[-1] <= pd.to_datetime(end))

                    # Check pricing or other data is available
                    self.assertTrue(price is not None)

    def test_oanda_historical_currencypair(self):
        start = "2014-03-19T09:00:00Z"
        end = "2014-03-20T9:00:00Z"
        symbols = ["EUR_USD"]

        try:
            pn = OANDARestHistoricalInstrumentReader(
                symbols=symbols,
                start=start, end=end,
                freq="5T",
                candleFormat="BA",
                access_credential=self.get_credential()
            ).read()
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        self.assertPanel(pn, start, end, symbols)

    def test_oanda_historical_currencypair2(self):
        start = "2014-03-19T09:00:00Z"
        end = "2014-03-20T09:00:00Z"
        symbols = "EUR_USD"

        try:
            pn = web.DataReader(
                symbols, data_source="oanda_historical_currency",
                start=start, end=end,
                access_key=self.get_credential()
            )
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        self.assertPanel(pn, start, end, [symbols])

    def test_oanda_historical_currencypair3(self):
        start = "2014-03-19T09:00:00Z"
        end = "2014-03-20T9:00:00Z"
        symbols = ["EUR_USD", "USD_JPY"]

        try:
            pn = OANDARestHistoricalInstrumentReader(
                symbols=symbols,
                start=start, end=end,
                freq=to_offset("5T"),
                access_credential=self.get_credential()
            ).read()
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        self.assertPanel(pn, start, end, symbols)


