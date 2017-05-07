import sys
import pandas as pd
import pandas.util.testing as tm
import pandas_datareader.data as web
from pandas.tseries.frequencies import to_offset
import nose

import logging
from logging import StreamHandler

from pandas_datareader.oandarest import OANDARestHistoricalInstrumentReader
from pandas_datareader.oandarest import OANDARestAccountInstrumentReader


def setupLogger(loggerName):
    logger = logging.getLogger(loggerName)
    consoleHandler = StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter('%(name)s/%(threadName)s/s%(asctime)s %(levelname)-8s #### %(message)s')
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)
    logger.setLevel(logging.DEBUG)


class TestOandaHistoricalInstrumentReader(tm.TestCase):
    start = "2014-03-19T09:00:00Z"
    end = "2014-03-20T9:00:00Z"
    currency1 = "EUR_USD"

    def get_credential(self):
        return {'accountType': "practice"}

    def assertPanel(self, pn, start, end, symbols):
        self.assertTrue(pn is not None)

        # Check all symbols have been downloaded
        self.assertTrue(set(pn.minor_axis) == set(symbols))

        # Data can be access using the following notations
        prices = []
        price = pn["Ask"]["Close"]["EUR_USD"][pd.to_datetime("2014-03-19 09:00:00")]
        prices.append(price)
        price = pn["Ask"]["Close", pd.to_datetime("2014-03-19 09:00:00"), "EUR_USD"]
        prices.append(price)
        price = pn.loc[("Ask", "Close"), pd.to_datetime("2014-03-19 09:00:00"), "EUR_USD"]
        prices.append(price)

        # For all prices types available
        for item in pn.items:
            for itemValue in item:
                itemValue = pn[item[0]]
                typeValue = itemValue[item[1]]
                for currency in typeValue.columns.values:
                    currencyValue = typeValue[currency]
                    # value = currencyValue[0]

                    # Check non empty time series is available
                    self.assertTrue(pd.to_datetime(start) <= currencyValue.index[0])
                    self.assertTrue(currencyValue.index[-1] <= pd.to_datetime(end))

    def test_oanda_historical_currencypair(self):
        symbols = [self.currency1]

        try:
            pn = OANDARestHistoricalInstrumentReader(
                symbols=symbols,
                start=self.start, end=self.end,
                freq="5T",
                candleFormat="BA",
                access_credential=self.get_credential()
            ).read()
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        self.assertPanel(pn, self.start, self.end, symbols)

    def test_oanda_historical_currencypair2(self):
        symbols = [self.currency1, "USD_JPY"]

        try:
            pn = web.DataReader(
                symbols, data_source="oanda_historical_currency",
                start=self.start, end=self.end,
                access_key=self.get_credential()
            )
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        self.assertPanel(pn, self.start, self.end, symbols)

    def test_oanda_historical_currencypair3(self):
        symbols = [self.currency1, "USD_JPY"]

        try:
            pn = OANDARestHistoricalInstrumentReader(
                symbols=symbols,
                start=self.start, end=self.end,
                freq=to_offset("4H"),
                access_credential=self.get_credential()
            ).read()
        except Exception as error:
            raise nose.SkipTest("API Token missing ?" + str(error))

        self.assertPanel(pn, self.start, self.end, symbols)


setupLogger("pandas_datareader.oanda.OANDARestHistoricalInstrumentReader")


class TestOandaAccountInstrumentReader(tm.TestCase):

    def get_credential(self):
        return {'accountType': "practice"}

    def test_oanda_account_currencypair(self):
        symbols = ["EUR_USD"]

        try:
            pn = OANDARestAccountInstrumentReader(
                symbols=symbols,
                access_credential=self.get_credential()
            ).read()
        except Exception as error:
            raise nose.SkipTest("API Token OR Account ID missing ?" + str(error))

        df = pn["Currencies"]

        self.assertTrue(df.shape[0] >= 0)
        self.assertTrue(df.shape[1] > 0)

    def test_oanda_account_currencypair2(self):
        symbols = None

        try:
            pn = OANDARestAccountInstrumentReader(
                symbols=symbols,
                access_credential=self.get_credential()
            ).read()
        except Exception as error:
            raise nose.SkipTest("API Token OR account ID missing ?" + str(error))

        df = pn["Currencies"]

        self.assertTrue(df.shape[1] >= 1)
        self.assertTrue(df.shape[1] > 0)


setupLogger("pandas_datareader.oanda.OANDARestAccountInstrumentReader")
