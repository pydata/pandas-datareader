from datetime import date, timedelta

import pytest

from pandas_datareader._utils import RemoteDataError
import pandas_datareader.data as web

pytestmark = pytest.mark.stable


class TestBankOfCanada(object):
    @staticmethod
    def get_symbol(currency_code, inverted=False):
        if inverted:
            return "FXCAD{}".format(currency_code)
        else:
            return "FX{}CAD".format(currency_code)

    def check_bankofcanada_count(self, code):
        start, end = date.today() - timedelta(days=30), date.today()
        df = web.DataReader(self.get_symbol(code), "bankofcanada", start, end)
        assert df.size > 15

    def check_bankofcanada_valid(self, code):
        symbol = self.get_symbol(code)
        df = web.DataReader(
            symbol, "bankofcanada", date.today() - timedelta(days=30), date.today()
        )
        assert symbol in df.columns

    def check_bankofcanada_inverted(self, code):
        symbol = self.get_symbol(code)
        symbol_inverted = self.get_symbol(code, inverted=True)

        df = web.DataReader(
            symbol, "bankofcanada", date.today() - timedelta(days=30), date.today()
        )
        df_i = web.DataReader(
            symbol_inverted,
            "bankofcanada",
            date.today() - timedelta(days=30),
            date.today(),
        )

        pairs = zip((1 / df)[symbol].tolist(), df_i[symbol_inverted].tolist())
        assert all(a - b < 0.01 for a, b in pairs)

    def test_bankofcanada_usd_count(self):
        self.check_bankofcanada_count("USD")

    def test_bankofcanada_eur_count(self):
        self.check_bankofcanada_count("EUR")

    def test_bankofcanada_usd_valid(self):
        self.check_bankofcanada_valid("USD")

    def test_bankofcanada_eur_valid(self):
        self.check_bankofcanada_valid("EUR")

    def test_bankofcanada_usd_inverted(self):
        self.check_bankofcanada_inverted("USD")

    def test_bankofcanada_eur_inverted(self):
        self.check_bankofcanada_inverted("EUR")

    def test_bankofcanada_bad_range(self):
        with pytest.raises(ValueError):
            web.DataReader(
                "FXCADUSD",
                "bankofcanada",
                date.today(),
                date.today() - timedelta(days=30),
            )

    def test_bankofcanada_bad_url(self):
        with pytest.raises(RemoteDataError):
            web.DataReader(
                "abcdefgh",
                "bankofcanada",
                date.today() - timedelta(days=30),
                date.today(),
            )
