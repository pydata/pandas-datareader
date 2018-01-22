from datetime import datetime

import pytest
import requests

from pandas_datareader._testing import skip_on_exception
from pandas_datareader._utils import RemoteDataError
from pandas_datareader.data import KeyRatiosReader, CashflowStatementReader, \
    BalanceSheetReader, IncomeStatementReader


class TestMorningstarFinancials(object):

    @classmethod
    def setup_class(cls):
        pytest.skip('Skip all Morningstar tests.')

    @skip_on_exception(RemoteDataError)
    def multi_symbol_keyratios(self):
        kr = KeyRatiosReader(symbols=['MSFT', "DIS"])
        key_ratios = kr.read()
        gm = key_ratios["2008-06-01"]["DIS"]["Financials - Summary"]["Gross Margin %"]
        assert (0.197 >= gm - .00001 and 0.197 <= gm + .00001)

    @skip_on_exception(RemoteDataError)
    def single_symbol_keyratios(self):
        kr = KeyRatiosReader(symbols="DIS")
        key_ratios = kr.read()
        gm = key_ratios["2008-06-01"]["DIS"]["Financials - Summary"]["Gross Margin %"]
        assert (0.197 >= gm - .00001 and 0.197 <= gm + .00001)

    @skip_on_exception(RemoteDataError)
    def invalid_symbol_type_keyratios(self):
        pytest.raises(TypeError, KeyRatiosReader, symbols=11221)

    @skip_on_exception(RemoteDataError)
    def invalid_symbol_type_main(self):
        pytest.raises(TypeError, CashflowStatementReader, "A", symbols=12311)

    @skip_on_exception(RemoteDataError)
    def invalid_args(self):
        view = "ordered"
        order = "InValId"
        rounding = "A"
        units = 10
        periods = 100
        freq = "Z"
        pytest.raises(ValueError, BalanceSheetReader, "MSFT", freq=freq)
        pytest.raises(ValueError, BalanceSheetReader, "MSFT", "A", view= view)
        pytest.raises(ValueError, BalanceSheetReader, "MSFT", "A", order= order)
        pytest.raises(TypeError, BalanceSheetReader, "MSFT", "A", order=21)
        pytest.raises(TypeError, BalanceSheetReader, "MSFT", "A", rounding = rounding)
        pytest.raises(ValueError, BalanceSheetReader, "MSFT", "A", periods = periods)
        pytest.raises(TypeError, BalanceSheetReader, "MSFT", "A", periods="x")
        pytest.raises(ValueError, BalanceSheetReader, "MSFT", "A", units = units)
        pytest.raises(TypeError, BalanceSheetReader, "MSFT", "A", units="a")
        pytest.raises(TypeError, BalanceSheetReader, "MSFT", "A", curry=112)

    @skip_on_exception(RemoteDataError)
    def check_session(self):
        sess = requests.session()
        bs = BalanceSheetReader(symbols="MSFT", freq="A", session=sess)
        assert (isinstance(requests.session(), type(bs.session)))

    @skip_on_exception(RemoteDataError)
    def single_symbol_financials(self):
        symbols = "IBM"

        BS_A = BalanceSheetReader(symbols=symbols, freq="A").read()
        CF_A = CashflowStatementReader(symbols=symbols, freq="A").read()
        IS_A = IncomeStatementReader(symbols=symbols, freq="A").read()
        BS_Q = BalanceSheetReader(symbols=symbols, freq="Q").read()
        CF_Q = CashflowStatementReader(symbols=symbols, freq="Q").read()
        IS_Q = IncomeStatementReader(symbols=symbols, freq="Q").read()

        assert (all([i for i in [BS_A, BS_Q, CF_A, CF_Q, IS_A, IS_Q] if len(i[i.keys()[0]]) == 1]))

    @skip_on_exception(RemoteDataError)
    def multi_symbol_financials(self):
        symbols = ["TWTR", "MSFT"]

        BS_A = BalanceSheetReader(symbols=symbols, freq="A").read()
        CF_A = CashflowStatementReader(symbols=symbols, freq="A").read()
        IS_A = IncomeStatementReader(symbols=symbols, freq="A").read()
        BS_Q = BalanceSheetReader(symbols=symbols, freq="Q").read()
        CF_Q = CashflowStatementReader(symbols=symbols, freq="Q").read()
        IS_Q = IncomeStatementReader(symbols=symbols, freq="Q").read()

        assert (all([i for i in [BS_A, BS_Q, CF_A, CF_Q, IS_A, IS_Q] if len(i[i.keys()[0]]) == 2]))

    @skip_on_exception(RemoteDataError)
    def check_restated_vs_reported(self):
        BS_A = BalanceSheetReader(symbols="TWTR", freq="A", dataType="A").read()
        assert ("TTM" in [i for i in BS_A.keys()])
        BS_Ax = BalanceSheetReader(symbols="TWTR", freq="A", dataType="R").read()
        assert ("TTM" not in [i for i in BS_Ax.keys()])

    @skip_on_exception(RemoteDataError)
    def check_order(self):
        BS_A = BalanceSheetReader(symbols='IBM', freq="A", order="desc").read()
        dates = [datetime.strptime(k, "%Y-%m-%d") for k in BS_A.keys()]
        assert (dates[0] > dates[1])


