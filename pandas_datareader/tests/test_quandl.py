import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError
from pandas_datareader._testing import skip_on_exception


class TestQuandl(object):
    # we test data from 10 years back where possible, 2 years otherwise, or...
    start10 = "2007-01-01"  # over ten years back
    end10 = "2007-01-05"
    day10 = "2007-01-04"
    start2 = "2015-01-01"  # over two years back
    end2 = "2015-01-05"
    day2 = "2015-01-02"

    def check_headers(self, df, expected_cols):
        expected_cols = frozenset(expected_cols)
        act_cols = frozenset(df.columns.tolist())
        assert expected_cols == act_cols, "unexpected cols: " + str(act_cols)

    @skip_on_exception(RemoteDataError)
    def test_db_wiki_us(self):
        df = web.DataReader('F', 'quandl', self.start10, self.end10)
        self.check_headers(df, ['Open', 'High', 'Low', 'Close', 'Volume',
                                'ExDividend', 'SplitRatio', 'AdjOpen',
                                'AdjHigh', 'AdjLow', 'AdjClose', 'AdjVolume'])
        assert df.Close.at[self.day10] == 7.70

    @skip_on_exception(RemoteDataError)
    def test_db_fse_frankfurt(self):
        # ALV_X: Allianz SE
        df = web.DataReader('FSE/ALV_X', 'quandl', self.start10, self.end10)
        self.check_headers(df, ['Open', 'High', 'Low', 'Close', 'Change',
                                'TradedVolume', 'Turnover',
                                'LastPriceoftheDay', 'DailyTradedUnits',
                                'DailyTurnover'])
        assert df.Close.at[self.day10] == 159.45

    @skip_on_exception(RemoteDataError)
    def test_db_sse_de_stuttgart(self):
        # ALV: Allianz SE
        df = web.DataReader('SSE/ALV', 'quandl', self.start2, self.end2)
        self.check_headers(df, [
                "High", "Low", "Last", "PreviousDayPrice", "Volume"])
        # as of 2017-06-11: PreviousDayPrice can be outside Low/High range;
        # Volume can be NaN
        assert df.Last.at[self.day2] == 136.47
        df2 = web.DataReader('ALV.DE', 'quandl', self.start2, self.end2)
        assert (df.Last == df2.Last).all()

    @skip_on_exception(RemoteDataError)
    def test_db_euronext_be_fr_nl_pt(self):
        # FP: Total SA
        # as of 2017-06-11, some datasets end a few months after their start,
        # e.g. ALVD, BASD
        df = web.DataReader('EURONEXT/FP', 'quandl', self.start2, self.end2)
        self.check_headers(df, [
                "Open", "High", "Low", "Last", "Turnover", "Volume"])
        assert df.Last.at[self.day2] == 42.525
        df2 = web.DataReader('FP.FR', 'quandl', self.start2, self.end2)
        assert (df.Last == df2.Last).all()

    @skip_on_exception(RemoteDataError)
    def test_db_lse_uk(self):
        # RBS: Royal Bank of Scotland
        df = web.DataReader('LSE/RBS', 'quandl', self.start10, self.end10)
        self.check_headers(df, ["High", "Low", "LastClose", "Price",
                                "Volume", "Change", "Var"])
        # as of 2017-06-11, Price == LastClose, all others are NaN
        assert df.Price.at[self.day10] == 5950.983

    @skip_on_exception(RemoteDataError)
    def test_db_nse_in(self):
        # TCS: Tata Consutancy Services
        df = web.DataReader('NSE/TCS', 'quandl', self.start10, self.end10)
        self.check_headers(df, ['Open', 'High', 'Low', 'Last', 'Close',
                                'TotalTradeQuantity', 'TurnoverLacs'])
        assert df.Close.at[self.day10] == 1259.05

    @skip_on_exception(RemoteDataError)
    def test_db_tse_jp(self):
        # TSE/6758: Sony Corp.
        df = web.DataReader('TSE/6758', 'quandl', self.start10, self.end10)
        self.check_headers(df, ['Open', 'High', 'Low', 'Close', 'Volume'])
        assert df.Close.at[self.day10] == 5190.0

    @skip_on_exception(RemoteDataError)
    def test_db_hkex_cn(self):
        # HKEX/00941: China Mobile
        df = web.DataReader('HKEX/00941', 'quandl', self.start2, self.end2)
        self.check_headers(df,
                           ['NominalPrice', 'NetChange', 'Change', 'Bid',
                            'Ask', 'PEx', 'High', 'Low', 'PreviousClose',
                            'ShareVolume000', 'Turnover000', 'LotSize'])
        assert df.High.at[self.day2] == 91.9
