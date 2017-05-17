from pandas import concat
from pandas_datareader.yahoo.daily import YahooDailyReader


class YahooActionReader(YahooDailyReader):
    """
    Returns DataFrame of historical corporate actions (dividends and stock
    splits) from symbols, over date range, start to end. All dates in the
    resulting DataFrame correspond with dividend and stock split ex-dates.
    """
    def read(self):
        dividends = YahooDivReader(symbols=self.symbols,
                                   start=self.start,
                                   end=self.end,
                                   retry_count=self.retry_count,
                                   pause=self.pause,
                                   session=self.session).read()
        # Add a label column so we can combine our two DFs
        dividends["action"] = "DIVIDEND"
        dividends = dividends.rename(columns={'Dividends': 'value'})

        splits = YahooSplitReader(symbols=self.symbols,
                                  start=self.start,
                                  end=self.end,
                                  retry_count=self.retry_count,
                                  pause=self.pause,
                                  session=self.session).read()
        # Add a label column so we can combine our two DFs
        splits["action"] = "SPLIT"
        splits = splits.rename(columns={'Stock Splits': 'value'})
        # Converts fractional form splits (i.e. "2/1") into conversion ratios,
        # then take the reciprocal
        splits['value'] = splits.apply(lambda x: 1/eval(x['value']), axis=1)

        output = concat([dividends, splits]).sort_index(ascending=False)

        return output


class YahooDivReader(YahooDailyReader):

    @property
    def service(self):
        return 'div'


class YahooSplitReader(YahooDailyReader):

    @property
    def service(self):
        return 'split'
