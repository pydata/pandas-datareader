from pandas import DataFrame, MultiIndex

from pandas_datareader.compat import concat
from pandas_datareader.yahoo.daily import YahooDailyReader


class YahooActionReader(YahooDailyReader):
    """
    Returns DataFrame of historical corporate actions (dividends and stock
    splits) from symbols, over date range, start to end. All dates in the
    resulting DataFrame correspond with dividend and stock split ex-dates.
    """

    def read(self):
        data = super().read()
        actions = {}
        if isinstance(data.columns, MultiIndex):
            data = data.swaplevel(0, 1, axis=1)
            for s in data.columns.levels[0]:
                actions[s] = _get_one_action(data[s])
            return actions
        else:
            return _get_one_action(data)

    @property
    def get_actions(self):
        return True


def _get_one_action(data):
    actions = DataFrame(columns=["action", "value"])

    if "Dividends" in data.columns:
        # Add a label column so we can combine our two DFs
        dividends = DataFrame(data["Dividends"]).dropna()
        dividends["action"] = "DIVIDEND"
        dividends = dividends.rename(columns={"Dividends": "value"})
        actions = concat([actions, dividends], sort=True)
        actions = actions.sort_index(ascending=False)

    if "Splits" in data.columns:
        # Add a label column so we can combine our two DFs
        splits = DataFrame(data["Splits"]).dropna()
        splits["action"] = "SPLIT"
        splits = splits.rename(columns={"Splits": "value"})
        actions = concat([actions, splits], sort=True)
        actions = actions.sort_index(ascending=False)

    return actions


class YahooDivReader(YahooActionReader):
    def read(self):
        data = super(YahooDivReader, self).read()
        return data[data["action"] == "DIVIDEND"]


class YahooSplitReader(YahooActionReader):
    def read(self):
        data = super(YahooSplitReader, self).read()
        return data[data["action"] == "SPLIT"]
