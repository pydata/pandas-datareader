import csv
from pandas import to_datetime, DataFrame

from pandas_datareader.base import _DailyBaseReader


class YahooActionReader(_DailyBaseReader):

    """
    Returns DataFrame of historical corporate actions (dividends and stock
    splits) from symbols, over date range, start to end. All dates in the
    resulting DataFrame correspond with dividend and stock split ex-dates.
    """

    @property
    def url(self):
        return 'http://ichart.finance.yahoo.com/x'

    def _get_params(self, symbols=None):
        params = {
            's': self.symbols,
            'a': self.start.month - 1,
            'b': self.start.day,
            'c': self.start.year,
            'd': self.end.month - 1,
            'e': self.end.day,
            'f': self.end.year,
            'g': 'v'
        }
        return params

    def _read_lines(self, out):
        actions_index = []
        actions_entries = []

        for line in csv.reader(out.readlines()):
            # Ignore lines that aren't dividends or splits (Yahoo
            # add a bunch of irrelevant fields.)
            if len(line) != 3 or line[0] not in ('DIVIDEND', 'SPLIT'):
                continue

            action, date, value = line
            if action == 'DIVIDEND':
                actions_index.append(to_datetime(date))
                actions_entries.append({
                    'action': action,
                    'value': float(value)
                })
            elif action == 'SPLIT' and ':' in value:
                # Convert the split ratio to a fraction. For example a
                # 4:1 split expressed as a fraction is 1/4 = 0.25.
                denominator, numerator = value.split(':', 1)
                split_fraction = float(numerator) / float(denominator)

                actions_index.append(to_datetime(date))
                actions_entries.append({
                    'action': action,
                    'value': split_fraction
                })

        return DataFrame(actions_entries, index=actions_index)
