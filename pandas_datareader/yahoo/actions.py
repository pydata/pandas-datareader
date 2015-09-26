import time
import csv
from pandas import to_datetime, DataFrame
from pandas.io.common import urlopen
from pandas.util.testing import _network_error_classes
from pandas.compat import StringIO, bytes_to_str

from pandas_datareader._utils import _sanitize_dates, _encode_url

_URL = 'http://ichart.finance.yahoo.com/x'


def _get_data(symbol, start=None, end=None, retry_count=3, pause=0.001):
    """
    Returns DataFrame of historical corporate actions (dividends and stock
    splits) from symbols, over date range, start to end. All dates in the
    resulting DataFrame correspond with dividend and stock split ex-dates.

    Parameters
    ----------
        sym : string with a single Single stock symbol (ticker).
        start : string, (defaults to '1/1/2010')
                Starting date, timestamp. Parses many different kind of date
                representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
        end : string, (defaults to today)
                Ending date, timestamp. Same format as starting date.
        retry_count : int, default 3
                Number of times to retry query request.
        pause : int, default 0
                Time, in seconds, of the pause between retries.
    """

    start, end = _sanitize_dates(start, end)
    params = {
        's': symbol,
        'a': start.month - 1,
        'b': start.day,
        'c': start.year,
        'd': end.month - 1,
        'e': end.day,
        'f': end.year,
        'g': 'v'
    }
    url = _encode_url(_URL, params)

    for _ in range(retry_count):

        try:
            with urlopen(url) as resp:
                lines = resp.read()
        except _network_error_classes:
            pass
        else:
            actions_index = []
            actions_entries = []

            for line in csv.reader(StringIO(bytes_to_str(lines))):
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

        time.sleep(pause)

    raise IOError("after %d tries, Yahoo! did not " \
                                "return a 200 for url %r" % (retry_count, url))
