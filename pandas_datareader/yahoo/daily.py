from pandas_datareader.commons.url_request import _retry_read_url
from pandas_datareader.commons.date_chunks import _sanitize_dates

_HISTORICAL_YAHOO_URL = 'http://ichart.finance.yahoo.com/table.csv?'


def _get_hist_yahoo(sym, start, end, interval, retry_count, pause):
    """
    Get historical data for the given name from yahoo.
    Date format is datetime

    Returns a DataFrame.
    """
    start, end = _sanitize_dates(start, end)
    url = (_HISTORICAL_YAHOO_URL + 's=%s' % sym +
           '&a=%s' % (start.month - 1) +
           '&b=%s' % start.day +
           '&c=%s' % start.year +
           '&d=%s' % (end.month - 1) +
           '&e=%s' % end.day +
           '&f=%s' % end.year +
           '&g=%s' % interval +
           '&ignore=.csv')
    return _retry_read_url(url, retry_count, pause, 'Yahoo!')
