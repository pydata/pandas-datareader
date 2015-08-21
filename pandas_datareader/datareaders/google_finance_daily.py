from pandas.io.common import urlencode
from pandas_datareader.data import _sanitize_dates, _retry_read_url

_HISTORICAL_GOOGLE_URL = 'http://www.google.com/finance/historical?'


def _get_hist_google(sym, start, end, interval, retry_count, pause):
    """
    Get historical data for the given name from google.
    Date format is datetime

    Returns a DataFrame.
    """
    start, end = _sanitize_dates(start, end)

    # www.google.com/finance/historical?q=GOOG&startdate=Jun+9%2C+2011&enddate=Jun+8%2C+2013&output=csv
    url = "%s%s" % (_HISTORICAL_GOOGLE_URL,
                    urlencode({"q": sym,
                               "startdate": start.strftime('%b %d, ' '%Y'),
                               "enddate": end.strftime('%b %d, %Y'),
                               "output": "csv"}))
    return _retry_read_url(url, retry_count, pause, 'Google')
