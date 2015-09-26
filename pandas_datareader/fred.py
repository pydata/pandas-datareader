import datetime as dt
from pandas.core.common import is_list_like
from pandas.io.common import urlopen
from pandas import concat, read_csv

from pandas_datareader._utils import _sanitize_dates

_URL = "http://research.stlouisfed.org/fred2/series/"


def _get_data(name, start=dt.datetime(2010, 1, 1),
                  end=dt.datetime.today()):
    """
    Get data for the given name from the St. Louis FED (FRED).
    Date format is datetime

    Returns a DataFrame.

    If multiple names are passed for "series" then the index of the
    DataFrame is the outer join of the indicies of each series.
    """
    start, end = _sanitize_dates(start, end)

    if not is_list_like(name):
        names = [name]
    else:
        names = name

    urls = [_URL + '%s' % n + '/downloaddata/%s' % n + '.csv' for
            n in names]

    def fetch_data(url, name):
        with urlopen(url) as resp:
            data = read_csv(resp, index_col=0, parse_dates=True,
                            header=None, skiprows=1, names=["DATE", name],
                            na_values='.')
        try:
            return data.truncate(start, end)
        except KeyError: # pragma: no cover
            if data.ix[3].name[7:12] == 'Error':
                raise IOError("Failed to get the data. Check that {0!r} is "
                              "a valid FRED series.".format(name))
            raise
    df = concat([fetch_data(url, n) for url, n in zip(urls, names)],
                axis=1, join='outer')
    return df
