import datetime as dt

import pandas as pd
from pandas.core.common import is_list_like
import pandas.compat as compat
from pandas.io.common import _urlopen
from pandas import concat, read_csv

from pandas_datareader._utils import _sanitize_dates
from pandas_datareader.io import read_jsdmx


_URL = 'http://stats.oecd.org/SDMX-JSON/data'


def _get_data(name, start=dt.datetime(2010, 1, 1),
              end=dt.datetime.today()):
    """
    Get data for the given name from OECD.
    Date format is datetime

    Returns a DataFrame.
    """
    start, end = _sanitize_dates(start, end)

    if not isinstance(name, compat.string_types):
        raise ValueError('data name must be string')

    # API: https://data.oecd.org/api/sdmx-json-documentation/
    url = '{0}/{1}/all/all?'.format(_URL, name)
    def fetch_data(url, name):
        resp = _urlopen(url)
        resp = resp.read()
        resp = resp.decode('utf-8')
        data = read_jsdmx(resp)
        try:
            idx_name = data.index.name # hack for pandas 0.16.2
            data.index = pd.to_datetime(data.index)
            data = data.sort_index()
            data = data.truncate(start, end)
            data.index.name = idx_name
        except ValueError:
            pass
        return data
    df = fetch_data(url, name)
    return df

