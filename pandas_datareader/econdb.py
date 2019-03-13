import requests
import pandas as pd
import pandas.compat as compat

from pandas_datareader.base import _BaseReader


class EcondbReader(_BaseReader):
    """Get data for the given name from Econdb."""

    _URL = 'https://www.econdb.com/api/series/'
    _format = None
    _show = 'labels'

    @property
    def url(self):
        """API URL"""
        if not isinstance(self.symbols, compat.string_types):
            raise ValueError('data name must be string')

        return ('{0}?{1}&format=json&page_size=500&expand=meta'
                .format(self._URL, self.symbols))

    def read(self):
        """ read one data from specified URL """
        results = requests.get(self.url).json()['results']
        df = pd.DataFrame({'dates': []}).set_index('dates')

        for entry in results:
            head = entry['additional_metadata']
            series = (pd.DataFrame(entry['data'])[['dates', 'values']]
                      .set_index('dates'))
            if self._show == 'labels':
                def show_func(x): return x.split(':')[1]
            elif self._show == 'codes':
                def show_func(x): return x.split(':')[0]

            series.columns = pd.MultiIndex.from_tuples(
                [[show_func(x) for x in head.values()]],
                names=[show_func(x) for x in head.keys()])

            if not df.empty:
                df = df.join(series, how='outer')
            else:
                df = series
        df.index = pd.to_datetime(df.index, errors='ignore')
        df.index.name = 'TIME_PERIOD'
        df = df.truncate(self.start, self.end)
        return df
