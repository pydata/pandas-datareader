import logging
logger = logging.getLogger(__name__)

import datetime

import pandas as pd
import pandas.compat as compat
from pandas.io.common import ZipFile

from pandas_datareader.base import _BaseReader

class TrueFXReader(_BaseReader):

    """Get data from TrueFX"""

    def __init__(self, retry_count=3, pause=0.001, session=None):
        if not isinstance(retry_count, int) or retry_count < 0:
            raise ValueError("'retry_count' must be integer larger than 0")
        self.retry_count = retry_count
        self.pause = pause
        self.session = self._init_session(session, retry_count)

    def url(self, symbol, year, month):
        month_name = datetime.datetime(year=year, month=month, day=1).strftime('%B').upper()
        return 'http://www.truefx.com/dev/data/{year:04d}/{month_name}-{year:04d}/{symbol}-{year:04d}-{month:02d}.zip'.format(year=year, month=month, symbol=symbol, month_name=month_name)

    def _sanitize_symbol(self, symbol):
        return symbol.replace("/", "").upper()

    def _filename_csv(self, symbol, year, month):
        return "{symbol}-{year:04d}-{month:02d}.csv".format(year=year, month=month, symbol=symbol)

    def read(self, symbols, start, end):
        """ read data """
        start, end = self._sanitize_dates(start, end)
        # If a single symbol, (e.g., 'GOOG')
        if isinstance(symbols, (compat.string_types, int)):
            df = self._read_several_months(symbols, start, end)
        # Or multiple symbols, (e.g., ['GOOG', 'AAPL', 'MSFT'])
        else:
            raise NotImplementedError("Can't download several symbols")
        return df

    def _read_one_month(self, symbol, year, month):
        url = self.url(symbol, year, month)
        symbol = symbol.replace("/", "").upper()

        logger.debug("querying '%s'" % url)
        response = self.session.get(url)
        zip_data = compat.BytesIO(response.content)

        with ZipFile(zip_data, 'r') as zf:
            zfile = zf.open(self._filename_csv(symbol, year, month))
            df = pd.read_csv(zfile, names=['Symbol', 'Date', 'Bid', 'Ask'])

        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d %H:%M:%S.%f')
        df = df.set_index('Date')
        
        return df

    def _read_several_months(self, symbol, start, end):
        symbol = self._sanitize_symbol(symbol)
        months = pd.date_range(start, end, freq='MS')
        lst = []
        for dt in months:
            year, month = dt.year, dt.month
            df = self._read_one_month(symbol, year, month)
            lst.append(df)
        return pd.concat(lst)
