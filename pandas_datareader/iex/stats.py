import pandas as pd
from datetime import datetime, timedelta
from pandas_datareader.iex import IEX

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class DailySummaryReader(IEX):
    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None):
        self.curr_date = start
        super(DailySummaryReader, self).__init__(symbols=symbols,
                                                 start=start, end=end,
                                                 retry_count=retry_count,
                                                 pause=pause, session=session)

    @property
    def service(self):
        return "stats/historical/daily"

    def _get_params(self, symbols):
        p = {}

        if self.curr_date is not None:
            p['date'] = self.curr_date.strftime('%Y%m%d')

        return p

    def read(self):
        """Unfortunately, IEX's API can only retrieve data one day or one month
        at a time. Rather than specifying a date range, we will have to run
        the read function for each date provided.

        :return: DataFrame
        """
        tlen = self.end - self.start
        dfs = []
        for date in (self.start + timedelta(n) for n in range(tlen.days)):
            self.curr_date = date
            tdf = super(IEX, self).read()
            dfs.append(tdf)
        return pd.concat(dfs)


class MonthlySummaryReader(IEX):
    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None):
        self.curr_date = start
        self.date_format = '%Y%m'

        super(MonthlySummaryReader, self).__init__(symbols=symbols,
                                                   start=start, end=end,
                                                   retry_count=retry_count,
                                                   pause=pause,
                                                   session=session)

    @property
    def service(self):
        return "stats/historical"

    def _get_params(self, symbols):
        p = {}

        if self.curr_date is not None:
            p['date'] = self.curr_date.strftime(self.date_format)

        return p

    def read(self):
        """Unfortunately, IEX's API can only retrieve data one day or one month
         at a time. Rather than specifying a date range, we will have to run
         the read function for each date provided.

        :return: DataFrame
        """
        tlen = self.end - self.start
        dfs = []

        # Build list of all dates within the given range
        lrange = [x for x in (self.start + timedelta(n)
                              for n in range(tlen.days))]

        mrange = []
        for dt in lrange:
            if datetime(dt.year, dt.month, 1) not in mrange:
                mrange.append(datetime(dt.year, dt.month, 1))
        lrange = mrange

        for date in lrange:
            self.curr_date = date
            tdf = super(IEX, self).read()

            # We may not return data if this was a weekend/holiday:
            if not tdf.empty:
                tdf['date'] = date.strftime(self.date_format)
                dfs.append(tdf)

        # We may not return any data if we failed to specify useful parameters:
        return pd.concat(dfs) if len(dfs) > 0 else pd.DataFrame()


class RecordsReader(IEX):
    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None):
        super(RecordsReader, self).__init__(symbols=symbols,
                                            start=start, end=end,
                                            retry_count=retry_count,
                                            pause=pause, session=session)

    @property
    def service(self):
        return "stats/records"

    def _get_params(self, symbols):
        # Record Stats API does not take any parameters, returning empty dict
        return {}


class RecentReader(IEX):
    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None):
        super(RecentReader, self).__init__(symbols=symbols,
                                           start=start, end=end,
                                           retry_count=retry_count,
                                           pause=pause, session=session)

    @property
    def service(self):
        return "stats/recent"

    def _get_params(self, symbols):
        # Record Stats API does not take any parameters, returning empty dict
        return {}
