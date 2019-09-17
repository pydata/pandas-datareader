from datetime import datetime, timedelta

import pandas as pd

from pandas_datareader.exceptions import UnstableAPIWarning
from pandas_datareader.iex import IEX

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class DailySummaryReader(IEX):
    """
    Daily statistics from IEX for a day or month
    """

    def __init__(
        self, symbols=None, start=None, end=None, retry_count=3, pause=0.1, session=None
    ):
        import warnings

        warnings.warn(
            "Daily statistics is not working due to issues with the " "IEX API",
            UnstableAPIWarning,
        )
        self.curr_date = start
        super(DailySummaryReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )

    @property
    def service(self):
        """Service endpoint"""
        return "stats/historical/daily"

    def _get_params(self, symbols):
        p = {}

        if self.curr_date is not None:
            p["date"] = self.curr_date.strftime("%Y%m%d")

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
            tdf = super(DailySummaryReader, self).read()
            dfs.append(tdf)
        return pd.concat(dfs)


class MonthlySummaryReader(IEX):
    """Monthly statistics from IEX"""

    def __init__(
        self, symbols=None, start=None, end=None, retry_count=3, pause=0.1, session=None
    ):
        self.curr_date = start
        self.date_format = "%Y%m"

        super(MonthlySummaryReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )

    @property
    def service(self):
        """Service endpoint"""
        return "stats/historical"

    def _get_params(self, symbols):
        p = {}

        if self.curr_date is not None:
            p["date"] = self.curr_date.strftime(self.date_format)

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
        lrange = [x for x in (self.start + timedelta(n) for n in range(tlen.days))]

        mrange = []
        for dt in lrange:
            if datetime(dt.year, dt.month, 1) not in mrange:
                mrange.append(datetime(dt.year, dt.month, 1))
        lrange = mrange

        for date in lrange:
            self.curr_date = date
            tdf = super(MonthlySummaryReader, self).read()

            # We may not return data if this was a weekend/holiday:
            if not tdf.empty:
                tdf["date"] = date.strftime(self.date_format)
                dfs.append(tdf)

        # We may not return any data if we failed to specify useful parameters:
        return pd.concat(dfs) if len(dfs) > 0 else pd.DataFrame()


class RecordsReader(IEX):
    """
    Total matched volume information from IEX
    """

    def __init__(
        self, symbols=None, start=None, end=None, retry_count=3, pause=0.1, session=None
    ):
        super(RecordsReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )

    @property
    def service(self):
        """Service endpoint"""
        return "stats/records"

    def _get_params(self, symbols):
        # Record Stats API does not take any parameters, returning empty dict
        return {}


class RecentReader(IEX):
    """Recent trading volume from IEX

    Notes
    -----
    Returns 6 fields for each day:

      * date: refers to the trading day.
      * volume: refers to executions received from order routed to away
        trading centers.
      * routedVolume: refers to single counted shares matched from executions
        on IEX.
      * marketShare: refers to IEX's percentage of total US Equity market
        volume.
      * isHalfday: will be true if the trading day is a half day.
      * litVolume: refers to the number of lit shares traded on IEX
        (single-counted).
    """

    def __init__(
        self, symbols=None, start=None, end=None, retry_count=3, pause=0.1, session=None
    ):
        super(RecentReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )

    @property
    def service(self):
        """Service endpoint"""
        return "stats/recent"

    def _get_params(self, symbols):
        # Record Stats API does not take any parameters, returning empty dict
        return {}
