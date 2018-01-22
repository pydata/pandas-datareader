from pandas_datareader.iex import IEX

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class TopsReader(IEX):
    """
    Near-real time aggregated bid and offer positions

    Notes
    -----
    IEX's aggregated best quoted bid and offer position for all securities
    on IEX's displayed limit order book.
    """

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None):
        super(TopsReader, self).__init__(symbols=symbols,
                                         start=start, end=end,
                                         retry_count=retry_count,
                                         pause=pause, session=session)

    @property
    def service(self):
        """Service endpoint"""
        return "tops"


class LastReader(IEX):
    """
    Information of executions on IEX

    Notes
    -----
    Last provides trade data for executions on IEX. Provides last sale price,
    size and time.
    """
    # todo: Eventually we'll want to implement WebSockets as an option.
    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None):
        super(LastReader, self).__init__(symbols=symbols,
                                         start=start, end=end,
                                         retry_count=retry_count,
                                         pause=pause, session=session)

    @property
    def service(self):
        """Service endpoint"""
        return "tops/last"
