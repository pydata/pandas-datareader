from pandas_datareader.iex import IEX

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class SymbolsReader(IEX):
    """
    Symbols available for trading on IEX

    Notes
    -----
    Returns symbols IEX supports for trading. Updated daily as of 7:45 a.m.
    ET.
    """

    def __init__(
        self, symbols=None, start=None, end=None, retry_count=3, pause=0.1, session=None
    ):
        super(SymbolsReader, self).__init__(
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
        return "ref-data/symbols"

    def _get_params(self, symbols):
        # Ref Data API does not take any parameters, returning empty dict
        return {}
