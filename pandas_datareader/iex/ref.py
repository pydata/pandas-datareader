from pandas_datareader.iex import IEX

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class SymbolsReader(IEX):
    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None):
        super(SymbolsReader, self).__init__(symbols=symbols,
                                            start=start, end=end,
                                            retry_count=retry_count,
                                            pause=pause, session=session)

    @property
    def service(self):
        return "ref-data/symbols"

    def _get_params(self, symbols):
        # Ref Data API does not take any parameters, returning empty dict
        return {}
