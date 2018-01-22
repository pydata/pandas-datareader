from pandas_datareader.iex import IEX
from datetime import datetime

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class Deep(IEX):
    """
    Retrieve order book data from IEX

    Notes
    -----
    Real-time depth of book quotations direct from IEX. Returns aggregated
    size of resting displayed orders at a price and side. Does not indicate
    the size or number of individual orders at any price level. Non-displayed
    orders and non-displayed portions of reserve orders are not counted.

    Also provides last trade price and size information. Routed executions
    are not reported.
    """
    def __init__(self, symbols=None, service=None, start=None, end=None,
                 retry_count=3, pause=0.001, session=None):
        if isinstance(symbols, str):
            symbols = symbols.lower()
        else:
            symbols = [s.lower() for s in symbols]

        super(Deep, self).__init__(symbols=symbols,
                                   start=start, end=end,
                                   retry_count=retry_count,
                                   pause=pause, session=session)
        self.sub = service

    @property
    def service(self):
        """Service endpoint"""
        ss = "/" + self.sub if self.sub is not None else ""
        return "deep{}".format(ss)

    def _read_lines(self, out):
        """
        IEX depth of book data varies and shouldn't always be returned in a DF

        :param out: Raw HTTP Output
        :return: DataFrame
        """

        # Runs appropriate output functions per the service being accessed.
        fmap = {
            'book': '_pass',
            'op-halt-status': '_convert_tstamp',
            'security-event': '_convert_tstamp',
            'ssr-status': '_convert_tstamp',
            'system-event': '_read_system_event',
            'trades': '_pass',
            'trade-breaks': '_convert_tstamp',
            'trading-status': '_read_trading_status',
            None: '_pass',
        }

        if self.sub in fmap:
            return getattr(self, fmap[self.sub])(out)
        else:
            raise "Invalid service specified: {}.".format(self.sub)

    def _read_system_event(self, out):
        # Map the response code to a string output per the API docs.
        # Per: https://www.iextrading.com/developer/docs/#system-event-message
        smap = {
            'O': 'Start of messages',
            'S': 'Start of system hours',
            'R': 'Start of regular market hours',
            'M': 'End of regular market hours',
            'E': 'End of system hours',
            'C': 'End of messages'
        }
        tid = out["systemEvent"]
        out["eventResponse"] = smap[tid]

        return self._convert_tstamp(out)

    @staticmethod
    def _pass(out):
        return out

    def _read_trading_status(self, out):
        # Reference: https://www.iextrading.com/developer/docs/#trading-status
        smap = {
            'H': 'Trading halted across all US equity markets',
            'O': 'Trading halt released into an Order Acceptance Period '
                 '(IEX-listed securities only)',
            'P': 'Trading paused and Order Acceptance Period on IEX '
                 '(IEX-listed securities only)',
            'T': 'Trading on IEX'
        }
        rmap = {
            # Trading Halt Reasons
            'T1': 'Halt News Pending',
            'IPO1': 'IPO/New Issue Not Yet Trading',
            'IPOD': 'IPO/New Issue Deferred',
            'MCB3': 'Market-Wide Circuit Breaker Level 3 - Breached',
            'NA': 'Reason Not Available',

            # Order Acceptance Period Reasons
            'T2': 'Halt News Dissemination',
            'IPO2': 'IPO/New Issue Order Acceptance Period',
            'IPO3': 'IPO Pre-Launch Period',
            'MCB1': 'Market-Wide Circuit Breaker Level 1 - Breached',
            'MCB2': 'Market-Wide Circuit Breaker Level 2 - Breached'
        }
        for ticker, data in out.items():
            if data['status'] in smap:
                data['statusText'] = smap[data['status']]

            if data['reason'] in rmap:
                data['reasonText'] = rmap[data['reason']]

            out[ticker] = data

        return self._convert_tstamp(out)

    @staticmethod
    def _convert_tstamp(out):
        # Searches for top-level timestamp attributes or within dictionaries
        if 'timestamp' in out:
            # Convert UNIX to datetime object
            f = float(out["timestamp"])
            out["timestamp"] = datetime.fromtimestamp(f/1000)
        else:
            for ticker, data in out.items():
                if 'timestamp' in data:
                    f = float(data["timestamp"])
                    data["timestamp"] = datetime.fromtimestamp(f/1000)
                    out[ticker] = data

        return out
