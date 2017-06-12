import re

from pandas_datareader.base import _DailyBaseReader


class QuandlReader(_DailyBaseReader):

    """
    Returns DataFrame of historical stock prices from symbol, over date
    range, start to end.

    Parameters
    ----------
    symbols : string.
        Possible formats:
        1. DB/SYM: The Quandl 'codes': DB is the database name,
        SYM is a ticker-symbol-like Quandl abbreviation for a particular security.
        2. SYM.CC: SYM is the same symbol and CC is an ISO country code,
        will try to map to the best single Quandl database for that country.
        Beware of ambiguous symbols (different securities per country)!
        Note: Cannot use more than a single string because of the inflexible way
        the URL is composed of url and _get_params in the superclass.
    start : string,
        Starting date, timestamp. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : string, (defaults to today)
        Ending date, timestamp. Same format as starting date.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    chunksize : int, default 25
        Number of symbols to download consecutively before intiating pause.
    session : Session, default None
        requests.sessions.Session instance to be used
    """

    @property
    def url(self):
        symbol = self.symbols if isinstance(self.symbols, str) else self.symbols[0]
        mm = re.fullmatch(r"([A-Z0-9]+)(([/\.])([A-Z0-9_]+))?", symbol)
        assert mm, ("Symbol '%s' must conform to Quandl convention 'DB/SYM'" %
                    symbol)
        if not mm.group(2):
            #--- bare symbol:
            datasetname = 'WIKI'  # default; symbol stays itself
        elif mm.group(3) == "/":
            # --- normal Quandl DB/SYM convention:
            symbol = mm.group(4)
            datasetname = mm.group(1)
        elif mm.group(3) == ".":
            #--- secondary convention SYM.CountryCode:
            symbol = mm.group(1)
            datasetname = self._db_from_countrycode(mm.group(4))
        base_url = "https://www.quandl.com/api/v3/datasets/"
        params = {
            'q': symbol,
            'start_date': self.start.strftime('%Y-%m-%d'),
            'end_date': self.end.strftime('%Y-%m-%d'),
            'order': "asc",
        }
        paramstring = '&'.join(['%s=%s' % (k,v) for k,v in params.items()])
        return '%s%s/%s.csv?%s' % (base_url, datasetname, symbol, paramstring)

    def _db_from_countrycode(self, code):
        map = dict(BE='EURONEXT', # https://www.quandl.com/data/EURONEXT-Euronext-Stock-Exchange
                   CN='HKEX',     # https://www.quandl.com/data/HKEX-Hong-Kong-Exchange
                   DE='SSE',      # https://www.quandl.com/data/SSE-Boerse-Stuttgart
                   FR='EURONEXT', #
                   IN='NSE',      # https://www.quandl.com/data/NSE-National-Stock-Exchange-of-India
                   JP='TSE',      # https://www.quandl.com/data/TSE-Tokyo-Stock-Exchange
                   NL='EURONEXT', #
                   PT='EURONEXT', #
                   UK='LSE',      # https://www.quandl.com/data/LSE-London-Stock-Exchange
                   US='WIKI',     # https://www.quandl.com/data/WIKI-Wiki-EOD-Stock-Prices
                  )
        assert code in map, "No Quandl database known for country code '%s'" % code
        return map[code]

    def _get_params(self, symbol):
        return {}

    def read(self):
        df = super(QuandlReader, self).read()
        df.rename(columns=lambda n: n.replace(' ', '')
                                     .replace('.', '')
                                     .replace('/', '')
                                     .replace('%', '')
                                     .replace('(', '')
                                     .replace(')', '')
                                     .replace("'", '')
                                     .replace('-', ''),
                         inplace=True)
        print(self.url)
        return df