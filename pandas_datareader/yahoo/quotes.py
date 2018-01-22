from collections import defaultdict
import csv

import pandas.compat as compat
from pandas import DataFrame

from pandas_datareader.base import _BaseReader

_yahoo_codes = {'symbol': 's', 'last': 'l1', 'change_pct': 'p2', 'PE': 'r',
                'time': 't1', 'short_ratio': 's7'}


class YahooQuotesReader(_BaseReader):

    """Get current yahoo quote"""

    @property
    def url(self):
        return 'http://finance.yahoo.com/d/quotes.csv'

    @property
    def params(self):
        """Parameters to use in API calls"""
        if isinstance(self.symbols, compat.string_types):
            sym_list = self.symbols
        else:
            sym_list = '+'.join(self.symbols)

        # For codes see: http://www.gummy-stuff.org/Yahoo-data.htm
        #
        # Construct the code request string.
        request = ''.join(compat.itervalues(_yahoo_codes))
        params = {'s': sym_list, 'f': request}
        return params

    def _read_lines(self, out):
        data = defaultdict(list)
        header = list(_yahoo_codes.keys())

        for line in csv.reader(out.readlines()):
            for i, field in enumerate(line):
                if field[-2:] == '%"':
                    v = float(field.strip('"%'))
                elif field[0] == '"':
                    v = field.strip('"')
                else:
                    try:
                        v = float(field)
                    except ValueError:
                        v = field
                data[header[i]].append(v)

        idx = data.pop('symbol')
        return DataFrame(data, index=idx)
