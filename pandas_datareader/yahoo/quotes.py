from collections import defaultdict
import pandas.compat as compat
from pandas.io.common import urlopen
from pandas import DataFrame


_yahoo_codes = {'symbol': 's', 'last': 'l1', 'change_pct': 'p2', 'PE': 'r',
                'time': 't1', 'short_ratio': 's7'}


_YAHOO_QUOTE_URL = 'http://finance.yahoo.com/d/quotes.csv?'


def get_quote_yahoo(symbols):
    """
    Get current yahoo quote

    Returns a DataFrame
    """
    if isinstance(symbols, compat.string_types):
        sym_list = symbols
    else:
        sym_list = '+'.join(symbols)

    # for codes see: http://www.gummy-stuff.org/Yahoo-data.htm
    request = ''.join(compat.itervalues(_yahoo_codes))  # code request string
    header = list(_yahoo_codes.keys())

    data = defaultdict(list)

    url_str = _YAHOO_QUOTE_URL + 's=%s&f=%s' % (sym_list, request)

    with urlopen(url_str) as url:
        lines = url.readlines()

    for line in lines:
        fields = line.decode('utf-8').strip().split(',')
        for i, field in enumerate(fields):
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
