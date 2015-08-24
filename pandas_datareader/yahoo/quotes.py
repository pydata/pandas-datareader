from collections import defaultdict
import pandas.compat as compat
from pandas.io.common import urlopen
from pandas import DataFrame
from pandas_datareader.utils import _encode_url

_yahoo_codes = {'symbol': 's', 'last': 'l1', 'change_pct': 'p2', 'PE': 'r',
                'time': 't1', 'short_ratio': 's7'}


_URL = 'http://finance.yahoo.com/d/quotes.csv'


def _get_data(symbols):
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

    params = {
        's': sym_list,
        'f': request
    }
    url = _encode_url(_URL, params)

    with urlopen(url) as response:
        lines = response.readlines()

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
