import pandas
from pandas_datareader.base import _BaseReader
import json
import re


class GoogleQuotesReader(_BaseReader):

    """Get current google quote"""

    @property
    def url(self):
        return 'http://www.google.com/finance/info'

    @property
    def params(self):
        if isinstance(self.symbols, pandas.compat.string_types):
            sym_list = self.symbols
        else:
            sym_list = ','.join(self.symbols)
        params = {'q': sym_list}
        return params

    def _read_lines(self, out):
        buffer = out.read()
        m = re.search('// ', buffer)
        result = json.loads(buffer[m.start() + len('// '):])
        return pandas.DataFrame([float(x['l']) for x in result],
                                index=[x['t'] for x in result])
