import datetime as dt
import re
import tempfile
from zipfile import ZipFile

from pandas import read_csv, to_datetime
from pandas.compat import lmap, StringIO

from pandas_datareader.base import _BaseReader

_URL = 'http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/'
_URL_PREFIX = 'ftp/'
_URL_SUFFIX = '_CSV.zip'


def get_available_datasets(**kwargs):
    """
    Get the list of datasets available from the Fama/French data library.

    Parameters
    ----------
    session : Session, default None
        requests.sessions.Session instance to be used

    Returns
    -------
    A list of valid inputs for get_data_famafrench.
    """
    return FamaFrenchReader(symbols='', **kwargs).get_available_datasets()


def _parse_date_famafrench(x):
    x = x.strip()
    try:
        return dt.datetime.strptime(x, '%Y%m')
    except Exception:
        pass
    return to_datetime(x)


class FamaFrenchReader(_BaseReader):
    """
    Get data for the given name from the Fama/French data library.

    For annual and monthly data, index is a pandas.PeriodIndex, otherwise
    it's a pandas.DatetimeIndex.
    """

    @property
    def url(self):
        """API URL"""
        return ''.join([_URL, _URL_PREFIX, self.symbols, _URL_SUFFIX])

    def _read_zipfile(self, url):
        raw = self._get_response(url).content

        with tempfile.TemporaryFile() as tmpf:
            tmpf.write(raw)

            with ZipFile(tmpf, 'r') as zf:
                data = zf.open(zf.namelist()[0]).read().decode()

        return data

    def read(self):
        """
        Read data

        Returns
        -------
        df : dict
            A dictionary of DataFrames. Tables are accessed by integer keys.
            See df['DESCR'] for a description of the data set.
        """
        return super(FamaFrenchReader, self).read()

    def _read_one_data(self, url, params):

        params = {'index_col': 0,
                  'parse_dates': [0],
                  'date_parser': _parse_date_famafrench}

        # headers in these files are not valid
        if self.symbols.endswith('_Breakpoints'):

            if self.symbols.find('-') > -1:
                c = ['<=0', '>0']
            else:
                c = ['Count']
            r = list(range(0, 105, 5))
            params['names'] = ['Date'] + c + list(zip(r, r[1:]))

            if self.symbols != 'Prior_2-12_Breakpoints':
                params['skiprows'] = 1
            else:
                params['skiprows'] = 3

        doc_chunks, tables = [], []
        data = self._read_zipfile(url)

        for chunk in data.split(2 * '\r\n'):
            if len(chunk) < 800:
                doc_chunks.append(chunk.replace('\r\n', ' ').strip())
            else:
                tables.append(chunk)

        datasets, table_desc = {}, []
        for i, src in enumerate(tables):
            match = re.search('^\s*,', src, re.M)  # the table starts there
            start = 0 if not match else match.start()

            df = read_csv(StringIO('Date' + src[start:]), **params)
            try:
                idx_name = df.index.name  # hack for pandas 0.16.2
                df = df.to_period(df.index.inferred_freq[:1])
                df.index.name = idx_name
            except Exception:
                pass
            df = df.truncate(self.start, self.end)
            datasets[i] = df

            title = src[:start].replace('\r\n', ' ').strip()
            shape = '({0} rows x {1} cols)'.format(*df.shape)
            table_desc.append('{0} {1}'.format(title, shape).strip())

        descr = '{0}\n{1}\n\n'.format(self.symbols.replace('_', ' '),
                                      len(self.symbols) * '-')
        if doc_chunks:
            descr += ' '.join(doc_chunks).replace(2 * ' ', ' ') + '\n\n'
        table_descr = map(lambda x: '{0:3} : {1}'.format(*x),
                          enumerate(table_desc))
        datasets['DESCR'] = descr + '\n'.join(table_descr)

        return datasets

    def get_available_datasets(self):
        """
        Get the list of datasets available from the Fama/French data library.

        Returns
        -------
        datasets: list
            A list of valid inputs for get_data_famafrench
        """
        try:
            from lxml.html import document_fromstring
        except ImportError:
            raise ImportError("Please install lxml if you want to use the "
                              "get_datasets_famafrench function")

        response = self.session.get(_URL + 'data_library.html')
        root = document_fromstring(response.content)

        datasets = [e.attrib['href'] for e in root.findall('.//a')
                    if 'href' in e.attrib]
        datasets = [ds for ds in datasets if ds.startswith(_URL_PREFIX)
                    and ds.endswith(_URL_SUFFIX)]

        return lmap(lambda x: x[len(_URL_PREFIX):-len(_URL_SUFFIX)], datasets)
