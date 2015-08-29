import tempfile
import re
import datetime as dt
from pandas.io.common import urlopen, ZipFile
from pandas.compat import lmap, StringIO
from pandas import read_csv, to_datetime


_URL = 'http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/'
_URL_PREFIX = 'ftp/'
_URL_SUFFIX = '_CSV.zip'


def get_available_datasets():
    """
    Get the list of datasets available from the Fama/French data library.

    Returns
    -------
    A list of valid inputs for get_data_famafrench.
    """
    try:
        from lxml.html import parse
    except ImportError:
        raise ImportError("Please install lxml if you want to use the "
                          "get_datasets_famafrench function")

    root = parse(_URL + 'data_library.html')

    l = filter(lambda x: x.startswith(_URL_PREFIX) and x.endswith(_URL_SUFFIX),
               [e.attrib['href'] for e in root.findall('.//a') if 'href' in e.attrib])

    return lmap(lambda x: x[len(_URL_PREFIX):-len(_URL_SUFFIX)], l)


def _download_data_famafrench(name):
    url = ''.join([_URL, _URL_PREFIX, name, _URL_SUFFIX])
    with urlopen(url) as socket:
        raw = socket.read()

    with tempfile.TemporaryFile() as tmpf:
        tmpf.write(raw)

        with ZipFile(tmpf, 'r') as zf:
            data = zf.open(zf.namelist()[0]).read().decode()

    return data


def _parse_date_famafrench(x):
    x = x.strip()
    try: return dt.datetime.strptime(x, '%Y%m')
    except: pass
    return to_datetime(x)


def _get_data(name):
    """
    Get data for the given name from the Fama/French data library.

    For annual and monthly data, index is a pandas.PeriodIndex, otherwise
    it's a pandas.DatetimeIndex.

    Returns
    -------
    df : a dictionary of pandas.DataFrame. Tables are accessed by integer keys.
         See df['DESCR'] for a description of the dataset
    """
    params = {'index_col': 0,
              'parse_dates': [0],
              'date_parser': _parse_date_famafrench}

    # headers in these files are not valid
    if name.endswith('_Breakpoints'):
        c = ['<=0', '>0'] if name.find('-') > -1 else ['Count']
        r = list(range(0, 105, 5))
        params['names'] = ['Date'] + c + list(zip(r, r[1:]))
        params['skiprows'] = 1 if name != 'Prior_2-12_Breakpoints' else 3

    doc_chunks, tables = [], []
    data = _download_data_famafrench(name)
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
        try: df = df.to_period(df.index.inferred_freq[:1])
        except: pass
        datasets[i] = df

        title = src[:start].replace('\r\n', ' ').strip()
        shape = '({0} rows x {1} cols)'.format(*df.shape)
        table_desc.append('{0} {1}'.format(title, shape).strip())

    descr = '{0}\n{1}\n\n'.format(name.replace('_', ' '), len(name) * '-')
    if doc_chunks: descr += ' '.join(doc_chunks).replace(2 * ' ', ' ') + '\n\n'

    table_descr = map(lambda x: '{0:3} : {1}'.format(*x), enumerate(table_desc))

    datasets['DESCR'] = descr + '\n'.join(table_descr)
    return datasets
