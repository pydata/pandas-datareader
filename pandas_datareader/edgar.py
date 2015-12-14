from pandas import read_csv
from pandas.io.common import ZipFile
from pandas.compat import StringIO
from pandas.compat import BytesIO

from pandas_datareader.base import _BaseReader


_URL_FULL = 'ftp://ftp.sec.gov/edgar/full-index/master.zip'
_COLUMNS = ['cik', 'company_name', 'form_type', 'date_filed', 'filename']


class EdgarIndexReader(_BaseReader):
    """
    Get master index from the SEC's EDGAR database.

    Returns
    -------
    edgar_index : pandas.DataFrame.
        DataFrame of EDGAR master index.
    """

    @property
    def url(self):
        return _URL_FULL

    def _read_zipfile(self, url):

        zipf = BytesIO(self._get_response(url).content)

        with ZipFile(zipf, 'r') as zf:
            data = zf.open(zf.namelist()[0]).read().decode()

        return data

    def _read_one_data(self, url, params):

        index_file = StringIO(self._read_zipfile(url))

        index = read_csv(index_file, delimiter='|', header=None,
                         index_col=False, skiprows=10, names=_COLUMNS,
                         low_memory=False)
        return index
