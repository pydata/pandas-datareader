import tempfile
import urllib
from pandas import read_csv
from pandas.io.common import ZipFile
from pandas.compat import StringIO

from pandas_datareader.base import _BaseReader

_URL = 'ftp://ftp.sec.gov/edgar/full-index/master.zip'
_COLUMNS = ['cik', 'company_name', 'form_type', 'date_filed', 'filename']

class EdgarIndexReader(_BaseReader):
    """
    Get master index from the SEC's EDGAR database.

    Returns
    -------
    edgar_index : pandas.DataFrame.
        WRITE DESCRIPTION OF RETURNED DATA
    """

    @property
    def url(self):
        return _URL

    def _read_zipfile(self, url):
        """
        Trying the one from `famafrench.py` to see if it generalizes.
        Nope.  # raw = self._get_response(url).content
        """
        raw = self._get_response(url)

        with tempfile.TemporaryFile() as tmpf:
            tmpf.write(raw)

            with ZipFile(tmpf, 'r') as zf:
                data = zf.open(zf.namelist()[0]).read().decode()

        return data

    def _read_one_data(self, url, params):

        index_file = StringIO(self._read_zipfile(url))

        index_csv = read_csv(index_file, delimiter='|', header=None,
                             index_col=False, skiprows=10, names=_COLUMNS,
                             low_memory=False)


        return index_csv

    def _get_response(self, url, params=None):
        """ Use urllib to get FTP file.

        Parameters
        ----------
        url : str
            target URL
        params : dict or None
            parameters ignored, as it's FTP.
        """

        # initial attempt + retry
        for i in range(self.retry_count + 1):
            response = urllib.request.urlopen(url).read()
            if response is not None:
                return response
            time.sleep(self.pause)

        raise RemoteDataError('Unable to read URL: {0}'.format(url))
