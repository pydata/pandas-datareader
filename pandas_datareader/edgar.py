import re
import datetime as dt
from ftplib import FTP
import gzip

from zipfile import ZipFile
from pandas.compat import StringIO
from pandas import read_csv, DataFrame, to_datetime

from pandas_datareader.base import _BaseReader
from pandas_datareader._utils import RemoteDataError
from pandas_datareader.compat import BytesIO, is_number


_URL_FULL = 'edgar/full-index/master.zip'
_URL_DAILY = 'ftp://ftp.sec.gov/'
_SEC_FTP = 'ftp.sec.gov'

_COLUMNS = ['cik', 'company_name', 'form_type', 'date_filed', 'filename']
_COLUMN_TYPES = {'cik': str, 'company_name': str, 'form_type': str,
                 'date_filed': str, 'filename': str}
_DIVIDER = re.compile('--------------')
_EDGAR = 'edgar/'
_EDGAR_DAILY = 'edgar/daily-index'
_EDGAR_RE = re.compile(_EDGAR)
_EDGAR_MIN_DATE = dt.datetime(1994, 7, 1)
_ZIP_RE = re.compile('\.zip$')
_GZ_RE = re.compile('\.gz$')

_MLSD_VALUES_RE = re.compile('modify=(?P<modify>.*?);.*'
                             'type=(?P<type>.*?);.*'
                             '; (?P<name>.*)$')
_FILENAME_DATE_RE = re.compile('\w*?\.(\d*)\.idx')
_FILENAME_MASTER_RE = re.compile('master\.\d*\.idx')
_EDGAR_MAX_6_DIGIT_DATE = dt.datetime(1998, 5, 15)


class EdgarIndexReader(_BaseReader):
    """
    Get master index from the SEC's EDGAR database.

    Returns
    -------
    edgar_index : pandas.DataFrame.
        DataFrame of EDGAR index.
    """

    @property
    def url(self):
        if self.symbols == 'full':
            return _URL_FULL
        elif self.symbols == 'daily':
            return _URL_DAILY
        else:
            return _URL_FULL  # Should probably raise or use full unless daily.

    def _read_zipfile(self, ftppath):

        zipf = BytesIO()
        try:
            self._sec_ftp_session.retrbinary('RETR ' + ftppath, zipf.write)
        except EOFError:
            raise RemoteDataError('FTP server has closed the connection.')
        zipf.seek(0)
        with ZipFile(zipf, 'r') as zf:
            data = zf.open(zf.namelist()[0]).read().decode()

        return StringIO(data)

    def _read_gzfile(self, ftppath):

        zipf = BytesIO()
        try:
            self._sec_ftp_session.retrbinary('RETR ' + ftppath, zipf.write)
        except EOFError:
            raise RemoteDataError('FTP server has closed the connection.')
        zipf.seek(0)
        zf = gzip.GzipFile(fileobj=zipf, mode='rb')
        try:
            data = zf.read().decode('iso-8859-1')
        finally:
            zf.close()

        return StringIO(data)

    def _read_one_data(self, ftppath, params):

        if re.search(_ZIP_RE, ftppath) is not None:
            index_file = self._read_zipfile(ftppath)
        elif re.search(_GZ_RE, ftppath) is not None:
            index_file = self._read_gzfile(ftppath)
        else:
            index_file = StringIO()
            index_list = []
            try:
                self._sec_ftp_session.retrlines('RETR ' + ftppath,
                                                index_list.append)
            except EOFError:
                raise RemoteDataError('FTP server has closed the connection.')

            for line in index_list:
                index_file.write(line + '\n')
            index_file.seek(0)

        index_file = self._remove_header(index_file)
        index = read_csv(index_file, delimiter='|', header=None,
                         index_col=False, names=_COLUMNS,
                         low_memory=False, dtype=_COLUMN_TYPES)
        index['filename'] = index['filename'].map(self._fix_old_file_paths)
        return index

    def _read_daily_data(self, url, params):
        doc_index = DataFrame()
        file_index = self._get_dir_lists()
        for idx_entry in file_index:
            if self._check_idx(idx_entry):
                daily_idx_path = (idx_entry['path'] + '/' + idx_entry['name'])
                daily_idx = self._read_one_data(daily_idx_path, params)
                doc_index = doc_index.append(daily_idx)
        doc_index['date_filed'] = to_datetime(doc_index['date_filed'],
                                              format='%Y%m%d')
        doc_index.set_index(['date_filed', 'cik'], inplace=True)
        return doc_index

    def _check_idx(self, idx_entry):
        if re.match(_FILENAME_MASTER_RE, idx_entry['name']):
            if idx_entry['date'] is not None:
                if self.start <= idx_entry['date'] <= self.end:
                    return True
        else:
            return False

    def _remove_header(self, data):
        header = True
        cleaned_datafile = StringIO()
        for line in data:
            if header is False:
                cleaned_datafile.write(line + '\n')
            elif re.search(_DIVIDER, line) is not None:
                header = False

        cleaned_datafile.seek(0)
        return cleaned_datafile

    def _fix_old_file_paths(self, path):
        if type(path) == float:  # pd.read_csv turns blank into np.nan
            return path
        if re.match(_EDGAR_RE, path) is None:
            path = _EDGAR + path
        return path

    def read(self):
        try:
            return self._read()
        finally:
            self.close()

    def _read(self):
        try:
            self._sec_ftp_session = FTP(_SEC_FTP, timeout=self.timeout)
            self._sec_ftp_session.login()
        except EOFError:
            raise RemoteDataError('FTP server has closed the connection.')
        try:
            if self.symbols == 'full':
                return self._read_one_data(self.url, self.params)

            elif self.symbols == 'daily':
                return self._read_daily_data(self.url, self.params)
        finally:
            self._sec_ftp_session.close()

    def _sanitize_dates(self, start, end):
        if is_number(start):
            start = dt.datetime(start, 1, 1)
        start = to_datetime(start)

        if is_number(end):
            end = dt.datetime(end, 1, 1)
        end = to_datetime(end)

        if start is None:
            start = dt.datetime(2015, 1, 1)
        if end is None:
            end = dt.datetime(2015, 1, 3)
        if start < _EDGAR_MIN_DATE:
            start = _EDGAR_MIN_DATE

        return start, end

    def _get_dir_lists(self):
        mlsd_tree = self._get_mlsd_tree(_EDGAR_DAILY)
        return mlsd_tree

    def _get_mlsd_tree(self, dir, top=True):
        initial_mlsd = self._get_mlsd(dir)
        mlsd = initial_mlsd[:]
        for entry in initial_mlsd:
            if entry['type'] == 'dir':
                if top is True:
                    if self._check_mlsd_year(entry) is not True:
                        continue
                subdir = dir + '/' + entry['name']
                mlsd.extend(self._get_mlsd_tree(subdir, False))
        return mlsd

    def _get_mlsd(self, dir):
        dir_list = []
        try:
            self._sec_ftp_session.retrlines('MLSD' + ' ' + dir,
                                            dir_list.append)
        except EOFError:
            raise RemoteDataError('FTP server has closed the connection.')

        dict_list = []
        for line in dir_list:
            entry = self._process_mlsd_line(line)
            entry['path'] = dir
            dict_list.append(entry)

        return dict_list

    def _process_mlsd_line(self, line):
        line_dict = re.match(_MLSD_VALUES_RE, line).groupdict()
        line_dict['date'] = self._get_index_date(line_dict['name'])
        return line_dict

    def _get_index_date(self, filename):
        try:
            idx_date = re.search(_FILENAME_DATE_RE, filename).group(1)
            if len(idx_date) == 6:
                if idx_date[-2:] == '94':
                    filedate = dt.datetime.strptime(idx_date, '%m%d%y')
                else:
                    filedate = dt.datetime.strptime(idx_date, '%y%m%d')
                    if filedate > _EDGAR_MAX_6_DIGIT_DATE:
                        filedate = None
            else:  # len(idx_date) == 8:
                filedate = dt.datetime.strptime(idx_date, '%Y%m%d')
        except AttributeError:
            filedate = None

        return filedate

    def _check_mlsd_year(self, entry):
        try:
            if self.start.year <= int(entry['name']) <= self.end.year:
                return True
            else:
                return False
        except TypeError:
            return False
