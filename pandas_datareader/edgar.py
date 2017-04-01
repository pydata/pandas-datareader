import re
import datetime as dt
import requests

from pandas import date_range
from pandas import read_csv
from pandas import to_datetime
from pandas.io.common import ZipFile
from pandas.compat import StringIO
from pandas.core.common import is_number

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import BytesIO

_URL_STEM = 'https://s3.amazonaws.com/indexes.sec.gov/'
_URL_FULL = 'full-index/'
_URL_DAILY = 'daily-index/'
_URL_MIDDLE = 'YYYY/QQQQ/'
_URL_FULL_END = 'master.zip'
_URL_DAILY_END = 'master.YYYYMMDD.idx'
_URL_DIR_END = 'index.json'
_URL_QTR = ['QTR1', 'QTR1', 'QTR1',
            'QTR2', 'QTR2', 'QTR2',
            'QTR3', 'QTR3', 'QTR3',
            'QTR4', 'QTR4', 'QTR4']

_COLUMNS = ['cik', 'company_name', 'form_type', 'date_filed', 'filename']
_COLUMN_TYPES = {'cik': str, 'company_name': str, 'form_type': str,
                 'date_filed': str, 'filename': str}

_DIVIDER = re.compile('--------------')

_EDGAR_MIN_DATE = dt.datetime(1994, 7, 1)
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
        if self.symbols == 'daily':
            return _URL_STEM + _URL_DAILY
        else:
            return _URL_STEM + _URL_FULL

    def read(self):
        self._sanitize_dates(start=self.start, end=self.end)
        self.gen_tree()
        if self.symbols == 'full':
            return self._read_full_data()
        elif self.symbols == 'daily':
            return self._read_daily_data()

    def gen_tree(self):
        temp_dates = date_range(start=self.start, end=self.end, freq='3M')
        self._URL_YEAR_AND_QTR = []
        if len(temp_dates.date) == 0:
            self._URL_YEAR_AND_QTR.append(str(self.start.year) + '/' + _URL_QTR[self.start.month] + '/')
        for date in temp_dates:
            self._URL_YEAR_AND_QTR.append(str(date.year) + '/' + _URL_QTR[date.month]+'/')
        return

    def _read_full_data(self):
        index_file = StringIO()
        for instance in self._URL_YEAR_AND_QTR:
            temp_url = self.url + instance + _URL_FULL_END
            temp = self._read_zipfile(temp_url)
            index_file.write(temp.read())
        index_file.seek(0)
        index = read_csv(index_file, delimiter='|', header=None,
                         index_col=False, names=_COLUMNS,
                         low_memory=False, dtype=_COLUMN_TYPES)
        return index

    def _read_daily_data(self):
        index_file = StringIO()
        for instance in self._URL_YEAR_AND_QTR:
            temp_url = self.url + instance + _URL_DIR_END
            r = requests.get(temp_url)
            index_json = r.json()
            file_list_by_date = {}
            for item in index_json['directory']['item']:
                file_date = self._get_index_date(item['href'].split('.')[1])
                if file_date is not None:
                    file_list_by_date[file_date] = item['href']
            for file_date in file_list_by_date.keys():
                if ((self.start <= file_date < self.end) or
                   (self.start == self.end == file_date)):
                    index_file.write(self._read_idx(self.url + instance +
                                                    file_list_by_date[file_date]).read())
        index_file.seek(0)
        the_index = read_csv(index_file, delimiter='|', header=None,
                             index_col=False, names=_COLUMNS,
                             low_memory=False, dtype=_COLUMN_TYPES)
        return the_index

    def _read_zipfile(self, url):
        zipf = BytesIO()
        r = requests.get(url)
        r.raise_for_status()
        zipf.write(r.content)
        zipf.seek(0)
        with ZipFile(zipf, 'r') as zf:
            d = zf.open(zf.namelist()[0]).read().decode(encoding='iso-8859-1')
        return self._remove_header(StringIO(d))

    def _read_idx(self, url):
        r = requests.get(url)
        r.raise_for_status()
        data = r.content
        temp = StringIO(str(data))
        data = []
        for line in temp.getvalue().split('\\n'):
            data.append(line)
        return self._remove_header(data)

    def _remove_header(self, data):
        header = True
        cleaned_datafile = StringIO()
        for line in data:
            if header is False:
                cleaned_datafile.write(line + '\n')
            elif re.search(_DIVIDER, line) is not None:
                header = False
        cleaned_datafile.seek(0)
        return self._normalize_date_column(cleaned_datafile)

    def _normalize_date_column(self, data):
        cleaned_data = StringIO()
        for line in data:
            row = line.split('|')
            if len(row) == 5:
                row[3] = self._get_index_date(row[3]).date().isoformat()
                row = '|'.join(row)
                cleaned_data.write(row)
        cleaned_data.seek(0)
        return cleaned_data

    def _get_index_date(self, idx_date):
        try:
            if len(idx_date) == 10:
                fd = dt.datetime.strptime(''.join(idx_date.split('-')), '%Y%m%d')
            elif len(idx_date) == 8:
                fd = dt.datetime.strptime(idx_date, '%Y%m%d')
            elif len(idx_date) == 6:
                if idx_date[-2:] == '94':
                    fd = dt.datetime.strptime(idx_date, '%m%d%y')
                else:
                    fd = dt.datetime.strptime(idx_date, '%y%m%d')
                    if fd > _EDGAR_MAX_6_DIGIT_DATE:
                        fd = None
        except AttributeError:
            fd = None
        return fd

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
        timedelta = dt.timedelta(days=1825)
        if end - start > timedelta:
            end = start + timedelta
        self.start = start
        self.end = end
        return start, end
