import datetime as dt
from pandas import to_datetime
from requests_file import FileAdapter
from pandas_datareader.compat import is_number

import requests
import requests_ftp

requests_ftp.monkeypatch_session()


class SymbolWarning(UserWarning):
    pass


class RemoteDataError(IOError):
    pass


def _sanitize_dates(start, end):
    """
    Return (datetime_start, datetime_end) tuple
    if start is None - default is 2010/01/01
    if end is None - default is today
    """
    if is_number(start):
        # regard int as year
        start = dt.datetime(start, 1, 1)
    start = to_datetime(start)

    if is_number(end):
        end = dt.datetime(end, 1, 1)
    end = to_datetime(end)

    if start is None:
        start = dt.datetime(2010, 1, 1)
    if end is None:
        end = dt.datetime.today()
    return start, end


def _init_session(session, retry_count=3):
    if session is None:
        session = requests.Session()
        session.mount('file://', FileAdapter())
        # do not set requests max_retries here to support arbitrary pause
    return session
