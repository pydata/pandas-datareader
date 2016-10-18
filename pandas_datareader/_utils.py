import datetime as dt
from distutils.version import LooseVersion

import pandas as pd
from pandas.core.common import PandasError
from pandas import to_datetime

import requests
from requests_file import FileAdapter
import requests_ftp
requests_ftp.monkeypatch_session()


if pd.compat.PY3:
    from urllib.error import HTTPError     # noqa
else:
    from urllib2 import HTTPError          # noqa

PANDAS_VERSION = LooseVersion(pd.__version__)

if PANDAS_VERSION >= LooseVersion('0.19.0'):
    PANDAS_0190 = True
    from pandas.api.types import is_number                   # noqa
else:
    PANDAS_0190 = False
    from pandas.core.common import is_number                 # noqa

if PANDAS_VERSION >= LooseVersion('0.17.0'):
    PANDAS_0170 = True
else:
    PANDAS_0170 = False

if PANDAS_VERSION >= LooseVersion('0.16.0'):
    PANDAS_0160 = True
else:
    PANDAS_0160 = False

if PANDAS_VERSION >= LooseVersion('0.14.0'):
    PANDAS_0140 = True
else:
    PANDAS_0140 = False


class SymbolWarning(UserWarning):
    pass


class RemoteDataError(PandasError, IOError):
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
