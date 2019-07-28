# flake8: noqa
import pandas as pd
import pandas.io.common as com
import sys
from distutils.version import LooseVersion
from io import BytesIO

PY3 = sys.version_info >= (3, 0)

PANDAS_VERSION = LooseVersion(pd.__version__)

PANDAS_0190 = (PANDAS_VERSION >= LooseVersion('0.19.0'))
PANDAS_0200 = (PANDAS_VERSION >= LooseVersion('0.20.0'))
PANDAS_0210 = (PANDAS_VERSION >= LooseVersion('0.21.0'))
PANDAS_0230 = (PANDAS_VERSION >= LooseVersion('0.23.0'))

if PANDAS_0190:
    from pandas.api.types import is_number
    from pandas.util.testing import assert_frame_equal
else:
    from pandas.core.common import is_number
    from pandas.testing import assert_frame_equal

if PANDAS_0200:
    from pandas.util.testing import assert_raises_regex

    def get_filepath_or_buffer(filepath_or_buffer, encoding=None,
                               compression=None):

        # Dictionaries are no longer considered valid inputs
        # for "get_filepath_or_buffer" starting in pandas >= 0.20.0
        if isinstance(filepath_or_buffer, dict):
            return filepath_or_buffer, encoding, compression

        return com.get_filepath_or_buffer(
            filepath_or_buffer, encoding=encoding, compression=None)
else:
    from pandas.util.testing import assertRaisesRegexp as assert_raises_regex
    get_filepath_or_buffer = com.get_filepath_or_buffer

if PANDAS_0230:
    from pandas.core.dtypes.common import is_list_like
else:
    from pandas.core.common import is_list_like

if PY3:
    from urllib.error import HTTPError
    from functools import reduce

    string_types = str,
    binary_type = bytes


    def str_to_bytes(s, encoding=None):
        return s.encode(encoding or 'ascii')


    def bytes_to_str(b, encoding=None):
        return b.decode(encoding or 'utf-8')
else:
    from urllib2 import HTTPError

    reduce = reduce
    binary_type = str
    string_types = basestring,


    def bytes_to_str(b, encoding=None):
        return b


    def str_to_bytes(s, encoding=None):
        return s


def lmap(*args, **kwargs):
    return list(map(*args, **kwargs))


def lrange(*args, **kwargs):
    return list(range(*args, **kwargs))
