from distutils.version import LooseVersion
import sys

import pandas as pd
from pandas.api.types import is_list_like, is_number
import pandas.io.common as com
from pandas.util.testing import assert_frame_equal

PY3 = sys.version_info >= (3, 0)

PANDAS_VERSION = LooseVersion(pd.__version__)

PANDAS_0210 = PANDAS_VERSION >= LooseVersion("0.21.0")
PANDAS_0220 = PANDAS_VERSION >= LooseVersion("0.22.0")
PANDAS_0230 = PANDAS_VERSION >= LooseVersion("0.23.0")


__all__ = [
    "HTTPError",
    "StringIO",
    "PANDAS_0210",
    "PANDAS_0220",
    "PANDAS_0230",
    "get_filepath_or_buffer",
    "str_to_bytes",
    "string_types",
    "assert_frame_equal",
    "is_list_like",
    "is_number",
    "lmap",
    "lrange",
    "concat",
]


def get_filepath_or_buffer(filepath_or_buffer, encoding=None, compression=None):

    # Dictionaries are no longer considered valid inputs
    # for "get_filepath_or_buffer" starting in pandas >= 0.20.0
    if isinstance(filepath_or_buffer, dict):
        return filepath_or_buffer, encoding, compression

    return com.get_filepath_or_buffer(
        filepath_or_buffer, encoding=encoding, compression=None
    )


if PY3:
    from urllib.error import HTTPError
    from functools import reduce

    string_types = (str,)
    binary_type = bytes
    from io import StringIO

    def str_to_bytes(s, encoding=None):
        return s.encode(encoding or "ascii")

    def bytes_to_str(b, encoding=None):
        return b.decode(encoding or "utf-8")


else:
    from urllib2 import HTTPError
    from cStringIO import StringIO

    reduce = reduce
    binary_type = str
    string_types = (basestring,)  # noqa: F821

    def bytes_to_str(b, encoding=None):
        return b

    def str_to_bytes(s, encoding=None):
        return s


def lmap(*args, **kwargs):
    return list(map(*args, **kwargs))


def lrange(*args, **kwargs):
    return list(range(*args, **kwargs))


def concat(*args, **kwargs):
    """
    Shim to wokr around sort keyword
    """
    if not PANDAS_0230 and "sort" in kwargs:
        del kwargs["sort"]
    return pd.concat(*args, **kwargs)
