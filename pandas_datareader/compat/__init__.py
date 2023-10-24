from io import StringIO
from urllib.error import HTTPError

from packaging import version
import pandas as pd
from pandas.api.types import is_list_like, is_number
from pandas.io import common as com
from pandas.testing import assert_frame_equal

__all__ = [
    "HTTPError",
    "StringIO",
    "get_filepath_or_buffer",
    "assert_frame_equal",
    "is_list_like",
    "is_number",
    "reduce",
]


def get_filepath_or_buffer(filepath_or_buffer, encoding=None, compression=None):
    # Dictionaries are no longer considered valid inputs
    # for "get_filepath_or_buffer" starting in pandas >= 0.20.0
    if isinstance(filepath_or_buffer, dict):
        return filepath_or_buffer, encoding, compression
    try:
        tmp = com._get_filepath_or_buffer(
            filepath_or_buffer, encoding=encoding, compression=None
        )
        return tmp.filepath_or_buffer, tmp.encoding, tmp.compression
    except AttributeError:
        tmp = com.get_filepath_or_buffer(
            filepath_or_buffer, encoding=encoding, compression=None
        )
        return tmp
