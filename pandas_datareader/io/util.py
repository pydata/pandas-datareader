from __future__ import unicode_literals

import os

import pandas.compat as compat
from pandas_datareader.compat import get_filepath_or_buffer


def _read_content(path_or_buf):
    """
    Copied part of internal logic from pandas.io.read_json.
    """

    filepath_or_buffer = get_filepath_or_buffer(path_or_buf)[0]

    if isinstance(filepath_or_buffer, compat.string_types):
        try:
            exists = os.path.exists(filepath_or_buffer)
        except (TypeError, ValueError):
            exists = False

        if exists:
            with open(filepath_or_buffer, 'r') as fh:
                data = fh.read()
        else:
            data = filepath_or_buffer
    elif hasattr(filepath_or_buffer, 'read'):
        data = filepath_or_buffer.read()
    else:
        data = filepath_or_buffer

    return data
