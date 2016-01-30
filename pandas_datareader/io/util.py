from __future__ import unicode_literals

import os

import pandas.compat as compat
from pandas.io.common import get_filepath_or_buffer


def _read_content(path_or_buf):
    """ copied part of internal logic from pandas.io.read_json """
    results = get_filepath_or_buffer(path_or_buf)
    # results length is 3 in pandas 0.17 or later, 2 in 0.16.2 or prior
    filepath_or_buffer = results[0]
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
