# flake8: noqa
import sys
from distutils.version import LooseVersion
from io import BytesIO

import pandas as pd
import pandas.compat as compat
import pandas.io.common as com

PY3 = sys.version_info >= (3, 0)

PANDAS_VERSION = LooseVersion(pd.__version__)

PANDAS_0190 = (PANDAS_VERSION >= LooseVersion('0.19.0'))
PANDAS_0200 = (PANDAS_VERSION >= LooseVersion('0.20.0'))
PANDAS_0210 = (PANDAS_VERSION >= LooseVersion('0.21.0'))

if PANDAS_0190:
    from pandas.api.types import is_number
else:
    from pandas.core.common import is_number

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

if compat.PY3:
    from urllib.error import HTTPError
else:
    from urllib2 import HTTPError
