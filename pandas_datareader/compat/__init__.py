# flake8: noqa
import pandas as pd
import pandas.compat as compat

from io import BytesIO
from distutils.version import LooseVersion

PANDAS_VERSION = LooseVersion(pd.__version__)

PANDAS_0190 = (PANDAS_VERSION >= LooseVersion('0.19.0'))

if PANDAS_0190:
    from pandas.api.types import is_number
else:
    from pandas.core.common import is_number

if compat.PY3:
    from urllib.error import HTTPError
else:
    from urllib2 import HTTPError
