# flake8: noqa
import pandas as pd

from io import BytesIO
from distutils.version import LooseVersion

pandas_version = LooseVersion(pd.__version__)
if pandas_version <= '0.19.2':
    from pandas.core.common import is_number
else:
    from pandas.api.types import is_number
