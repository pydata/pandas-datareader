import pandas as pd
from pandas.core.common import PandasError


class SymbolWarning(UserWarning):
    pass

class RemoteDataError(PandasError, IOError):
    pass


from distutils.version import LooseVersion

PANDAS_VERSION = LooseVersion(pd.__version__)

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