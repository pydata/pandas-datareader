import warnings

from pandas.core.common import PandasError


class SymbolWarning(UserWarning):
    pass

class RemoteDataError(PandasError, IOError):
    pass


