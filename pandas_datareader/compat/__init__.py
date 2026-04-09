from functools import wraps
from io import StringIO
import sys
from urllib.error import HTTPError
import warnings

from pandas.api.types import is_list_like, is_number
from pandas.io import common as com
from pandas.testing import assert_frame_equal

__all__ = [
    "HTTPError",
    "StringIO",
    "deprecate_kwarg",
    "get_filepath_or_buffer",
    "assert_frame_equal",
    "is_list_like",
    "is_number",
    "PYTHON_LT_3_10",
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


def deprecate_kwarg(old_arg_name, new_arg_name):
    """
    Decorator to deprecate a keyword argument of a function.

    Emits a FutureWarning when ``old_arg_name`` is passed and forwards the
    value to ``new_arg_name``.  This is a self-contained replacement for the
    private ``pandas.util._decorators.deprecate_kwarg`` helper whose signature
    changed in pandas 2.3, making it incompatible with the call-site in
    ``pandas_datareader.data``.

    Parameters
    ----------
    old_arg_name : str
        Name of the argument to deprecate.
    new_arg_name : str
        Name of the preferred replacement argument.
    """

    def _decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if old_arg_name in kwargs:
                old_arg_value = kwargs.pop(old_arg_name)
                msg = (
                    f"the {old_arg_name!r} keyword is deprecated, "
                    f"use {new_arg_name!r} instead."
                )
                warnings.warn(msg, FutureWarning, stacklevel=2)
                if kwargs.get(new_arg_name) is not None:
                    raise TypeError(
                        f"Can only specify {old_arg_name!r} or {new_arg_name!r}, "
                        "not both."
                    )
                kwargs[new_arg_name] = old_arg_value
            return func(*args, **kwargs)

        return wrapper

    return _decorator


PYTHON_LT_3_10 = sys.version_info <= (3, 10)
