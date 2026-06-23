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


def deprecate_kwarg(old_arg_name, new_arg_name, mapping=None, stacklevel=2):
    """
    Decorator to deprecate a keyword argument of a function.

    Vendored from pandas.util._decorators to avoid relying on a private pandas
    API removed in pandas 3.0 (pandas-datareader issue #1005).

    Parameters
    ----------
    old_arg_name : str
        Name of argument to deprecate.
    new_arg_name : str or None
        Replacement argument name. Pass None to warn without renaming.
    mapping : dict or callable, optional
        Translate old argument values to new ones.
    stacklevel : int, default 2
        Passed to warnings.warn.
    """
    if mapping is not None and not hasattr(mapping, "get") and not callable(mapping):
        raise TypeError(
            "mapping from old to new argument values must be dict or callable!"
        )

    def _decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            old_arg_value = kwargs.pop(old_arg_name, None)

            if old_arg_value is not None:
                if new_arg_name is None:
                    msg = (
                        f"the {repr(old_arg_name)} keyword is deprecated and "
                        "will be removed in a future version. Please take "
                        f"steps to stop the use of {repr(old_arg_name)}"
                    )
                    warnings.warn(msg, FutureWarning, stacklevel=stacklevel)
                    kwargs[old_arg_name] = old_arg_value
                    return func(*args, **kwargs)
                elif mapping is not None:
                    if callable(mapping):
                        new_arg_value = mapping(old_arg_value)
                    else:
                        new_arg_value = mapping.get(old_arg_value, old_arg_value)
                    msg = (
                        f"the {old_arg_name}={repr(old_arg_value)} keyword is "
                        f"deprecated, use {new_arg_name}={repr(new_arg_value)} instead."
                    )
                else:
                    new_arg_value = old_arg_value
                    msg = (
                        f"the {repr(old_arg_name)} keyword is deprecated, "
                        f"use {repr(new_arg_name)} instead."
                    )

                warnings.warn(msg, FutureWarning, stacklevel=stacklevel)
                if kwargs.get(new_arg_name) is not None:
                    msg = (
                        f"Can only specify {repr(old_arg_name)} "
                        f"or {repr(new_arg_name)}, not both."
                    )
                    raise TypeError(msg)
                kwargs[new_arg_name] = new_arg_value
            return func(*args, **kwargs)

        return wrapper

    return _decorator


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


PYTHON_LT_3_10 = sys.version_info <= (3, 10)
