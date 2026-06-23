import os
import sys

from ._version import __version__
from .data import (
    DataReader,
    get_data_econdb,
    get_data_famafrench,
    get_data_fred,
)
from .macro import (
    EurostatClient,
    MacroResult,
    OECDClient,
    describe_macro_dataset,
    read_macro,
    search_macro_datasets,
)

PKG = os.path.dirname(__file__)

__all__ = [
    "__version__",
    "get_data_econdb",
    "get_data_famafrench",
    "get_data_fred",
    "DataReader",
    "MacroResult",
    "OECDClient",
    "EurostatClient",
    "read_macro",
    "search_macro_datasets",
    "describe_macro_dataset",
    "test",
]


def test(extra_args=None):
    """
    Run the test suite

    Parameters
    ----------
    extra_args : {str, List[str]}
        A string or list of strings to pass to pytest. Default is
        ["--only-stable", "--skip-requires-api-key"]
    """
    try:
        import pytest
    except ImportError as err:
        raise ImportError("Need pytest>=5.0.1 to run tests") from err
    cmd = ["--only-stable", "--skip-requires-api-key"]
    if extra_args:
        if not isinstance(extra_args, list):
            extra_args = [extra_args]
        cmd = extra_args
    cmd += [PKG]
    joined = " ".join(cmd)
    print(f"running: pytest {joined}")
    sys.exit(pytest.main(cmd))
