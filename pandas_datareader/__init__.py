import os
import sys

from ._version import get_versions
from .data import (
    DataReader,
    Options,
    get_components_yahoo,
    get_dailysummary_iex,
    get_data_alphavantage,
    get_data_enigma,
    get_data_famafrench,
    get_data_fred,
    get_data_moex,
    get_data_quandl,
    get_data_stooq,
    get_data_tiingo,
    get_data_yahoo,
    get_data_yahoo_actions,
    get_iex_book,
    get_iex_data_tiingo,
    get_iex_symbols,
    get_last_iex,
    get_markets_iex,
    get_nasdaq_symbols,
    get_quote_yahoo,
    get_recent_iex,
    get_records_iex,
    get_summary_iex,
    get_tops_iex,
)

PKG = os.path.dirname(__file__)

__version__ = get_versions()["version"]
del get_versions

__all__ = [
    "__version__",
    "get_components_yahoo",
    "get_data_enigma",
    "get_data_famafrench",
    "get_data_yahoo",
    "get_data_yahoo_actions",
    "get_quote_yahoo",
    "get_iex_book",
    "get_iex_symbols",
    "get_last_iex",
    "get_markets_iex",
    "get_recent_iex",
    "get_records_iex",
    "get_summary_iex",
    "get_tops_iex",
    "get_nasdaq_symbols",
    "get_data_quandl",
    "get_data_moex",
    "get_data_fred",
    "get_dailysummary_iex",
    "get_data_stooq",
    "DataReader",
    "Options",
    "get_data_tiingo",
    "get_iex_data_tiingo",
    "get_data_alphavantage",
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
