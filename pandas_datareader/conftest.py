import os

import pytest


def pytest_addoption(parser):
    parser.addoption("--only-stable", action="store_true", help="run only stable tests")
    parser.addoption(
        "--skip-requires-api-key",
        action="store_true",
        help="skip tests that require an API key",
    )
    parser.addoption(
        "--strict-data-files",
        action="store_true",
        help="Fail if a test is skipped for missing data file.",
    )


def pytest_runtest_setup(item):
    if "stable" not in item.keywords and item.config.getoption("--only-stable"):
        pytest.skip("skipping due to --only-stable")

    if "requires_api_key" in item.keywords and item.config.getoption(
        "--skip-requires-api-key"
    ):
        pytest.skip("skipping due to --skip-requires-api-key")


@pytest.fixture
def datapath(request):
    """Get the path to a data file.

    Parameters
    ----------
    path : str
        Path to the file, relative to ``pandas/tests/``

    Returns
    -------
    path : path including ``pandas/tests``.

    Raises
    ------
    ValueError
        If the path doesn't exist and the --strict-data-files option is set.
    """
    BASE_PATH = os.path.join(os.path.dirname(__file__), "tests")

    def deco(*args):
        path = os.path.join(BASE_PATH, *args)
        if not os.path.exists(path):
            if request.config.getoption("--strict-data-files"):
                msg = "Could not find file {} and --strict-data-files is set."
                raise ValueError(msg.format(path))
            else:
                msg = "Could not find {}."
                pytest.skip(msg.format(path))
        return path

    return deco
