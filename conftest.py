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
    base_path = os.path.join(os.path.dirname(__file__), "pandas_datareader", "tests")

    def deco(*args):
        path = os.path.join(base_path, *args)
        if not os.path.exists(path):
            if request.config.getoption("--strict-data-files"):
                msg = "Could not find file {} and --strict-data-files is set."
                raise ValueError(msg.format(path))
            msg = "Could not find {}."
            pytest.skip(msg.format(path))
        return path

    return deco
