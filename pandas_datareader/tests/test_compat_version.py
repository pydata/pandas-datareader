import pytest

from pandas_datareader.compat import LooseVersion


def test_loose_version_comparison():
    assert LooseVersion("1.0") < LooseVersion("2.0")
    assert LooseVersion("2.0") > LooseVersion("1.0")

