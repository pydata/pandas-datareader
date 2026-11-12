from io import StringIO
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest

from pandas_datareader._utils import RemoteDataError
from pandas_datareader.fred import FredReader


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("This test must not make HTTP requests")

    monkeypatch.setattr("requests.sessions.Session.request", fail)


def _generator(values):
    yield from values


@pytest.mark.parametrize("names", [["GDP"], ["GDP", "CPI"]])
@pytest.mark.parametrize(
    "factory",
    [list, tuple, iter, _generator, pd.Index, pd.Series, np.array],
    ids=["list", "tuple", "iterator", "generator", "index", "series", "array"],
)
def test_fred_symbol_containers(factory, names, monkeypatch):
    responses = {
        "GDP": "DATE,GDP\n2019-12-31,99\n2020-01-01,100\n2020-01-03,.\n2020-01-04,400\n",
        "CPI": "DATE,CPI\n2020-01-02,2\n2020-01-03,3\n",
    }
    reader = FredReader(factory(names), "2020-01-01", "2020-01-03")
    requested = []

    def fetch(url):
        requested.append(url)
        name = url.rsplit("=", 1)[1]
        return StringIO(responses[name])

    close = Mock(wraps=reader.session.close)
    monkeypatch.setattr(reader, "_read_url_as_StringIO", fetch)
    monkeypatch.setattr(reader.session, "close", close)
    actual = reader.read()

    dates = ["2020-01-01", "2020-01-03"]
    values = {"GDP": [100.0, np.nan]}
    if len(names) == 2:
        dates = ["2020-01-01", "2020-01-02", "2020-01-03"]
        values = {"GDP": [100.0, np.nan, np.nan], "CPI": [np.nan, 2.0, 3.0]}
    index = pd.DatetimeIndex(dates, freq="D" if len(names) == 2 else None)
    expected = pd.DataFrame(values, index=index)
    expected.index.name = "DATE"
    pd.testing.assert_frame_equal(actual, expected)
    assert requested == [f"{reader.url}?id={name}" for name in names]
    close.assert_called_once_with()


def test_fred_scalar_symbol_is_not_split(monkeypatch):
    reader = FredReader("GDP", "2020-01-01", "2020-01-03")
    fetch = Mock(return_value=StringIO("DATE,GDP\n2020-01-01,100\n"))
    monkeypatch.setattr(reader, "_read_url_as_StringIO", fetch)
    result = reader.read()
    assert list(result.columns) == ["GDP"]
    fetch.assert_called_once_with(f"{reader.url}?id=GDP")


def test_fred_iterator_closes_session_on_read_error(monkeypatch):
    reader = FredReader(iter(["GDP"]), "2020-01-01", "2020-01-03")
    fetch = Mock(side_effect=RemoteDataError("synthetic provider failure"))
    close = Mock(wraps=reader.session.close)
    monkeypatch.setattr(reader, "_read_url_as_StringIO", fetch)
    monkeypatch.setattr(reader.session, "close", close)
    with pytest.raises(RemoteDataError, match="synthetic provider failure"):
        reader.read()
    fetch.assert_called_once_with(f"{reader.url}?id=GDP")
    close.assert_called_once_with()
