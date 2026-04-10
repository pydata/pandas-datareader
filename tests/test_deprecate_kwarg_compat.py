import pytest

from pandas_datareader.data import DataReader


def test_access_key_delegates_to_api_key(monkeypatch):
    captured = {}

    class DummyReader:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        def read(self):
            return "ok"

    monkeypatch.setattr("pandas_datareader.data.IEXDailyReader", DummyReader)

    with pytest.warns(FutureWarning, match="access_key"):
        result = DataReader("GS10", "iex", access_key="secret")

    assert result == "ok"
    assert captured["api_key"] == "secret"
