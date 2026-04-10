import pandas as pd
import pytest

from pandas_datareader.macro import (
    MacroResult,
    describe_macro_dataset,
    read_macro,
    search_macro_datasets,
)


def test_macro_result_fields():
    result = MacroResult(
        data=pd.DataFrame({"value": [1]}),
        metadata={"title": "Example"},
        provider="oecd",
        dataset_id="OECD.ELS.SAE,DSD_TUD_CBC@DF_CBC,1.0",
    )

    assert result.provider == "oecd"
    assert result.dataset_id == "OECD.ELS.SAE,DSD_TUD_CBC@DF_CBC,1.0"
    assert list(result.data.columns) == ["value"]
    assert result.metadata["title"] == "Example"


def test_read_macro_rejects_unknown_provider():
    with pytest.raises(ValueError, match="Unknown provider"):
        read_macro(provider="unknown", dataset="x")


def test_read_macro_routes_to_provider(monkeypatch):
    called = {}

    class DummyClient:
        def read(self, dataset, **kwargs):
            called["dataset"] = dataset
            called["kwargs"] = kwargs
            return MacroResult(
                data=pd.DataFrame({"value": [1]}),
                metadata={},
                provider="oecd",
                dataset_id=dataset,
            )

    monkeypatch.setattr(
        "pandas_datareader.macro.api._get_client", lambda provider: DummyClient()
    )
    result = read_macro("oecd", "dataset-id", start="2020-01-01")

    assert called["dataset"] == "dataset-id"
    assert called["kwargs"]["start"] == "2020-01-01"
    assert result.provider == "oecd"


def test_search_macro_datasets_routes_to_provider(monkeypatch):
    expected = pd.DataFrame([{"dataset_id": "x"}])

    class DummyClient:
        def search_datasets(self, query=None, **kwargs):
            assert query == "tourism"
            return expected

    monkeypatch.setattr(
        "pandas_datareader.macro.api._get_client", lambda provider: DummyClient()
    )

    result = search_macro_datasets("oecd", query="tourism")

    pd.testing.assert_frame_equal(result, expected)


def test_describe_macro_dataset_routes_to_provider(monkeypatch):
    expected = {"dataset_id": "x", "title": "Example"}

    class DummyClient:
        def describe_dataset(self, dataset, **kwargs):
            assert dataset == "x"
            return expected

    monkeypatch.setattr(
        "pandas_datareader.macro.api._get_client", lambda provider: DummyClient()
    )

    result = describe_macro_dataset("eurostat", "x")

    assert result == expected
