# Macro Modern API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `pandas_datareader.macro` submodule for current OECD and Eurostat APIs, with unified top-level entry points and provider-specific clients.

**Architecture:** The implementation adds a new macro package rather than modifying legacy reader contracts. A thin public API routes to `OECDClient` and `EurostatClient`, both of which return a typed `MacroResult` carrying `DataFrame` data plus metadata. Provider-specific protocol parsing stays isolated in provider modules.

**Tech Stack:** Python 3.9+, pandas, requests, dataclasses, pytest

---

## File Map

**Create**

- `pandas_datareader/macro/__init__.py`
- `pandas_datareader/macro/api.py`
- `pandas_datareader/macro/base.py`
- `pandas_datareader/macro/result.py`
- `pandas_datareader/macro/oecd.py`
- `pandas_datareader/macro/eurostat.py`
- `pandas_datareader/tests/macro/test_api.py`
- `pandas_datareader/tests/macro/test_oecd.py`
- `pandas_datareader/tests/macro/test_eurostat.py`

**Modify**

- `pandas_datareader/__init__.py`
- `pandas_datareader/io/jsdmx.py`
- `pandas_datareader/io/sdmx.py`
- `pyproject.toml`

### Task 1: Result Object And Public API

**Files:**
- Create: `pandas_datareader/macro/result.py`
- Create: `pandas_datareader/macro/api.py`
- Create: `pandas_datareader/macro/__init__.py`
- Create: `pandas_datareader/tests/macro/test_api.py`
- Modify: `pandas_datareader/__init__.py`

- [ ] **Step 1: Write the failing tests for the result object and API routing**

```python
from pandas import DataFrame

from pandas_datareader.macro import MacroResult, read_macro


def test_macro_result_fields():
    result = MacroResult(
        data=DataFrame({"value": [1]}),
        metadata={"title": "x"},
        provider="oecd",
        dataset_id="TUD",
    )

    assert result.provider == "oecd"
    assert result.dataset_id == "TUD"
    assert list(result.data.columns) == ["value"]


def test_read_macro_rejects_unknown_provider():
    try:
        read_macro(provider="unknown", dataset="x")
    except ValueError as err:
        assert "unknown provider" in str(err).lower()
    else:
        raise AssertionError("expected ValueError")
```

- [ ] **Step 2: Run the API tests to verify they fail**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_api.py
```

Expected: FAIL because `pandas_datareader.macro` does not exist yet.

- [ ] **Step 3: Implement the minimal result object and routing shell**

```python
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class MacroResult:
    data: pd.DataFrame
    metadata: dict[str, Any]
    provider: str
    dataset_id: str
```

```python
def read_macro(provider, dataset, **kwargs):
    raise ValueError(f"Unknown provider: {provider}")
```

- [ ] **Step 4: Verify the tests pass after wiring exports**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_api.py
```

Expected: PASS

### Task 2: Base Client And Shared Exceptions

**Files:**
- Create: `pandas_datareader/macro/base.py`
- Modify: `pandas_datareader/macro/api.py`
- Create: `pandas_datareader/tests/macro/test_api.py`

- [ ] **Step 1: Add failing tests for base-level date normalization and provider registry**

```python
from pandas_datareader.macro.api import _get_client


def test_get_client_returns_provider_client():
    client = _get_client("oecd")
    assert client.__class__.__name__ == "OECDClient"
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_api.py -k get_client
```

Expected: FAIL because registry/client classes are not implemented.

- [ ] **Step 3: Implement `MacroClientBase` and exception types**

```python
class MacroDataError(Exception):
    pass


class MacroProviderError(MacroDataError):
    pass


class MacroSchemaError(MacroDataError):
    pass


class MacroNotFoundError(MacroDataError):
    pass
```

- [ ] **Step 4: Wire a minimal provider registry**

```python
def _get_client(provider):
    if provider == "oecd":
        return OECDClient()
    if provider == "eurostat":
        return EurostatClient()
    raise ValueError(f"Unknown provider: {provider}")
```

- [ ] **Step 5: Re-run the focused API tests**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_api.py
```

Expected: PASS

### Task 3: OECD Discovery, Describe, And Read

**Files:**
- Create: `pandas_datareader/macro/oecd.py`
- Create: `pandas_datareader/tests/macro/test_oecd.py`
- Modify: `pandas_datareader/io/jsdmx.py`

- [ ] **Step 1: Write failing offline OECD tests using trimmed SDMX-JSON v2 fixtures**

```python
def test_oecd_read_parses_sdmx_json_v2_fixture():
    client = OECDClient()
    result = client._build_result_from_payload("TUD", payload)
    assert result.provider == "oecd"
    assert result.dataset_id == "TUD"
    assert result.data.index.name == "Time period"
```

- [ ] **Step 2: Run the OECD offline test to verify it fails**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_oecd.py
```

Expected: FAIL because `OECDClient` is not implemented.

- [ ] **Step 3: Implement OECD client methods**

Required methods:

```python
class OECDClient(MacroClientBase):
    def search_datasets(self, query=None, **kwargs): ...
    def describe_dataset(self, dataset, **kwargs): ...
    def read(self, dataset, start=None, end=None, filters=None, **kwargs): ...
```

- [ ] **Step 4: Reuse the updated `read_jsdmx()` parser rather than duplicating SDMX-JSON flattening**

Implementation rule:

```text
Use `read_jsdmx` for tabular conversion.
Build `metadata["dimensions"]` from the live/fixture `structures` object.
```

- [ ] **Step 5: Verify offline OECD tests pass**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_oecd.py
```

Expected: PASS

### Task 4: Eurostat Discovery, Describe, And Read

**Files:**
- Create: `pandas_datareader/macro/eurostat.py`
- Create: `pandas_datareader/tests/macro/test_eurostat.py`
- Modify: `pandas_datareader/io/sdmx.py`

- [ ] **Step 1: Write failing offline Eurostat tests**

```python
def test_eurostat_read_uses_sdmx_21_fixture():
    client = EurostatClient()
    result = client._build_result_from_payload("ert_h_eur_a", xml_bytes, dsd_bytes)
    assert result.provider == "eurostat"
    assert result.data.index.name == "TIME_PERIOD"
    assert "dimensions" in result.metadata
```

- [ ] **Step 2: Run the Eurostat offline tests to verify they fail**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_eurostat.py
```

Expected: FAIL because `EurostatClient` is not implemented.

- [ ] **Step 3: Implement the Eurostat client around SDMX 2.1**

Required methods:

```python
class EurostatClient(MacroClientBase):
    def search_datasets(self, query=None, **kwargs): ...
    def describe_dataset(self, dataset, **kwargs): ...
    def read(self, dataset, start=None, end=None, filters=None, **kwargs): ...
```

- [ ] **Step 4: Keep observation attributes in metadata rather than mixing them into the values table**

Implementation rule:

```text
`result.data` contains only observations.
Observation-level flags and related metadata belong under `result.metadata`.
```

- [ ] **Step 5: Verify offline Eurostat tests pass**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_eurostat.py
```

Expected: PASS

### Task 5: Unified Entry Points And Top-Level Exports

**Files:**
- Modify: `pandas_datareader/macro/api.py`
- Modify: `pandas_datareader/macro/__init__.py`
- Modify: `pandas_datareader/__init__.py`
- Create: `pandas_datareader/tests/macro/test_api.py`

- [ ] **Step 1: Add failing end-to-end routing tests**

```python
def test_read_macro_routes_to_oecd_client(monkeypatch):
    called = {}

    class DummyClient:
        def read(self, dataset, **kwargs):
            called["dataset"] = dataset
            return MacroResult(
                data=DataFrame({"value": [1]}),
                metadata={},
                provider="oecd",
                dataset_id=dataset,
            )

    monkeypatch.setattr("pandas_datareader.macro.api.OECDClient", DummyClient)
    result = read_macro("oecd", "TUD")
    assert called["dataset"] == "TUD"
    assert result.provider == "oecd"
```

- [ ] **Step 2: Run the routing tests to verify behavior before final wiring**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_api.py
```

Expected: PASS after routing is complete.

### Task 6: Validation

**Files:**
- No new files

- [ ] **Step 1: Run style checks**

Run:

```bash
black --check pandas_datareader tests conftest.py
isort --check pandas_datareader tests conftest.py
flake8 pandas_datareader tests
```

Expected: PASS

- [ ] **Step 2: Run offline macro tests**

Run:

```bash
python -m pytest -q pandas_datareader/tests/macro/test_api.py pandas_datareader/tests/macro/test_oecd.py pandas_datareader/tests/macro/test_eurostat.py
```

Expected: PASS

- [ ] **Step 3: Run lightweight live macro smoke tests**

Run:

```bash
python - <<'PY'
from pandas_datareader.macro import read_macro, search_macro_datasets, describe_macro_dataset

res = read_macro("oecd", "TUD", start="2010-01-01", end="2012-01-01")
print(res.provider, res.dataset_id, res.data.shape)

res2 = read_macro("eurostat", "ert_h_eur_a", start="2009-01-01", end="2010-01-01")
print(res2.provider, res2.dataset_id, res2.data.shape)

print(search_macro_datasets("oecd", query="tourism").head(3))
print(describe_macro_dataset("eurostat", "ert_h_eur_a").keys())
PY
```

Expected:

```text
- OECD read returns `MacroResult`
- Eurostat read returns `MacroResult`
- search returns non-empty DataFrame
- describe returns dimension metadata
```
