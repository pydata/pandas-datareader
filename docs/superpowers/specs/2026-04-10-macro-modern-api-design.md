# Macro Modern API Design

**Goal**

Add a new macro-data API that targets current OECD and Eurostat interfaces directly, without preserving the legacy `DataReader(..., "oecd"|"eurostat")` contract.

**Scope**

This design adds a dedicated `pandas_datareader.macro` submodule with:

- a unified top-level API
- provider-specific clients
- a structured result object carrying both `DataFrame` data and metadata
- support for dataset discovery, metadata inspection, and data reads

It does not attempt backward compatibility with the legacy OECD or Eurostat readers.

## Public API

Top-level functions:

- `read_macro(provider, dataset, *, start=None, end=None, filters=None, session=None, **kwargs) -> MacroResult`
- `search_macro_datasets(provider, query=None, *, session=None, **kwargs) -> pd.DataFrame`
- `describe_macro_dataset(provider, dataset, *, session=None, **kwargs) -> dict`

Provider-specific clients:

- `OECDClient`
- `EurostatClient`

## Object Model

`MacroResult` is the standard return type for all macro data reads.

Required fields:

- `data: pd.DataFrame`
- `metadata: dict`
- `provider: str`
- `dataset_id: str`

Required `metadata` keys:

- `title`
- `description`
- `updated_at`
- `dimensions`
- `attributes`
- `labels`
- `source`
- `query`
- `notes`
- `raw`

`raw` stores a compact summary of the provider response rather than the entire payload.

## Module Layout

- `pandas_datareader/macro/__init__.py`
- `pandas_datareader/macro/api.py`
- `pandas_datareader/macro/result.py`
- `pandas_datareader/macro/base.py`
- `pandas_datareader/macro/oecd.py`
- `pandas_datareader/macro/eurostat.py`

Tests:

- `pandas_datareader/tests/macro/test_api.py`
- `pandas_datareader/tests/macro/test_oecd.py`
- `pandas_datareader/tests/macro/test_eurostat.py`

## Responsibilities

### `macro/api.py`

- provider registry
- provider validation
- routing to the correct client
- returning `MacroResult`

No provider-specific parsing logic belongs here.

### `macro/result.py`

- `MacroResult` dataclass

### `macro/base.py`

- common client base
- session management
- standardized date normalization
- query normalization
- shared exception types

### `macro/oecd.py`

- discovery using current OECD SDMX endpoints
- metadata describe using OECD structure responses
- data reads using current SDMX-JSON v2 payloads
- flattening series dimensions into column MultiIndex
- returning current official labels, not legacy aliases

### `macro/eurostat.py`

- discovery using Eurostat SDMX 2.1 dataflow endpoints
- metadata describe using Eurostat DSD + codelist responses
- data reads using Eurostat SDMX 2.1 data endpoints
- DSD-backed code-to-label mapping
- observation attributes represented in metadata rather than mixed into value columns

## Data Shape Rules

The new macro API is defined around current provider semantics.

- observation/time dimensions become the row index
- series dimensions become a column `MultiIndex`
- labels are taken from current provider metadata
- no attempt is made to remap provider labels to legacy test fixtures

## Error Model

New exceptions:

- `MacroDataError`
- `MacroProviderError`
- `MacroSchemaError`
- `MacroNotFoundError`

Mapping:

- provider 404 or unknown dataset -> `MacroNotFoundError`
- unexpected live schema change -> `MacroSchemaError`
- request/timeout/provider operational failure -> `MacroProviderError`

## Testing Strategy

Three levels:

1. Offline parser tests with trimmed OECD and Eurostat fixtures
2. Lightweight online smoke tests for discovery, describe, and read
3. Result-object tests to ensure `MacroResult` shape is stable

The new tests do not pin large historical value matrices that are likely to drift with provider changes.

## Non-Goals

- preserving legacy OECD/Eurostat `DataReader` output
- legacy label compatibility layers
- provider-specific historical baselines rewritten to current semantics
- a general provider plugin system

## Recommended First Release

First release should expose:

- `read_macro`
- `search_macro_datasets`
- `describe_macro_dataset`
- `OECDClient`
- `EurostatClient`
- `MacroResult`

That is enough to prove the new architecture and support downstream exploration without inheriting legacy API debt.
