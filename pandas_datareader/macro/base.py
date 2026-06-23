from __future__ import annotations

import pandas as pd

from pandas_datareader._utils import _init_session


class MacroDataError(Exception):
    pass


class MacroProviderError(MacroDataError):
    pass


class MacroSchemaError(MacroDataError):
    pass


class MacroNotFoundError(MacroDataError):
    pass


class MacroClientBase:
    provider = ""

    def __init__(self, session=None):
        self.session = _init_session(session)

    @staticmethod
    def _normalize_date(value):
        if value is None:
            return None
        return pd.to_datetime(value).strftime("%Y-%m-%d")

    def _get(self, url, **kwargs):
        response = self.session.get(url, timeout=30, **kwargs)
        if response.status_code == 404:
            raise MacroNotFoundError(f"Resource not found: {url}")
        if response.status_code >= 400:
            raise MacroProviderError(
                f"Provider request failed: {url} ({response.status_code})"
            )
        return response

    def search_datasets(self, query=None, **kwargs):
        raise NotImplementedError

    def describe_dataset(self, dataset, **kwargs):
        raise NotImplementedError

    def read(self, dataset, **kwargs):
        raise NotImplementedError
