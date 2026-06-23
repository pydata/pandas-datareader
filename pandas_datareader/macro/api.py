from pandas_datareader.macro.eurostat import EurostatClient
from pandas_datareader.macro.oecd import OECDClient


def _get_client(provider):
    if provider == "oecd":
        return OECDClient()
    if provider == "eurostat":
        return EurostatClient()
    raise ValueError(f"Unknown provider: {provider}")


def read_macro(provider, dataset, **kwargs):
    return _get_client(provider).read(dataset, **kwargs)


def search_macro_datasets(provider, query=None, **kwargs):
    return _get_client(provider).search_datasets(query=query, **kwargs)


def describe_macro_dataset(provider, dataset, **kwargs):
    return _get_client(provider).describe_dataset(dataset, **kwargs)
