from ._version import get_versions
from .data import (get_components_yahoo, get_data_famafrench, get_data_google,
                   get_data_yahoo, get_data_enigma, get_data_yahoo_actions,
                   get_quote_google, get_quote_yahoo, DataReader, Options)

__version__ = get_versions()['version']
del get_versions

__all__ = ['__version__', 'get_components_yahoo', 'get_data_enigma',
           'get_data_famafrench', 'get_data_google', 'get_data_yahoo',
           'get_data_yahoo_actions', 'get_quote_google', 'get_quote_yahoo',
           'DataReader', 'Options']
