import pandas as pd

from pandas_datareader.eurostat import EurostatReader


def test_api_urls():
    reader = EurostatReader(
        "ert_h_eur_a",
        start=pd.Timestamp("2009-01-01"),
        end=pd.Timestamp("2010-01-01"),
    )

    assert reader.url.startswith(
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    )
    assert reader.dsd_url.startswith(
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/datastructure/"
    )
