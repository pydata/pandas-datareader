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


def test_parse_statistics_payload():
    payload = {
        "id": ["geo", "time"],
        "size": [2, 2],
        "dimension": {
            "geo": {
                "label": "Geopolitical entity",
                "category": {
                    "index": {"DE": 0, "FR": 1},
                    "label": {"DE": "Germany", "FR": "France"},
                },
            },
            "time": {
                "label": "Time period",
                "category": {
                    "index": {"2020": 0, "2021": 1},
                    "label": {"2020": "2020", "2021": "2021"},
                },
            },
        },
        "value": {"0": 1.0, "1": 2.0, "2": 3.0, "3": 4.0},
    }

    result = EurostatReader._read_statistics_payload(payload)

    expected = pd.DataFrame(
        [[1.0, 3.0], [2.0, 4.0]],
        index=pd.DatetimeIndex(["2020-01-01", "2021-01-01"], name="TIME_PERIOD"),
        columns=pd.Index(["Germany", "France"], name="GEO"),
    )

    pd.testing.assert_frame_equal(result, expected)
