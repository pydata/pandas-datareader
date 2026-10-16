from unittest import mock

import pandas as pd

from pandas_datareader.econdb import EcondbReader


def test_read_returns_datetime_index_without_network():
    payload = {
        "results": [
            {
                "ticker": "CPIUS",
                "additional_metadata": {"GEO": "1:United States"},
                "data": {
                    "dates": ["2020-01-01", "2020-02-01", "2020-03-01"],
                    "values": [1.0, 2.0, 3.0],
                },
            }
        ]
    }
    reader = EcondbReader(
        "ticker=CPIUS", start="2019-01-01", end="2020-02-15", api_key="x"
    )
    with mock.patch.object(reader.session, "get") as get:
        get.return_value.json.return_value = payload
        df = reader.read()
    assert isinstance(df.index, pd.DatetimeIndex)
    assert list(df.iloc[:, 0]) == [1.0, 2.0]
