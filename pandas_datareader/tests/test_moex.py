import pytest
from requests.exceptions import HTTPError

import pandas_datareader.data as web

pytestmark = pytest.mark.stable


class TestMoex(object):
    def test_moex_datareader(self):
        try:
            df = web.DataReader(
                "USD000UTSTOM", "moex", start="2017-07-01", end="2017-07-31"
            )
            assert "SECID" in df.columns
        except HTTPError as e:
            pytest.skip(e)

    def test_moex_stock_datareader(self):
        try:
            df = web.DataReader(
                ["GAZP", "SIBN"], "moex", start="2019-12-26", end="2019-12-26"
            )
            assert df.size == 740
        except HTTPError as e:
            pytest.skip(e)
