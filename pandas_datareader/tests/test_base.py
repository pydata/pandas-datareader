import datetime as dt

import pytest
import requests

from pandas_datareader import base as base

pytestmark = pytest.mark.stable


class TestBaseReader:
    def test_requests_not_monkey_patched(self):
        assert not hasattr(requests.Session(), "stor")

    def test_valid_retry_count(self):
        with pytest.raises(ValueError):
            base._BaseReader([], retry_count="stuff")
        with pytest.raises(ValueError):
            base._BaseReader([], retry_count=-1)

    def test_invalid_url(self):
        with pytest.raises(NotImplementedError):
            base._BaseReader([]).url

    def test_invalid_format(self):
        b = base._BaseReader([])
        b._format = "IM_NOT_AN_IMPLEMENTED_TYPE"
        with pytest.raises(NotImplementedError):
            b._read_one_data("a", None)

    def test_default_start_date(self):
        b = base._BaseReader([])
        assert b.default_start_date == dt.date.today() - dt.timedelta(days=365 * 5)

    def test_get_response_retries_request_exception(self, monkeypatch):
        class DummyResponse:
            status_code = requests.codes.ok
            encoding = "utf-8"
            text = ""

        reader = base._BaseReader([], retry_count=1, pause=0)
        calls = {"count": 0}

        def flaky_get(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise requests.exceptions.SSLError("temporary ssl failure")
            return DummyResponse()

        monkeypatch.setattr(reader.session, "get", flaky_get)

        response = reader._get_response("https://example.com")

        assert response.status_code == requests.codes.ok
        assert calls["count"] == 2


class TestDailyBaseReader:
    def test_get_params(self):
        b = base._DailyBaseReader()
        with pytest.raises(NotImplementedError):
            b._get_params()
