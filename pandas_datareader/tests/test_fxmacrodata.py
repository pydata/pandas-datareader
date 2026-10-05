from datetime import date, timedelta
import json
from urllib.parse import parse_qs, urlparse
from urllib.request import HTTPRedirectHandler

from pandas import Timestamp
import pytest

from pandas_datareader import fxmacrodata
from pandas_datareader.fxmacrodata import FXMacroDataCalendarReader, FXMacroDataClient


def test_fxmacrodata_calendar_reader_parses_and_filters_events():
    reader = FXMacroDataCalendarReader("usd", start="2026-01-01", end="2026-12-31")
    payload = {
        "data": [
            {
                "date": "2026-07-08",
                "release": "nfp",
                "name": "Nonfarm Payrolls",
                "market_tier": 1,
                "announcement_datetime": 1783497600,
            },
            {
                "date": "2027-01-08",
                "release": "cpi",
                "name": "Consumer Price Index",
                "market_tier": 1,
                "announcement_datetime": 1799452800,
            },
        ]
    }

    frame = reader._parse_payload(payload)
    reader.close()

    assert list(frame["release"]) == ["nfp"]
    assert frame.index[0] == Timestamp("2026-07-08")
    assert str(frame["announcement_datetime"].dt.tz) == "UTC"


def test_fxmacrodata_client_flattens_catalogue_payload():
    payload = {
        "data": {
            "inflation": {"name": "Consumer Price Index", "frequency": "monthly"},
            "policy_rate": {"name": "Policy Rate", "frequency": "scheduled"},
        }
    }

    frame = FXMacroDataClient().to_dataframe(payload, index=False)

    assert list(frame["indicator"]) == ["inflation", "policy_rate"]


class _Response:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _rows(total):
    # Newest first, like the API.
    start = date(2024, 1, 1)
    return [
        {"date": (start + timedelta(days=i)).isoformat(), "val": float(i)}
        for i in reversed(range(total))
    ]


@pytest.fixture
def fake_api(monkeypatch):
    for name in fxmacrodata.FXMACRODATA_API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    requests = []
    rows = _rows(250)

    def urlopen(request, timeout=None):
        requests.append(request)
        query = parse_qs(urlparse(request.full_url).query)
        limit = int(query.get("limit", ["20"])[0])
        offset = int(query.get("offset", ["0"])[0])
        page = rows[offset : offset + limit]
        has_more = offset + len(page) < len(rows)
        return _Response(
            {
                "data": page,
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "returned_count": len(page),
                    "total_count": len(rows),
                    "has_more": has_more,
                    "next_offset": offset + len(page) if has_more else None,
                },
            }
        )

    monkeypatch.setattr(fxmacrodata, "urlopen", urlopen)
    return requests


def test_api_key_is_sent_as_header(fake_api):
    client = fxmacrodata.FXMacroDataClient(api_key="test-key")
    client.fetch_dataset("forex", base="eur", quote="usd", limit=5)
    request = fake_api[0]
    assert request.get_header("X-api-key") == "test-key"
    assert "test-key" not in request.full_url
    assert "api_key" not in request.full_url


def test_no_api_key_header_without_key(fake_api):
    client = fxmacrodata.FXMacroDataClient()
    client.fetch_dataset("announcements", currency="usd", indicator="inflation")
    request = fake_api[0]
    assert request.get_header("X-api-key") is None
    assert "api_key" not in request.full_url


def test_dataframe_pages_through_window(fake_api):
    client = fxmacrodata.FXMacroDataClient(api_key="test-key")
    frame = client.dataframe(
        "forex", base="eur", quote="usd", start_date="2024-01-01", end_date="2024-12-31"
    )
    assert len(frame) == 250
    assert frame.index.is_monotonic_increasing
    queries = [parse_qs(urlparse(r.full_url).query) for r in fake_api]
    assert [q["offset"] for q in queries] == [["0"], ["100"], ["200"]]
    assert all(q["limit"] == ["100"] for q in queries)
    assert all("api_key" not in r.full_url for r in fake_api)


def test_dataframe_limit_caps_rows_and_page_size(fake_api):
    client = fxmacrodata.FXMacroDataClient()
    frame = client.dataframe("cot", currency="eur", limit=150)
    assert len(frame) == 150
    queries = [parse_qs(urlparse(r.full_url).query) for r in fake_api]
    assert [(q["limit"], q["offset"]) for q in queries] == [
        (["100"], ["0"]),
        (["50"], ["100"]),
    ]
    assert frame.index.max().isoformat().startswith("2024-09-06")


def test_fetch_dataset_never_sends_limit_above_100(fake_api):
    client = fxmacrodata.FXMacroDataClient()
    client.fetch_dataset("commodity", indicator="gold", limit=500)
    assert parse_qs(urlparse(fake_api[0].full_url).query)["limit"] == ["100"]


def test_fxmacrodata_reader_reads_whole_window(fake_api):
    reader = fxmacrodata.FXMacroDataReader(
        "EURUSD", dataset="forex", start="2024-01-01", end="2024-12-31"
    )
    frame = reader.read()

    assert len(frame) == 250
    assert frame.index.is_monotonic_increasing
    query = parse_qs(urlparse(fake_api[0].full_url).query)
    assert query["start_date"] == ["2024-01-01"]
    assert query["end_date"] == ["2024-12-31"]


def test_api_key_is_not_forwarded_on_redirect(fake_api):
    client = fxmacrodata.FXMacroDataClient(api_key="test-key")
    client.fetch_dataset("forex", base="eur", quote="usd", limit=5)
    redirected = HTTPRedirectHandler().redirect_request(
        fake_api[0], None, 302, "Found", {}, "https://other.example/v1/forex"
    )
    assert redirected.get_header("X-api-key") is None


def test_api_key_is_stripped_and_rejected_without_echo(fake_api):
    client = fxmacrodata.FXMacroDataClient(api_key="  test-key\n")
    client.fetch_dataset("forex", base="eur", quote="usd", limit=5)
    assert fake_api[0].get_header("X-api-key") == "test-key"

    with pytest.raises(ValueError) as excinfo:
        fxmacrodata.FXMacroDataClient(api_key="test-key\r\nX-Other: 1")
    assert "test-key" not in str(excinfo.value)
