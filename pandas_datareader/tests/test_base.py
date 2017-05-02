import pytest
import requests

import pandas_datareader.base as base


class TestBaseReader(object):
    def test_requests_not_monkey_patched(self):
        assert not hasattr(requests.Session(), 'stor')

    def test_valid_retry_count(self):
        with pytest.raises(ValueError):
            base._BaseReader([], retry_count='stuff')
        with pytest.raises(ValueError):
            base._BaseReader([], retry_count=-1)

    def test_invalid_url(self):
        with pytest.raises(NotImplementedError):
            base._BaseReader([]).url

    def test_invalid_format(self):
        with pytest.raises(NotImplementedError):
            b = base._BaseReader([])
            b._format = 'IM_NOT_AN_IMPLEMENTED_TYPE'
            b._read_one_data('a', None)


class TestDailyBaseReader(object):
    def test_get_params(self):
        with pytest.raises(NotImplementedError):
            b = base._DailyBaseReader()
            b._get_params()
