import math
import re

import pandas as pd
import requests

from pandas_datareader.base import _BaseReader
from pandas_datareader.io.sdmx import _read_sdmx_dsd, read_sdmx


class EurostatReader(_BaseReader):
    """Get data for the given name from Eurostat."""

    _URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"
    _STATISTICS_PREFERRED = {"prc_hicp_manr"}

    @property
    def url(self):
        """API URL"""
        if not isinstance(self.symbols, str):
            raise ValueError("data name must be string")

        q = "{0}/data/{1}?startPeriod={2}&endPeriod={3}"
        return q.format(self._URL, self.symbols, self.start.year, self.end.year)

    @property
    def dsd_url(self):
        """API DSD URL"""
        if not isinstance(self.symbols, str):
            raise ValueError("data name must be string")

        return f"{self._URL}/datastructure/ESTAT/{self.symbols}/latest?references=descendants"

    @property
    def statistics_url(self):
        if not isinstance(self.symbols, str):
            raise ValueError("data name must be string")

        return (
            "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
            f"{self.symbols}?format=JSON&startPeriod={self.start.year}&endPeriod={self.end.year}"
        )

    @staticmethod
    def _should_parse_datetime_index(index):
        if isinstance(index, pd.MultiIndex):
            return False
        for value in index:
            text = str(value)
            if not re.fullmatch(r"\d{4}(-\d{2})?(-\d{2})?", text):
                return False
        return True

    def _read_one_data(self, url, params):
        if self.symbols in self._STATISTICS_PREFERRED:
            payload = self._read_statistics_response()
            data = self._read_statistics_payload(payload)
            return self._format_columns(data)

        resp_dsd = self._get_response(self.dsd_url)
        dsd = _read_sdmx_dsd(resp_dsd.content)

        try:
            resp = self._get_response(url)
            data = read_sdmx(resp.content, dsd=dsd)
        except requests.exceptions.RequestException:
            payload = self._get_response(self.statistics_url).json()
            data = self._read_statistics_payload(payload)

        if self._should_parse_datetime_index(data.index):
            data.index = pd.to_datetime(data.index)
            data = data.sort_index()

        try:
            data = data.truncate(self.start, self.end)
        except TypeError:
            pass

        return self._format_columns(data)

    def _read_statistics_response(self):
        last_error = None
        for _ in range(self.retry_count + 1):
            try:
                return self._get_response(self.statistics_url).json()
            except requests.exceptions.RequestException as exc:
                last_error = exc
                continue
        raise last_error

    @staticmethod
    def _format_columns(data):
        if not isinstance(data.columns, pd.MultiIndex):
            return data

        columns = data.columns
        level_names = list(columns.names)

        priority = {
            "currency": 0,
            "indic_bt": 0,
            "statinfo": 1,
            "unit": 2,
            "siec": 3,
            "s_adj": 3,
            "tax": 4,
            "cpa2_1": 4,
            "nrg_cons": 5,
            "geo": 98,
            "freq": 99,
        }
        order = sorted(
            range(len(level_names)),
            key=lambda i: (priority.get(level_names[i].lower(), 50), i),
        )
        if order != list(range(len(level_names))):
            columns = columns.reorder_levels(order)
            level_names = [level_names[i] for i in order]

        columns = columns.set_names([name.upper() for name in level_names])
        data.columns = columns
        return data

    @staticmethod
    def _read_statistics_payload(payload):
        ids = payload["id"]
        sizes = payload["size"]
        dimensions = payload["dimension"]
        value_map = payload.get("value", {})

        labels_by_dim = {}
        for dim in ids:
            category = dimensions[dim]["category"]
            inverse = {position: code for code, position in category["index"].items()}
            labels_by_dim[dim] = [
                category["label"][inverse[i]] for i in range(len(inverse))
            ]

        time_dim = "time" if "time" in ids else "TIME_PERIOD"
        time_pos = ids.index(time_dim)
        column_dims = [dim for dim in ids if dim != time_dim]
        column_sizes = [sizes[ids.index(dim)] for dim in column_dims]

        row_labels = labels_by_dim[time_dim]
        rows = []
        for row_pos in range(sizes[time_pos]):
            row = []
            for flat_pos in range(math.prod(column_sizes) if column_sizes else 1):
                coords = []
                remaining = flat_pos
                for size in reversed(column_sizes):
                    coords.append(remaining % size)
                    remaining //= size
                coords = list(reversed(coords))

                full_coords = []
                col_iter = iter(coords)
                for idx, _dim in enumerate(ids):
                    if idx == time_pos:
                        full_coords.append(row_pos)
                    else:
                        full_coords.append(next(col_iter))

                stride = 1
                key = 0
                for coord, size in zip(
                    reversed(full_coords), reversed(sizes), strict=True
                ):
                    key += coord * stride
                    stride *= size
                row.append(value_map.get(str(key), pd.NA))
            rows.append(row)

        if column_dims:
            column_levels = [labels_by_dim[dim] for dim in column_dims]
            if len(column_levels) == 1:
                columns = pd.Index(column_levels[0], name=column_dims[0].upper())
            else:
                columns = pd.MultiIndex.from_product(
                    column_levels, names=[dim.upper() for dim in column_dims]
                )
        else:
            columns = pd.Index(["value"])

        try:
            index = pd.DatetimeIndex(row_labels, name="TIME_PERIOD")
        except ValueError:
            index = pd.Index(row_labels, name="TIME_PERIOD")

        return EurostatReader._format_columns(
            pd.DataFrame(rows, index=index, columns=columns)
        )
