# pylint: disable-msg=E1101,W0613,W0603

from __future__ import unicode_literals

from collections import OrderedDict
import itertools
import re
import sys

import numpy as np
import pandas as pd

from pandas_datareader.io.util import _read_content


def read_jsdmx(path_or_buf):
    """
    Convert a SDMX-JSON string to pandas object

    Parameters
    ----------
    path_or_buf : a valid SDMX-JSON string or file-like
        https://github.com/sdmx-twg/sdmx-json

    Returns
    -------
    results : Series, DataFrame, or dictionary of Series or DataFrame.
    """

    jdata = _read_content(path_or_buf)

    try:
        import simplejson as json
    except ImportError:
        if sys.version_info[:2] < (2, 7):
            raise ImportError("simplejson is required in python 2.6")
        import json

    if isinstance(jdata, dict):
        data = jdata
    else:
        data = json.loads(jdata, object_pairs_hook=OrderedDict)

    structure = data["structure"]
    index = _parse_dimensions(structure["dimensions"]["observation"])
    columns = _parse_dimensions(structure["dimensions"]["series"])

    dataset = data["dataSets"]
    if len(dataset) != 1:
        raise ValueError("length of 'dataSets' must be 1")
    dataset = dataset[0]
    values = _parse_values(dataset, index=index, columns=columns)

    df = pd.DataFrame(values, columns=columns, index=index)
    return df


def _get_indexer(index):
    if index.nlevels == 1:
        return [str(i) for i in range(len(index))]
    else:
        it = itertools.product(*[range(len(level)) for level in index.levels])
        return [":".join(map(str, i)) for i in it]


def _fix_quarter_values(value):
    """Make raw quarter values Pandas-friendly (e.g. 'Q4-2018' -> '2018Q4')."""
    m = re.match(r"Q([1-4])-(\d\d\d\d)", value)
    if not m:
        return value
    quarter, year = m.groups()
    value = "%sQ%s" % (quarter, year)
    return value


def _parse_values(dataset, index, columns):
    size = len(index)
    series = dataset["series"]

    values = []
    # for s_key, s_value in iteritems(series):
    for s_key in _get_indexer(columns):
        try:
            observations = series[s_key]["observations"]
            observed = []
            for o_key in _get_indexer(index):
                try:
                    observed.append(observations[o_key][0])
                except KeyError:
                    observed.append(np.nan)
        except KeyError:
            observed = [np.nan] * size

        values.append(observed)

    return np.transpose(np.array(values))


def _parse_dimensions(dimensions):
    arrays = []
    names = []
    for key in dimensions:
        values = [v["name"] for v in key["values"]]

        role = key.get("role", None)
        if role in ("time", "TIME_PERIOD"):
            values = [_fix_quarter_values(v) for v in values]
            values = pd.DatetimeIndex(values)

        arrays.append(values)
        names.append(key["name"])
    midx = pd.MultiIndex.from_product(arrays, names=names)
    if len(arrays) == 1 and isinstance(midx, pd.MultiIndex):
        # Fix for pandas >= 0.21
        midx = midx.levels[0]

    return midx
