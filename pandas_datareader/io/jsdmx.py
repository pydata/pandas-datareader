# pylint: disable-msg=E1101,W0613,W0603

from __future__ import unicode_literals

import itertools
import sys

import numpy as np
import pandas as pd
import pandas.compat as compat

from pandas_datareader.io.util import _read_content


def read_jsdmx(path_or_buf):
    """
    Convert a SDMX-JSON string to pandas object

    Parameters
    ----------
    path_or_buf : a valid SDMX-JSON string or file-like
        http://sdmx.org/wp-content/uploads/2014/07/sdmx-json-data-message.pdf

    Returns
    -------
    results : Series, DataFrame, or dictionary of Series or DataFrame.
    """

    jdata = _read_content(path_or_buf)

    try:
        import simplejson as json
    except ImportError:
        if sys.version_info[:2] < (2, 7):
            raise ImportError('simplejson is required in python 2.6')
        import json

    if isinstance(jdata, dict):
        data = jdata
    else:
        data = json.loads(jdata, object_pairs_hook=compat.OrderedDict)

    structure = data['structure']
    index = _parse_dimensions(structure['dimensions']['observation'])
    columns = _parse_dimensions(structure['dimensions']['series'])

    dataset = data['dataSets']
    if len(dataset) != 1:
        raise ValueError("length of 'dataSets' must be 1")
    dataset = dataset[0]
    values = _parse_values(dataset, index=index, columns=columns)

    df = pd.DataFrame(values, columns=columns, index=index)
    return df


def _get_indexer(index):
    if index.nlevels == 1:
        return [str(i) for i in compat.range(len(index))]
    else:
        it = itertools.product(*[compat.range(
            len(level)) for level in index.levels])
        return [':'.join(map(str, i)) for i in it]


def _parse_values(dataset, index, columns):
    size = len(index)
    series = dataset['series']

    values = []
    # for s_key, s_value in compat.iteritems(series):
    for s_key in _get_indexer(columns):
        try:
            observations = series[s_key]['observations']
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
        values = [v['name'] for v in key['values']]

        role = key.get('role', None)
        if role in ('time', 'TIME_PERIOD'):
            values = pd.DatetimeIndex(values)

        arrays.append(values)
        names.append(key['name'])
    midx = pd.MultiIndex.from_product(arrays, names=names)
    if len(arrays) == 1 and isinstance(midx, pd.MultiIndex):
        # Fix for pandas >= 0.21
        midx = midx.levels[0]

    return midx
