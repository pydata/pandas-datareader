from __future__ import unicode_literals

import collections
from io import BytesIO
import time
import zipfile

import pandas as pd

from pandas_datareader.compat import HTTPError, str_to_bytes
from pandas_datareader.io.util import _read_content

_STRUCTURE = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
_MESSAGE = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}"
_GENERIC = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}"
_COMMON = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"
_XML = "{http://www.w3.org/XML/1998/namespace}"

_DATASET = _MESSAGE + "DataSet"
_SERIES = _GENERIC + "Series"
_SERIES_KEY = _GENERIC + "SeriesKey"
_OBSERVATION = _GENERIC + "Obs"
_VALUE = _GENERIC + "Value"
_OBSDIMENSION = _GENERIC + "ObsDimension"
_OBSVALUE = _GENERIC + "ObsValue"
_CODE = _STRUCTURE + "Code"
_TIMEDIMENSION = _STRUCTURE + "TimeDimension"


def read_sdmx(path_or_buf, dtype="float64", dsd=None):
    """
    Convert a SDMX-XML string to pandas object

    Parameters
    ----------
    path_or_buf : a valid SDMX-XML string or file-like
        https://webgate.ec.europa.eu/fpfis/mwikis/sdmx/index.php/Main_Page
    dtype : str
        dtype to coerce values
    dsd : dict
        parsed DSD dict corresponding to the SDMX-XML data

    Returns
    -------
    results : Series, DataFrame, or dictionaly of Series or DataFrame.
    """

    xdata = _read_content(path_or_buf)

    from xml.etree import ElementTree as ET

    root = ET.fromstring(xdata)

    try:
        structure = _get_child(root, _MESSAGE + "Structure")
    except ValueError:
        # get zipped path
        result = list(root.iter(_COMMON + "Text"))[1].text
        if not result.startswith("http"):
            raise ValueError(result)

        for _ in range(60):
            # wait zipped data is prepared
            try:
                data = _read_zipped_sdmx(result)
                return read_sdmx(data, dtype=dtype, dsd=dsd)
            except HTTPError:
                time.sleep(1)
                continue

        msg = (
            "Unable to download zipped data within 60 secs, "
            "please download it manually from: {0}"
        )
        raise ValueError(msg.format(result))

    idx_name = structure.get("dimensionAtObservation")
    dataset = _get_child(root, _DATASET)

    keys = []
    obss = []

    for series in dataset.iter(_SERIES):
        key = _parse_series_key(series)
        obs = _parse_observations(series.iter(_OBSERVATION))
        keys.append(key)
        obss.append(obs)

    mcols = _construct_index(keys, dsd=dsd)
    mseries = _construct_series(obss, name=idx_name, dsd=dsd)

    df = pd.DataFrame(mseries, dtype=dtype)
    df = df.T
    df.columns = mcols

    return df


def _construct_series(values, name, dsd=None):

    # ts defines attributes to be handled as times
    times = dsd.ts if dsd is not None else []

    if len(values) < 1:
        raise ValueError("Data contains no 'Series'")
    results = []
    for value in values:

        if name in times:
            tvalue = [v[0] for v in value]
            try:
                idx = pd.DatetimeIndex(tvalue, name=name)
            except ValueError:
                # time may be unsupported format, like '2015-B1'
                idx = pd.Index(tvalue, name=name)
        else:
            idx = pd.Index([v[0] for v in value], name=name)

        results.append(pd.Series([v[1] for v in value], index=idx))
    return results


def _construct_index(keys, dsd=None):

    # code defines a mapping to key's internal code to its representation
    codes = dsd.codes if dsd is not None else {}

    if len(keys) < 1:
        raise ValueError("Data contains no 'Series'")
    names = [t[0] for t in keys[0]]
    values = {}
    # initialize
    for key in keys:
        for name, value in key:
            # apply DSD
            try:
                value = codes[name][value]
            except KeyError:
                pass

            try:
                values[name].append(value)
            except KeyError:
                values[name] = [value]

    midx = pd.MultiIndex.from_arrays([values[name] for name in names], names=names)
    return midx


def _parse_observations(observations):
    results = []
    for observation in observations:
        obsdimension = _get_child(observation, _OBSDIMENSION)
        obsvalue = _get_child(observation, _OBSVALUE)
        results.append((obsdimension.get("value"), obsvalue.get("value")))
    # return list of key/value tuple, eg: [(key, value), ...]
    return results


def _parse_series_key(series):
    serieskey = _get_child(series, _SERIES_KEY)
    key_values = serieskey.iter(_VALUE)
    keys = [(key.get("id"), key.get("value")) for key in key_values]
    # return list of key/value tuple, eg: [(key, value), ...]
    return keys


def _get_child(element, key):
    elements = list(element.iter(key))
    if len(elements) == 1:
        return elements[0]
    elif len(elements) == 0:
        raise ValueError("Element {0} contains " "no {1}".format(element.tag, key))
    else:
        raise ValueError(
            "Element {0} contains " "multiple {1}".format(element.tag, key)
        )


_NAME_EN = ".//{0}Name[@{1}lang='en']".format(_COMMON, _XML)


def _get_english_name(element):
    name = element.find(_NAME_EN).text
    return name


SDMXCode = collections.namedtuple("SDMXCode", ["codes", "ts"])


def _read_sdmx_dsd(path_or_buf):
    """
    Convert a SDMX-XML DSD string to mapping dictionary

    Parameters
    ----------
    filepath_or_buffer : a valid SDMX-XML DSD string or file-like
        https://webgate.ec.europa.eu/fpfis/mwikis/sdmx/index.php/Main_Page

    Returns
    -------
    results : namedtuple (SDMXCode)
    """

    xdata = _read_content(path_or_buf)

    from xml.etree import cElementTree as ET

    root = ET.fromstring(xdata)

    structure = _get_child(root, _MESSAGE + "Structures")
    codes = _get_child(structure, _STRUCTURE + "Codelists")
    # concepts = _get_child(structure, _STRUCTURE + 'Concepts')
    datastructures = _get_child(structure, _STRUCTURE + "DataStructures")

    code_results = {}
    for codelist in codes:
        # codelist_id = codelist.get('id')
        codelist_name = _get_english_name(codelist)
        mapper = {}
        for code in codelist.iter(_CODE):
            code_id = code.get("id")
            name = _get_english_name(code)
            mapper[code_id] = name
        # codeobj = SDMXCode(id=codelist_id, name=codelist_name, mapper=mapper)
        # code_results[codelist_id] = codeobj
        code_results[codelist_name] = mapper

    times = list(datastructures.iter(_TIMEDIMENSION))
    times = [t.get("id") for t in times]

    result = SDMXCode(codes=code_results, ts=times)
    return result


def _read_zipped_sdmx(path_or_buf):
    """Unzipp data contains SDMX-XML"""
    data = _read_content(path_or_buf)

    zp = BytesIO()
    zp.write(str_to_bytes(data))
    f = zipfile.ZipFile(zp)
    files = f.namelist()
    assert len(files) == 1
    return f.open(files[0])
