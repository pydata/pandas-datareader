# pylint: disable-msg=E1101,W0613,W0603

import os

import numpy as np
import pandas as pd
import pandas.testing as tm
import pytest

from pandas_datareader.compat import PANDAS_0210
from pandas_datareader.io import read_jsdmx

pytestmark = pytest.mark.stable


@pytest.fixture
def dirpath(datapath):
    return datapath("io", "data")


@pytest.mark.skipif(not PANDAS_0210, reason="Broken on old pandas")
def test_tourism(dirpath):
    # OECD -> Industry and Services -> Inbound Tourism
    result = read_jsdmx(os.path.join(dirpath, "jsdmx", "tourism.json"))
    assert isinstance(result, pd.DataFrame)
    jp = result["Japan"]
    visitors = [
        "China",
        "Hong Kong, China",
        "Total international arrivals",
        "Korea",
        "Chinese Taipei",
        "United States",
    ]

    exp_col = pd.Index(
        [
            "China",
            "Hong Kong, China",
            "Total international arrivals",
            "Korea",
            "Chinese Taipei",
            "United States",
        ],
        name="Variable",
    )
    exp_idx = pd.DatetimeIndex(
        [
            "2008-01-01",
            "2009-01-01",
            "2010-01-01",
            "2011-01-01",
            "2012-01-01",
            "2013-01-01",
            "2014-01-01",
            "2015-01-01",
            "2016-01-01",
        ],
        name="Year",
    )
    values = [
        [1000000.0, 550000.0, 8351000.0, 2382000.0, 1390000.0, 768000.0],
        [1006000.0, 450000.0, 6790000.0, 1587000.0, 1024000.0, 700000.0],
        [1413000.0, 509000.0, 8611000.0, 2440000.0, 1268000.0, 727000.0],
        [1043000.0, 365000.0, 6219000.0, 1658000.0, 994000.0, 566000.0],
        [1430000.0, 482000.0, 8368000.0, 2044000.0, 1467000.0, 717000.0],
        [1314000.0, 746000.0, 10364000.0, 2456000.0, 2211000.0, 799000.0],
        [2409000.0, 926000.0, 13413000.0, 2755000.0, 2830000.0, 892000.0],
        [4993689.0, 1524292.0, 19737409.0, 4002095.0, 3677075.0, 1033258.0],
        [6373564.0, 1839193.0, 24039700.0, 5090302.0, 4167512.0, 1242719.0],
    ]
    values = np.array(values, dtype="object")
    expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
    tm.assert_frame_equal(jp[visitors], expected)


@pytest.mark.skipif(not PANDAS_0210, reason="Broken on old pandas")
def test_land_use(dirpath):
    # OECD -> Environment -> Resources Land Use
    result = read_jsdmx(os.path.join(dirpath, "jsdmx", "land_use.json"))
    assert isinstance(result, pd.DataFrame)
    result = result.loc["2010":"2011"]

    cols = [
        "Arable land and permanent crops",
        "Arable and cropland % land area",
        "Total area",
        "Forest",
        "Forest % land area",
        "Land area",
        "Permanent meadows and pastures",
        "Meadows and pastures % land area",
        "Other areas",
        "Other % land area",
    ]
    exp_col = pd.MultiIndex.from_product(
        [["Japan", "United States"], cols], names=["Country", "Variable"]
    )
    exp_idx = pd.DatetimeIndex(["2010", "2011"], name="Year")
    values = [
        [
            53790.0,
            14.753154141525,
            377800.0,
            np.nan,
            np.nan,
            364600.0,
            5000.0,
            1.3713658804169,
            np.nan,
            np.nan,
            1897990.0,
            20.722767650476,
            9629090.0,
            np.nan,
            np.nan,
            9158960.0,
            2416000.0,
            26.378540795025,
            np.nan,
            np.nan,
        ],
        [
            53580.0,
            14.691527282698,
            377800.0,
            np.nan,
            np.nan,
            364700.0,
            5000.0,
            1.3709898546751,
            np.nan,
            np.nan,
            1897990.0,
            20.722767650476,
            9629090.0,
            np.nan,
            np.nan,
            9158960.0,
            2416000.0,
            26.378540795025,
            np.nan,
            np.nan,
        ],
    ]
    values = np.array(values)
    expected = pd.DataFrame(values, index=exp_idx, columns=exp_col)
    tm.assert_frame_equal(result[exp_col], expected)


@pytest.mark.skipif(not PANDAS_0210, reason="Broken on old pandas")
def test_quartervalue(dirpath):
    # https://stats.oecd.org/sdmx-json/data/QNA/AUS+AUT+BEL+CAN+CHL.GDP+B1_
    #    GE.CUR+VOBARSA.Q/all?startTime=2009-Q1&endTime=2011-Q4
    result = read_jsdmx(os.path.join(dirpath, "jsdmx", "oecd1.json"))
    assert isinstance(result, pd.DataFrame)
    expected = pd.DatetimeIndex(
        [
            "2009-01-01",
            "2009-04-01",
            "2009-07-01",
            "2009-10-01",
            "2010-01-01",
            "2010-04-01",
            "2010-07-01",
            "2010-10-01",
            "2011-01-01",
            "2011-04-01",
            "2011-07-01",
            "2011-10-01",
        ],
        dtype="datetime64[ns]",
        name=u"Period",
        freq=None,
    )
    tm.assert_index_equal(result.index, expected)
