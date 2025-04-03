from datetime import datetime

import numpy as np
import pandas as pd
from pandas import testing as tm
import pytest

from pandas_datareader import data as web
from pandas_datareader._utils import RemoteDataError


class TestOECD:
    @pytest.mark.xfail(reason="Incorrect URL")
    def test_get_un_den(self):
        df = web.DataReader(
            "TUD", "oecd", start=datetime(1960, 1, 1), end=datetime(2012, 1, 1)
        )

        au = [
            50.17292785,
            49.47181009,
            49.52106174,
            49.16341327,
            48.19296375,
            47.8863461,
            45.83517292,
            45.02021403,
            44.78983834,
            44.37794217,
            44.15358142,
            45.38865546,
            46.33092037,
            47.2343406,
            48.80023876,
            50.0639872,
            50.23390644,
            49.8214994,
            49.67636585,
            49.55227375,
            48.48657368,
            47.41179739,
            47.52526561,
            47.93048854,
            47.26327162,
            45.4617105,
            43.90202112,
            42.32759607,
            40.35838899,
            39.35157364,
            39.55023059,
            39.93212859,
            39.21948472,
            37.24343693,
            34.42549573,
            32.51172639,
            31.16809569,
            29.78835077,
            28.14657769,
            25.41970706,
            25.71752984,
            24.53108811,
            23.21936888,
            22.99140633,
            22.29380238,
            22.29160819,
            20.22236326,
            18.51151852,
            18.56792804,
            19.31219498,
            18.44405734,
            18.51105048,
            18.19718895,
        ]
        jp = [
            32.32911392,
            33.73688458,
            34.5969919,
            35.01871257,
            35.46869345,
            35.28164117,
            34.749499,
            34.40573103,
            34.50762389,
            35.16411379,
            35.10284332,
            34.57209848,
            34.31168831,
            33.46611342,
            34.26450371,
            34.53099287,
            33.69881466,
            32.99814274,
            32.59541985,
            31.75696594,
            31.14832536,
            30.8917513,
            30.56612982,
            29.75285171,
            29.22391559,
            28.79202411,
            28.18680064,
            27.71454381,
            26.94358748,
            26.13165206,
            25.36711479,
            24.78408637,
            24.49892557,
            24.34256055,
            24.25324675,
            23.96731902,
            23.3953401,
            22.78797997,
            22.52794337,
            22.18157944,
            21.54406273,
            20.88284597,
            20.26073907,
            19.73945642,
            19.25116713,
            18.79844243,
            18.3497807,
            18.25095057,
            18.2204924,
            18.45787546,
            18.40380743,
            18.99504195,
            17.97238372,
        ]
        us = [
            30.89748411,
            29.51891217,
            29.34276869,
            28.51337535,
            28.30646144,
            28.16661991,
            28.19557735,
            27.76578899,
            27.9004622,
            27.30836054,
            27.43402867,
            26.94941363,
            26.25996487,
            25.83134349,
            25.74427582,
            25.28771204,
            24.38412814,
            23.59186681,
            23.94328194,
            22.36400776,
            22.06009466,
            21.01328205,
            20.47463895,
            19.45290876,
            18.22953818,
            17.44855678,
            17.00126975,
            16.5162476,
            16.24744487,
            15.86401127,
            15.45147174,
            15.46986912,
            15.1499578,
            15.13654544,
            14.91544059,
            14.31762091,
            14.02052225,
            13.55213736,
            13.39571457,
            13.36670812,
            12.84874656,
            12.85719022,
            12.63753733,
            12.39142968,
            12.02130767,
            11.96023574,
            11.48458378,
            11.56435375,
            11.91022276,
            11.79401904,
            11.38345975,
            11.32948829,
            10.81535229,
        ]

        index = pd.date_range("1960-01-01", "2012-01-01", freq="AS", name="Time")
        for label, values in [("Australia", au), ("Japan", jp), ("United States", us)]:
            expected = pd.Series(values, index=index, name=label)
            tm.assert_series_equal(df[label], expected)

    @pytest.mark.xfail(reason="Changes in API need fixes")
    def test_get_tourism(self):
        df = web.DataReader(
            "TOURISM_INBOUND",
            "oecd",
            start=datetime(2008, 1, 1),
            end=datetime(2012, 1, 1),
        )

        jp = np.array([8351000, 6790000, 8611000, 6219000, 8368000], dtype=float)
        us = np.array(
            [175702309, 160507417, 164079732, 167600277, 171320408], dtype=float
        )
        index = pd.date_range("2008-01-01", "2012-01-01", freq="AS", name="Year")
        for label, values in [("Japan", jp), ("United States", us)]:
            expected = pd.Series(values, index=index, name="Tourism demand surveys")
            expected.index.freq = None
            series = df[label]["Total international arrivals"]["Tourism demand surveys"]
            tm.assert_series_equal(series, expected)

    def test_oecd_invalid_symbol(self):
        with pytest.raises(RemoteDataError):
            web.DataReader("INVALID_KEY", "oecd")

        with pytest.raises(ValueError):
            web.DataReader(1234, "oecd")
