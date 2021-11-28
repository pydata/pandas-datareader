#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module providing several utility functions needed across the whole program.
"""

from typing import Any, Optional, Dict, List

import os
from sys import stdout
import calendar
import datetime
from datetime import timedelta
import dateutil.parser
import yaml
import pandas as pd

from pandas_datareader.crypto_utils.time_helper import TimeHelper, TimeUnit
import pandas_datareader.crypto_utils._paths as _paths

TYPE_CONVERSIONS = {
    ("float", "from_timestamp"): {"function": TimeHelper.from_timestamp, "params": 1},
    ("bool", "int"): {"function": int, "params": 0},
    ("float", "int"): {"function": int, "params": 0},
    ("int", "bool"): {"function": bool, "params": 0},
    ("int", "div"): {"function": lambda integer, div: integer / (1 * div), "params": 1},
    ("any", "value"): {"function": lambda number: float(number) > 0, "params": 0},
    ("str", "bool"): {"function": lambda string: string.lower() == "true", "params": 0},
    ("str", "int"): {"function": int, "params": 0},
    ("str", "float"): {"function": float, "params": 0},
    ("str", "float_absolut"): {
        "function": lambda string: abs(float(string)),
        "params": 0,
    },
    ("str", "floatNA"): {
        "function": lambda string: float(string) if string != "N/A" else None,
        "params": 0,
    },
    ("str", "strptime"): {
        "function": lambda string, *args: datetime.datetime.strptime(string, args[0]),
        "params": 1,
    },
    ("strptime_w_f", "strptime_wo_f"): {
        "function": lambda string, *args: datetime.datetime.strptime(
            string.split(".")[0], *args
        ),
        "params": 1,
    },
    ("str", "split"): {
        "function": lambda string, *args: string.split(args[0])[args[1]]
        if args[0] in string
        else None,
        "params": 2,
    },
    ("str", "splitupper"): {
        "function": lambda string, *args: string.split(args[0])[args[1]].upper(),
        "params": 2,
    },
    ("str", "slice"): {
        "function": lambda string, *args: string[args[0] : args[1]],
        "params": 2,
    },
    ("str", "upper"): {"function": lambda string: string.upper(), "params": 0},
    ("str", "lower"): {"function": lambda string: string.lower(), "params": 0},
    ("str", "dateparser"): {"function": dateutil.parser.parse, "params": 0},
    ("datetime", "strftime"): {
        "function": lambda time, *args: datetime.datetime.strftime(time, args[0]),
        "params": 1,
    },
    ("dateparser", "totimestamp"): {
        "function": lambda time: int(time.timestamp()),
        "params": 0,
    },
    ("datetime", "totimestamp"): {
        "function": lambda time: int(time.timestamp()),
        "params": 0,
    },
    ("datetime", "totimestampms"): {
        "function": lambda time: int(round(time.timestamp() * 1000)),
        "params": 0,
    },
    ("datetime", "utctotimestamp"): {
        "function": lambda time: calendar.timegm(time.utctimetuple()),
        "params": 0,
    },
    ("strptime", "totimestamp"): {
        "function": lambda string, *args: int(
            datetime.datetime.timestamp(datetime.datetime.strptime(string, args[0]))
        ),
        "params": 1,
    },
    ("none", "nowstrptime"): {
        "function": lambda arg: TimeHelper.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ),
        "params": 0,
    },
    ("none", "now"): {"function": TimeHelper.now, "params": 0},
    ("none", "now_format"): {
        "function": lambda spec: format(TimeHelper.now(), spec),
        "params": 1,
    },
    ("none", "constant"): {  # Returns the first argument
        "function": lambda *args: args[0],
        "params": 1,
    },
    ("none", "range"): {"function": lambda: range(1), "params": 0},
    ("value", "map"): {
        # translate into buy/sell. Args: {0: 'buy', 1:'sell'} and arg[0]
        # is the response value (i.e. 0/1)
        "function": lambda *args: {args[1]: args[2], args[3]: args[4]}[args[0]],
        "params": 4,
    },
    ("str", "split_at_del_or_index"): {
        "function": lambda string, *args: string.split(args[0])[args[2]]
        if len(string) != len(string.split(args[0])[0])
        else string[: args[1]]
        if args[2] == 0
        else string[args[1] :],
        "params": 3,  # delimiter, index, 0 or 1 aka. left or right
    },
    ("none", "now_timestamp"): {
        "function": lambda: int(TimeHelper.now_timestamp()),
        "params": 0,
    },
    ("none", "now_timestampms"): {
        "function": lambda: int(TimeHelper.now_timestamp(TimeUnit.MILLISECONDS)),
        "params": 0,
    },
    ("now", "timedelta"): {
        "function": lambda delta: int(
            TimeHelper.to_timestamp(TimeHelper.now() - timedelta(days=int(delta)))
        ),
        "params": 1,
    },
    ("datetime", "timedelta"): {
        "function": lambda time, interval, delta: int(
            TimeHelper.to_timestamp(time - timedelta(**{interval: int(delta)}))
        ),
        "params": 2,
    },
    ("utcfromtimestamp", "timedelta"): {
        "function": lambda time, interval, value: TimeHelper.from_timestamp(time)
        - timedelta(**{interval: value})
        if isinstance(time, int)
        else dateutil.parser.parse(time) - timedelta(**{interval: value}),
        "params": 2,
    },
    ("datetime", "timedeltams"): {
        "function": lambda time, interval, delta: int(
            TimeHelper.to_timestamp(time - timedelta(**{interval: int(delta)}))
        )
        * 1000,
        "params": 2,
    },
    ("datetime", "timestamp"): {
        "function": lambda time: int(TimeHelper.to_timestamp(time)),
        "params": 0,
    },
    ("datetime", "timestampms"): {
        "function": lambda time: int(TimeHelper.to_timestamp(time)) * 1000,
        "params": 0,
    },
    ("datetime", "format"): {"function": format, "params": 1},
    ("timedelta", "from_timestamp"): {
        "function": lambda time, unit, spec: format(
            TimeHelper.from_timestamp(time, unit), spec
        ),
        "params": 2,
    },
    ("from_timestamp", "to_start"): {
        "function": lambda time, interval: TimeHelper.start_end_conversion(
            time, interval, False
        ),
        "params": 1,
    },
    ("from_timestamp", "to_end"): {
        "function": lambda time, interval: TimeHelper.start_end_conversion(
            time, interval, True
        ),
        "params": 1,
    },
}
"""
    Type Conversions used to convert extracted values from the API-Response
    into the desired type ("first", "second"). The values are specified in the 
    .yaml-file of each exchange under the "mapping" of each method.
    The function is called in the Mapping Class of utilities.py under 
    the method convert_types().

    "first":
        The actual type extracted from the API-Request (.json)
    "second":
        The desired type to convert
    "function":
        the actual function to apply
    "params":
        the number of additional parameters needed
"""


def yaml_loader(exchange: str) -> Dict[str, Any]:
    """
    Loads, reads and returns the data of a .yaml-file specified by the param exchange.

    @param exchange: The file name to load (exchange).
    @type exchange: str

    @return: Returns a dict of the loaded data from the .yaml-file.
    @rtype: dict

    @raise Exception: If the .yaml file could not be evaluated for a given exchange.
    """
    exchange = exchange.replace(" ", "")

    path = _paths.all_paths.get("yaml_path")

    try:
        with open(
            path.joinpath(".".join([exchange.lower(), "yaml"])), "r", encoding="UTF-8"
        ) as file:
            return yaml.load(file, Loader=yaml.FullLoader)

    except FileNotFoundError:
        print(
            f"\nFile {path.joinpath('.'.join([exchange.lower(), 'yaml']))} not found."
        )


def get_exchange_names() -> Optional[List[str]]:
    """
    Gives information about all exchange that the program will send
    requests to. This means if the name of a exchange is not part of the
    list that is returned, the program will not send any request to said
    exchange.

    @return: Names from all the exchange, which have a .yaml-file in
             the directory described in YAML_PATH.
    """
    path_to_resources = _paths.all_paths.get("yaml_path")

    try:
        exchanges = os.listdir(path_to_resources)
        exchanges = [x.split(".yaml")[0] for x in exchanges if x.endswith(".yaml")]
        exchanges.sort()
    except FileNotFoundError:
        print(f"YAML files not found. The path {path_to_resources} is incorrect.")
        return

    return exchanges


def replace_list_item(replace_list: list, condition: str, value: str) -> list:
    """
    Replaces a specific value from a list.
    @param replace_list: The list in which the value needs to be replaced
    @param condition: The value to be updated
    @param value: The new value
    @return: Updated list
    """
    for i, item in enumerate(replace_list):
        if item == condition:
            replace_list[i] = value
    return replace_list


def split_str_to_list(string: str, splitter: str = ",") -> List[str]:
    """
    Splits a string into a list of string.

    @param string: A long string.
    @param splitter: The splitting parameter.

    @return: List of strings.
    """
    items = string.rsplit(splitter)

    # remove possible blanks from strings
    return [item.replace(" ", "").lower() for item in items]


def sort_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """ Sort columns in OHLCV order.

    @param dataframe: Requested data with unordered columns
    @return: pd.DataFrame with ordered columns
    """

    # remove columns not returned by the exchange and maintain order.
    columns = ["open", "high", "low", "close", "volume", "market_cap"]
    columns = sorted(set(columns).intersection(dataframe.columns), key=columns.index)

    return dataframe.loc[:, columns]


def print_timestamp(timestamp):
    """ Prints the actual request timestamp.

    @param timestamp: The timestamp
    """

    stdout.write("Requesting from: \r{}".format(timestamp))
    stdout.flush()
