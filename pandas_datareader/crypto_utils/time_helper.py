#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Contains helper classes to manage dates and times.

Classes:
- TimeHelper: Used to create/convert timezone aware (UTC+0) dates and times.

Enums:
- TimeUnit: Used to indicate the unit of timestamps.
"""

from datetime import datetime, timezone
from enum import IntEnum

from datetime_periods import period
from dateutil.parser import parse


class TimeUnit(IntEnum):
    """
    An enumeration to indicate the unit of timestamps.
    """
    SECONDS = 0
    MILLISECONDS = 1
    MICROSECONDS = 2
    NANOSECONDS = 3


class TimeHelper:
    """
    A helper class to create/convert dates and times.

    It ensures that all dates and times are timezone aware (UTC+0).

    freq_map is used to convert specific strings from plural into singular.
    """
    freq_map = {
        "minutes": "minute",
        "hours": "hour",
        "days": "day",
        "weeks": "week",
        "months": "month"
    }

    @staticmethod
    def now() -> datetime:
        """
        Get the current datetime (UTC+0).

        The accuracy is limited to milliseconds and the remaining microseconds are cleared.

        @return: The current datetime (UTC+0).
        @rtype: datetime
        """
        now = datetime.now(tz=timezone.utc)
        return now.replace(microsecond=now.microsecond - now.microsecond % 1000)

    @staticmethod
    def now_timestamp(unit: TimeUnit = TimeUnit.SECONDS) -> float:
        """
        Get the timestamp of the current datetime (UTC+0).

        @param unit: The desired time unit of the timestamp.
        @type unit: TimeUnit

        @return: The timestamp of the current datetime (UTC+0).
        @rtype: float
        """
        return TimeHelper.to_timestamp(TimeHelper.now(), unit)

    @staticmethod
    def from_string(representation: str) -> datetime:
        """
        Get a datetime (UTC+0) from a given representation.

        @param representation: The string that represents a datetime.
        @type representation: str

        @return: The datetime (UTC+0) of the given representation.
        @rtype: datetime
        """
        return parse(representation).replace(tzinfo=timezone.utc)

    @staticmethod
    def from_timestamp(timestamp: float, unit: TimeUnit = TimeUnit.SECONDS) -> datetime:
        """
        Get a datetime (UTC+0) from a given timestamp.

        @param timestamp: The timestamp whose datetime is to be obtained.
        @type timestamp: float
        @param unit: The time unit in which the timestamp is given.
        @type unit: TimeUnit

        @return: The datetime (UTC+0) of the given timestamp.
        @rtype: datetime
        """
        timestamp_in_sec: float = timestamp / (1000 ** int(unit))
        return datetime.fromtimestamp(timestamp_in_sec, tz=timezone.utc)

    @staticmethod
    def to_timestamp(date_time: datetime, unit: TimeUnit = TimeUnit.SECONDS) -> float:
        """
        Convert a datetime to a timestamp.

        @param date_time: The datetime to be converted.
        @type date_time: datetime
        @param unit: The desired time unit of the timestamp.
        @type unit: TimeUnit

        @return: The timestamp of the given datetime in the desired time unit.
        @rtype: float
        """
        return date_time.replace(tzinfo=timezone.utc).timestamp() * (1000 ** int(unit))

    @staticmethod
    def start_end_conversion(date_time: datetime, frequency: str, to_end: bool = True) -> datetime:
        """
        Returns the beginning/end of a period.

        @param date_time: The datetime object to be converted.
        @type date_time: datetime
        @param frequency: The underlying period frequency.
        @type frequency: str
        @param to_end: boolean, return end of period. Default: True
        @type to_end: bool

        @return: datetime of start/end of period.
        @rtype: datetime
        """
        return period(date_time, TimeHelper.freq_map[frequency])[int(to_end)]
