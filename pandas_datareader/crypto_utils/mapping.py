#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module to find, extract and convert values from the exchange responses.

Classes:
    - Mapping
Functions:
    - convert_type
    - extract_mappings
    - is_scalar
"""

from collections import deque
from collections.abc import Iterable
from typing import Any, List, Dict, Deque, Tuple

from pandas_datareader.crypto_utils.utilities import TYPE_CONVERSIONS


def convert_type(value: Any, types_queue: Deque[str]) -> Any:
    """ Converts the value via type conversions.

    Helper method to convert the given value via a queue of type conversions.

    @param value: The value to get converted to another type.
    @type value: Any
    @param types_queue: The queue of type conversion instructions.
    @type types_queue: deque

    @return: The converted value.
    @rtype: Any
    """
    current_type = types_queue.popleft()

    result = value

    while types_queue:
        next_type = types_queue.popleft()

        types_tuple = (current_type, next_type)

        if "continue" in types_tuple:
            continue

        conversion = TYPE_CONVERSIONS[types_tuple]

        params = list()

        for _ in range(conversion["params"]):
            params.append(types_queue.popleft())

        # Change here to avoid "None" as result value in the params when no
        # value to convert is needed (i.e. when methods are called with
        # ("none", ...).
        # if not result and isinstance(result, (str, list)):
        try:
            if result is None:
                result = conversion["function"](*params)
            else:
                result = conversion["function"](result, *params)

            current_type = next_type
        except Exception:
            return None

    return result


class Mapping:
    """ Class representing mapping data and logic.

    Class representing mapping data und further functionality provided
    with methods.

    Attributes:
        key:
            String being the keyword indicating one or several
            database table columns. See "database_column_mapping"
            in "config.yaml".
        path:
            An ordered list of keys used for traversal through the
            response dict with the intention of returning the value wanted
            for the database.
        types:
            An ordered sequence of types and
            additional parameters (if necessary). Is used to conduct
            type conversions within the method "extract_value()".
    """

    def __init__(self, key: str, path: List[str], types: List[str]):
        """ Constructor of Mapping.

        Constructor method for constructing method objects.

        @param key: String being the keyword indicating one or several
                    database table columns. See "database_column_mapping"
                    in "config.yaml".
        @type key: str
        @param path: An ordered list of keys used for traversal through the
                     response dict with the intention of returning the value wanted
                     for the database.
        @type path: list
        @param types: An ordered sequence of types and
                      additional parameters (if necessary). Is used to conduct
                      type conversions within the method "extract_value()".
        @type types: list
        """
        self.key = key
        self.path = path
        self.types = types

    def traverse_path(
        self,
        response: Dict[str, Any],
        path_queue: Deque[str],
        currency_pair_info: Tuple[str, str, str] = None,
    ) -> Any:
        """ Traverses the path on a response.

        Helper method for traversing the path on the given response dict (subset).

        @param response: The response dict (subset).
        @type response: dict
        @param path_queue: The queue of path traversal instructions.
        @type path_queue: deque
        @param currency_pair_info: The formatted String of a currency pair.
                                   For special case that the key of a dictionary
                                   is the formatted currency pair string.
        @type currency_pair_info: tuple[str, str, str]

        @return: The traversed response dict.
        @rtype: Optional[dict]
        """
        path_element = path_queue.popleft()

        if path_element == "dict_key":
            # Special case to extract value from "dict_key"
            traversed = list(response.keys())
        elif path_element == "dict_values":
            # Special case to extract value from "dict_values"
            traversed = list(response.values())
        elif path_element == "list_key":
            # Special case with the currency_pair prior to a list
            traversed = list(response.keys())
        elif path_element == "list_values":
            traversed = list(response.values())
        elif path_element == []:
            # Case to extract multiple values from a single list ["USD","BTC",...]
            traversed = response
        elif path_element == "currency_pair" and currency_pair_info[2] is not None:
            traversed = response[currency_pair_info[2]]
        elif is_scalar(response):
            return None
        else:  # Special case for kraken.
            if isinstance(response, dict) and path_element not in response.keys():
                return None
            else:
                traversed = response[path_element]

        return traversed

    def extract_value(
        self,
        response: Any,
        path_queue: Deque[str] = None,
        types_queue: Deque[str] = None,
        iterate: bool = True,
        currency_pair_info: Tuple[str, str, str] = (None, None, None),
    ) -> Any:
        """ Extracts the value specified by "self.path".

        Extracts the value specified by the path sequence and converts it
        using the "types" specified.

        @param response: The response dict (JSON) returned by an API request.
        @type response: Collection
        @param path_queue: The queue of path traversal instructions.
        @type path_queue: deque
        @param types_queue: The queue of type conversion instructions.
        @type types_queue: deque
        @param iterate: Whether still an auto-iteration is possible.
        @type iterate: bool
        @param currency_pair_info: The formatted String of a currency pair.
        @type currency_pair_info: tuple[str, str, str]

        @return: The value specified by "path_queue" and converted
                 using "types_queue".
                 Can be a list of values which get extracted iteratively from
                 the response.
        @rtype: Any
        """

        if path_queue is None:
            path_queue = deque(self.path)

        if types_queue is None:
            types_queue = deque(self.types)

        if not response:
            return None

        if not path_queue:
            # TODO: after integration tests, look if clause for first
            #  and second currency can be deleted!
            if types_queue[0] == "first_currency":
                return currency_pair_info[0]
            elif types_queue[0] == "second_currency":
                return currency_pair_info[1]
            return convert_type(None, types_queue)

        while path_queue:

            if iterate and isinstance(response, list):
                # Iterate through list of results
                result = list()

                # special case for bitfinex
                if len(response) == 1:
                    response = response[0]
                    continue  # because instance of response has to be checked

                for item in response:

                    if is_scalar(item):
                        return self.extract_value(
                            response, path_queue, types_queue, iterate=False
                        )

                    result.append(
                        self.extract_value(item, deque(path_queue), deque(types_queue))
                    )

                return result

            elif is_scalar(response):
                # Return converted scalar value
                return convert_type(response, types_queue)

            # Special case for Bitz to handle empty dicts/lists.
            elif not response:
                return None

            else:
                # Traverse path
                response = self.traverse_path(
                    response, path_queue, currency_pair_info=currency_pair_info
                )

        if (
            types_queue and response is not None
        ):  # None to allow to change 0 to boolean.

            if isinstance(response, list):

                result = list()

                for item in response:
                    result.append(convert_type(item, deque(types_queue)))

                # for dict_key special_case aka.
                # test_extract_value_list_containing_dict_where_key_is_value()
                # in test_mapping.py
                if len(result) == 1:
                    result = result[0]

                response = result

            else:
                response = convert_type(response, types_queue)

        return response

    def __str__(self) -> str:
        """String representation of a Mapping"""
        string_path = list()

        for item in self.path:
            string_path.append(str(item))

        return " / ".join(string_path) + " -> " + str(self.key)


def extract_mappings(
    exchange_name: str, requests: Dict[str, Any]
) -> Dict[str, List[Mapping]]:
    """ Helper-Method which should be only called by the constructor.
    Extracts out of a given exchange .yaml-requests-section for each
    request the necessary mappings so the values can be extracted from
    the response for said request.

    The key-value in the dictionary is the same as the key for the request.
    i.e. behind 'ticker' are all the mappings stored which are necessary for
    extracting the values out of a ticker-response.

    If there is no mapping specified in the .yaml for a value which is contained
    by the response, the value will not be extracted later on because there won't
    be a Mapping-object for said value.

    @param exchange_name: str
        String representation of the exchange name.
    @param requests: Dict[str: List[Mapping]]
        Requests-section from a exchange.yaml as dictionary.
        Method does not check if dictionary contains viable information.

    @return:
        Dictionary with the following structure:
            {'request_name': List[Mapping]}
    """

    response_mappings = dict()
    if requests:
        for request in requests:
            request_mapping = requests[request]

            if "mapping" in request_mapping.keys():
                mapping = request_mapping["mapping"]
                mapping_list = list()

                try:
                    for entry in mapping:
                        mapping_list.append(
                            Mapping(entry["key"], entry["path"], entry["type"])
                        )
                except KeyError:
                    print(
                        f"Error loading mappings of {exchange_name} "
                        f"in {request}: {entry}"
                    )
                    break

                response_mappings[request] = mapping_list

    return response_mappings


def is_scalar(value: Any) -> bool:
    """ Indicates whether a value is a scalar or not.

    Convenience function returning a bool whether the provided value
    is a single value or not. Strings count as scalar although they are iterable.

    @param value: The value to evaluate concerning whether it is a single value
                  or multiple values (iterable).
    @type value: Any

    @return: Bool indicating whether the provided value is a single value or not.
    @rtype: bool
    """
    return isinstance(value, str) or not isinstance(value, Iterable)
