#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Tuple, Any, Dict, Union, List, Optional, Iterator

from abc import ABC
from datetime import datetime
import string
from collections import OrderedDict, deque
import itertools

from pandas_datareader.crypto_utils.utilities import yaml_loader, replace_list_item
from pandas_datareader.crypto_utils.mapping import extract_mappings
from pandas_datareader.crypto_utils.mapping import convert_type
from pandas_datareader.base import _BaseReader
from pandas_datareader.exceptions import EmptyResponseError


class Exchange(_BaseReader, ABC):
    """ Class for every exchange supported. The class extracts the request url, fits parameters,
    extracts the values from the response json and performs type-conversions."""

    def __init__(self, exchange_name: str, interval: str = 'days', *args, **kwargs):
        """ Constructor.

        @param exchange_name: The exchange name.
        @param interval: The candle interval.
        @param args: Additional ordered arguments for the BaseReader.
        @param kwargs: Additional keyword arguments for the BaseReader.
        """

        super(Exchange, self).__init__(*args, **kwargs)
        self.name = exchange_name
        self.yaml_file = yaml_loader(self.name)
        self.interval: str = interval
        self.rate_limit = self.get_rate_limit()
        self._base_end = kwargs.get("end", datetime.utcnow())

    def get_rate_limit(self) -> Union[int, float]:
        """ Calculates the rate-limit of an exchange.

        @return: The rate limit, i.e. time to "sleep" to not violate the limit in seconds.
        """
        if self.yaml_file.get("rate_limit"):
            if self.yaml_file["rate_limit"]["max"] <= 0:
                rate_limit = 0
            else:
                rate_limit = self.yaml_file["rate_limit"]["unit"] / self.yaml_file["rate_limit"]["max"]
        else:
            rate_limit = 0

        return rate_limit

    def apply_currency_pair_format(self, currency_pair: str) -> str:
        """ Helper method that applies the format described in the yaml for the specific
        request on the given currency-pair.

        @param currency_pair: String repr of the currency-pair
        @return: String of the formatted currency-pair. Example: BTC and ETH -> "btc_eth"
        """

        first, second = currency_pair.split("-")

        request_url_and_params = self.yaml_file.get("requests").get("historic_rates").get("request")
        pair_template_dict = request_url_and_params["pair_template"]
        pair_template = pair_template_dict["template"]

        formatted_string: str = pair_template.format(first=first, second=second)

        if pair_template_dict["lower_case"]:
            formatted_string = formatted_string.lower()
        else:
            formatted_string = formatted_string.upper()

        return formatted_string

    def get_formatted_url_and_params(self, url_and_parameters: Any, currency_pair: str,
                                     request_type: str = 'historic_rates') -> Tuple[str, Dict]:
        """ Formats the request url, inserts the currency-pair representation and/or
        extracts the parameters specified for the exchange and request.

        @param url_and_parameters: Extracted url and parameters from self.extract_request_urls.
        @param currency_pair: The currency pair of interest to format the url and params on.
        @param request_type: The request type. Default: "historic_rates". Possible: "historic_rates", "currency_pairs".
        @return Tuple of formatted url and formatted parameters.
        """

        url = url_and_parameters.get(request_type).get("url")
        pair_template = url_and_parameters.get(request_type).get("pair_template")
        pair_formatted = self.apply_currency_pair_format(currency_pair)
        parameters = url_and_parameters.get(request_type).get("params")

        parameters.update({key: parameters[key][currency_pair]
                           for key, val in parameters.items() if isinstance(val, dict)})

        # Case 1: Currency-Pairs in request parameters: eg. www.test.com?market=BTC-USD
        if "alias" in pair_template.keys() and pair_template["alias"]:
            parameters[pair_template["alias"]] = pair_formatted
        # Case 2: Currency-Pairs directly in URL: eg. www.test.com/BTC-USD
        elif pair_formatted:
            parameters.update({"currency_pair": pair_formatted})

        else:
            return url, parameters

        variables = [item[1] for item in string.Formatter().parse(url) if item[1] is not None]
        url_formatted = url.format(**parameters)
        # drop params who are filled directly into the url
        parameters = {k: v for k, v in parameters.items() if k not in variables}

        return url_formatted, parameters

    def extract_request_urls(self, currency_pairs: Optional[Dict[str, datetime]],
                             request_type: str = "historic_rates") -> Dict:
        """ Extracts the request url from the yaml-file and implements the parameters.

        @param currency_pairs: Currency-pair with the timestamp of the latest request.
        @param request_type: Request name, default: "historic_rates". Possible: "historic_rates", "currency_pairs".
        @return: Dict of the extracted url and parameters.
        """

        request_dict = self.yaml_file.get("requests").get(request_type).get("request")
        request_parameters = dict()
        request_parameters["url"] = self.yaml_file.get("api_url", "") + request_dict.get("template", "")
        request_parameters["pair_template"] = request_dict.get("pair_template", None)
        urls = dict()
        parameter = dict()
        parameters = request_dict.get("params", False)

        if not parameters:
            request_parameters["params"] = {}
            urls[request_type] = request_parameters
            return urls

        mapping: dict = {"allowed": self._allowed, "function": self._function,
                         "default": self._default, "type": self._type_con}

        # enumerate mapping dict to sort parameter values accordingly, i.e. {"allowed": 0, "function": 1, ...}
        mapping_index = {val: key for key, val in enumerate(mapping.keys())}

        for param, options in parameters.items():
            # Kick out all option keys which are not in the mapping dict or where required: False.
            # Sort the dict options according to the mapping-keys to ensure the right order of function calls.
            # Otherwise a (valid) specified value might be overwritten by the default value.
            options = {k: v for k, v in options.items() if k in mapping.keys()}
            options = OrderedDict(sorted(options.items(), key=lambda x: mapping_index.get(x[0])))

            if not parameters[param].get("required", True):
                continue
            # Iterate over the functions and fill the params dict with values. Kwargs are needed only partially.
            kwargs = {"has_value": None, "currency_pairs": currency_pairs}
            for key, val in options.items():
                kwargs.update({"has_value": parameter.get(param, None)})
                parameter[param] = mapping.get(key)(val, **kwargs)

        request_parameters["params"] = parameter
        urls[request_type] = request_parameters
        return urls

    def _allowed(self, val: dict, **_: dict) -> Any:
        """ Extract the configured value from all allowed values. If there is no match, return str "default".

        @param val: dict of allowed key, value pairs.
        @param _: unused additional arguments needed in other methods.
        @return: value if key in dict, else None.
        """
        if isinstance(self.interval, dict):
            value = None
        else:
            value = val.get(self.interval, None)

        if not bool(value):
            all_intervals = {"minutes": 1, "hours": 2, "days": 3, "weeks": 4, "months": 5}
            self.interval = {v: k for k, v in val.items() if all_intervals.get(k)}
        return value

    def _function(self, val: str, **kwargs: dict) -> Dict[str, datetime]:
        """ Execute function for all currency-pairs. Function returns the first
        timestamp in the DB, or datetime.now() if none exists.

        @param val: contains the function name as string.
        @param kwargs: not used but needed for another function.
        @return: The currency-pair with the respective timestamp to continue requesting.
        """
        if val == "last_timestamp":
            return {cp: self._get_first_timestamp(last_ts) for cp, last_ts in kwargs.get("currency_pairs").items()}

    def _default(self, val: str, **kwargs: dict) -> Optional[str]:
        """ Returns the default value if kwargs value (the parameter) is None.

        @param val: Default value.
        @param kwargs: Parameter value. If None, return default value.
        @return: Default value as a string.
        """

        default_val = val if not bool(kwargs.get("has_value")) else kwargs.get("has_value")
        if isinstance(self.interval, dict):
            self.interval = self.interval.get(default_val, None)

        return default_val if self.interval else None

    def _type_con(self, val: Any, **kwargs: dict) -> Any:
        """ Performs type conversions.

        @param val: The conversion values specified under "type".
        @param kwargs: The value to be converted.
        @return: Converted value.
        """

        param_value = kwargs.get("has_value", None)
        conv_params = val

        # to avoid conversion when only a type declaration was done. If a parameter is of type "int".
        if isinstance(conv_params, str) or len(conv_params) < 2:
            return param_value

        # replace the key "interval" with the interval specified in the configuration file.
        conv_params = [self.interval if x == "interval" else x for x in conv_params]

        # return {cp: convert_type(param_value[cp], deque(conv_params)) for cp in currency_pairs}
        # ToDo: Check if the above line works. The older version needed both if statements below.
        if isinstance(param_value, dict):
            return {cp: convert_type(param_value[cp], deque(conv_params)) for cp in kwargs.get("currency_pairs")}
        elif isinstance(conv_params, list):
            return convert_type(param_value, deque(conv_params))

    def _get_first_timestamp(self, last_ts: datetime) -> datetime:
        """ Returns the timestamp to continue requesting with.

        @param last_ts: The oldest timestamp from the previous request
        @return: The latest timestamp.
        """

        if last_ts:
            return last_ts

        if self.end.timestamp() > datetime.utcnow().timestamp():
            return datetime.utcnow()
        else:
            return self.end

    def format_data(self, responses: Union[Dict, List]) -> Tuple[Optional[List], Optional[List]]:
        """ Extracts and formats the response data, according to the mapping keys and path.
        Data is then ordered and returned as a tuple.

        @param responses: The response json
        @return: Tuple of extracted and formatted data and a list of the mapping keys in the same order.
        """

        if not responses:
            raise EmptyResponseError
            # return None, None

        results = list()
        mappings = extract_mappings(self.name, self.yaml_file.get('requests')).get('historic_rates')
        mapping_keys = [mapping.key for mapping in mappings]

        # creating dictionary where key is the name of the mapping which holds an empty list
        temp_results = dict(zip((key for key in mapping_keys), itertools.repeat([], len(mappings))))

        try:
            for mapping in mappings:
                if "interval" in mapping.types:
                    mapping.types = replace_list_item(mapping.types, "interval", self.interval)

                temp_results[mapping.key] = mapping.extract_value(responses)

                if isinstance(temp_results[mapping.key], str):
                    # Bugfix: if value is a single string, it is an iterable, and the string will
                    # be split in every letter. Therefore it is put into a list.
                    temp_results[mapping.key] = [temp_results[mapping.key]]

        except Exception:
            print("Error extracting values from response.")
            return None, None

        else:
            # CHANGE: One filed invalid -> all fields invalid.
            # changed this in order to avoid responses kicked out just because of one invalid field.
            # The response will be filtered out in the DB-Handler if the primary-keys are missing anyways.
            if all(value is None and not isinstance(value, datetime) for value in list(temp_results.values())):
                return None, None

            # asserting that the extracted lists for each mapping are having the same length
            assert (len(results[0]) == len(result) for result in temp_results)

            len_results = {key: len(value) for key, value in temp_results.items() if hasattr(value, "__iter__")}
            len_results = max(len_results.values()) if bool(len_results) else 1

            # update new keys only if not already exists to prevent overwriting!
            # temp_results = {"time": time, **temp_results}
            result = [v if hasattr(v, "__iter__")
                      else itertools.repeat(v, len_results) for k, v in temp_results.items()]

            result = list(itertools.zip_longest(*result))

            return result, list(temp_results.keys())

    def format_currency_pairs(self, response: Tuple[str, dict]) -> Optional[Iterator[Tuple[str, str, str]]]:
        """
        Extracts the currency-pairs of out of the given json-response
        that was collected from the Rest-API of this exchange.

        Process is similar to @see{self.format_ticker()}.

        @param response:
            Raw json-response from the Rest-API of this exchange that needs be formatted.
        @return:
            Iterator containing tuples of the following structure:
            (self.name, name of first currency-pair, name of second currency-pair)
        """

        results = {"currency_pair_first": [],
                   "currency_pair_second": []}
        mappings = extract_mappings(self.name, self.yaml_file.get('requests')).get('currency_pairs')

        for mapping in mappings:
            results[mapping.key] = mapping.extract_value(response)

            if isinstance(results[mapping.key], str):
                # If the result is only one currency, it will be split into every letter.
                # To avoid this, put it into a list.
                results[mapping.key] = [results[mapping.key]]

        # Check if all dict values do have the same length
        values = list(results.values())
        # Get the max length from all dict values
        len_results = {key: len(value) for key, value in results.items() if hasattr(value, "__iter__")}
        len_results = max(len_results.values()) if bool(len_results) else 1

        if not all(len(value) == len_results for value in values):
            # Update all dict values with equal length
            results.update({k: itertools.repeat(*v, len_results) for k, v in results.items() if len(v) == 1})

        return list(itertools.zip_longest(itertools.repeat(self.name, len_results),
                                          results["currency_pair_first"],
                                          results["currency_pair_second"]))
