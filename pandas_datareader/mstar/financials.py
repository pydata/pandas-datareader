import re
from abc import abstractmethod
from datetime import datetime
from time import sleep

import requests
from pandas import DataFrame, MultiIndex

from pandas_datareader._utils import RemoteDataError


class BaseFinancialsReader(object):

    def __init__(self, symbols, freq, report, **kwargs):
        self._baseurl = "http://financials.morningstar.com/ajax/" \
                        "ReportProcess4CSV.html?"
        self.session = kwargs.get("session", requests.session())
        self.pause = kwargs.get("pause", .001)
        self.retries = kwargs.get('retry_count', 3)
        self.retry_list = []
        self.data_buffer = []
        self.header = []

        if type(symbols) is list:
            self.symbols = symbols
        elif type(symbols) is str:
            self.symbols = [symbols]
        else:
            raise TypeError("Invalid symbols, must be iterable or string")

        if len(report) != 2:
            raise AttributeError(
                "Invalid report type. Must be in ['is','bs','cf']")
        elif type(report) is not str:
            raise TypeError("Invalid report type. Must be string.")
        else:
            self.report = report

        if freq in ["A", "Y"]:
            self.freq = 12
        elif freq == "Q":
            self.freq = 3
        else:
            raise ValueError("Invalid period. Must be either 'A/Y' or 'Q'")

        self.request_params = self.url_params()
        self.request_params.update(**self._check_optional_kwargs(**kwargs))

    def _check_optional_kwargs(self, **kwargs):
        valid_kwargs = ['view', 'rounding', 'currency',
                        'order', 'units', 'dataType', 'periods']
        invalid_kwargs = [k for k in kwargs.keys() if k not in valid_kwargs]
        if len(invalid_kwargs) > 0:
            raise AttributeError("%s arguments are invalid." % invalid_kwargs)

        # Set required arguments

        if ("rounding" in kwargs.keys() and type(
                kwargs.get("rounding")) is not int):
            raise TypeError("Invalid rounding argument. rounding digits must "
                            "be int")

        if "periods" in kwargs.keys() and kwargs.get("periods") > 10:
            raise ValueError("Max number of periods is 10 when using a "
                             "free account")

        if ("dataType" in kwargs.keys() and
                kwargs.get("dataType") not in ["A", "R"]):
            raise ValueError("Invalid dtype. Must be either 'A'ctual "
                             "or 'R'estated")

        if ("view" in kwargs.keys() and
                kwargs.get("view") not in ["raw", "decimal", "percentage"]):
            raise AttributeError("invalid view method, must be ('raw', "
                                 "'decimal' or 'percentage')")

        if "units" in kwargs.keys():
            if kwargs.get("units") not in [1, 2, 3, 4]:
                raise ValueError(
                    "Invalid units. Must be int value between 1-4. "
                    "\nAccepted Values:\n"
                    "1=None, 2=Thousands, 3=Millions, 4=Billions")
            elif type(kwargs.get("units")) is not int:
                raise TypeError("Invalid units. Must be int value between 1-4."
                                "\nAccepted Values:\n"
                                "1=None, 2=Thousands, 3=Millions, 4=Billions")

        if "order" in kwargs.keys():
            if kwargs.get("order") not in ["asc", "desc"]:
                raise ValueError("Invalid ordering argument must be either, "
                                 "'asc' or 'desc'")
            elif type(kwargs.get("order")) is not str:
                raise TypeError("Invalid ordering argument must be either, "
                                "'asc' or 'desc'")

        if "curry" in kwargs.keys():
            if type(kwargs.get("curry")) is not str:
                raise TypeError("Invalid currency agrument, must be string")
            else:
                kwargs.update({"cur": kwargs.pop("curry")})

        return dict(**kwargs)

    @property
    def baseurl(self):
        return self._baseurl

    def _set_header(self, row):
        self.header.extend(["Symbol", "Account"])
        dates = [datetime.strptime(v, "%Y-%m").strftime("%Y-%m-%d") for v in
                 row[1:len(row)]]
        self.header.extend(dates)
        if self.request_params["dataType"] == "A":
            if self.request_params["order"] == "desc":
                self.header.insert(2, "TTM")
            else:
                self.header.append("TTM")

    def url_params(self):
        url_params = {"reportType": self.report, "cur": "usd", "rounding": 3,
                      "view": "raw", "units": 1, "period": self.freq,
                      "dataType": "R", "columnYear": 5, "order": "asc"}

        if self.freq in ["A", "Y"] and url_params.get("columnYear") > 5:
            raise ValueError("periods restricted to max 5 for annual data "
                             "unless using paid account")
        elif self.freq == "Q" and url_params.get("columnYear") > 10:
            raise ValueError("periods restricted to max 10 for annual data "
                             "unless using paid account")
        else:

            return url_params

    def _request(self, params):
        req = self.session.request(method="GET", url=self._baseurl,
                                   params=params)
        resp = self.session.get(url=req.url)
        if resp.status_code == 200:
            if len(resp.text) > 0:
                return resp.text
            else:
                print("Error in trying to collect data for %s" %
                      self.request_params["t"])
                if len(self.retries) == 0:
                    print("Out of retries...Skipping...")
                else:
                    self.retry_list.append(self.request_params)

    @abstractmethod
    def _parse_response(self, resp, symbol):
        raise NotImplementedError()

    def _process(self):
        for s in self.symbols:
            self.request_params.update({"t": s})
            resp = self._request(params=self.request_params)
            resp = self._parse_response(resp=resp, symbol=s)
            self.data_buffer.extend(resp)
            sleep(self.pause)
        if len(self.data_buffer) == 0:
            raise RemoteDataError("No data found using morningstar")

    def _detect_date_row(self, row):
        date_pattern = re.compile(r'[0-9].*\-[0-9].')
        matched = False
        for item in row:
            if date_pattern.match(item) is not None:
                matched = True
            else:
                pass
        return matched

    def read(self):
        self._process()
        if len(self.data_buffer) == 0:
            raise RemoteDataError("No data found using morningstar")
        else:
            df = DataFrame([d[2:] for d in self.data_buffer],
                           index=MultiIndex.from_tuples(
                               [(d[0], d[1]) for d in self.data_buffer],
                               names=["Symbol", "Account"]),
                           columns=self.header[2:])
            return df


class IncomeStatementReader(BaseFinancialsReader):

    def __init__(self, symbols, freq, **kwargs):
        super().__init__(symbols=symbols, report="is", freq=freq, **kwargs)

    def _special_case_formatter(self, group, symbol, vals):
        newrow = [symbol, " -- ".join([group, vals[0]])]
        for v in vals[1:]:
            if v == "":
                newrow.append(None)
            else:
                newrow.append(float(v))
        return newrow

    def _parse_response(self, resp, symbol):
        splitresp = str(resp).splitlines()
        lines = []
        n = 0
        for l in splitresp[1:]:
            vals = l.split(",")
            if self._detect_date_row(row=vals):
                if len(self.data_buffer) > 0:
                    pass
                else:
                    self._set_header(row=vals)

            elif len(vals) == 1:
                pass
            else:

                if vals[0] == "Basic":
                    if n == 0:
                        row = self._special_case_formatter(group="EPS",
                                                           symbol=symbol,
                                                           vals=vals)
                        lines.append(row)
                        n += 1
                    elif n == 2:
                        row = self._special_case_formatter(
                            group="Wt Avg ShsOut", symbol=symbol,
                            vals=vals)
                        lines.append(row)
                        n += 1
                elif vals[0] == "Diluted":
                    if n == 1:
                        row = self._special_case_formatter(group="EPS",
                                                           symbol=symbol,
                                                           vals=vals)
                        lines.append(row)
                        n += 1
                    elif n == 3:
                        row = self._special_case_formatter(
                            group="Wt Avg ShsOut", symbol=symbol,
                            vals=vals)
                        lines.append(row)
                        n += 1
                else:
                    newrow = [symbol]
                    if l.find('"Sales, General') > -1:

                        newrow.append("SG&A")
                        for v in vals[2:]:
                            if v == "":
                                newrow.append(0)
                            else:
                                try:
                                    v = float(v)
                                except ValueError:
                                    pass
                                else:
                                    newrow.append(v)
                    else:
                        newrow.append(vals[0])
                        for v in vals[1:]:
                            if v == "":
                                newrow.append(0)
                            else:
                                try:
                                    v = float(v)
                                except ValueError:
                                    pass
                                else:
                                    newrow.append(v)
                    lines.append(newrow)
        return lines


class BalanceSheetReader(BaseFinancialsReader):

    def __init__(self, symbols, freq, **kwargs):
        super().__init__(symbols=symbols, report="bs", freq=freq, **kwargs)

    def _parse_response(self, resp, symbol):
        resp = str(resp).splitlines()
        lines = []
        for l in resp[1:]:
            vals = l.split(",")
            if self._detect_date_row(row=vals):
                if len(self.data_buffer) > 0:
                    if "Symbol" in self.data_buffer:
                        pass
                else:
                    self._set_header(row=vals)

            elif len(vals) > 2:
                if vals[1].find(' plant and equipment"') > -1:
                    if vals[0].find("Gross") > -1:
                        newrow = [symbol, "Gross PPE"]
                    elif vals[0].find("Net") > -1:
                        newrow = [symbol, "Net PPE"]

                    for v in vals[2:]:
                        if v == "":
                            newrow.append(0)
                        else:
                            try:
                                v = float(v)
                            except ValueError:
                                pass
                            else:
                                newrow.append(v)
                else:
                    newrow = [symbol, vals[0]]
                    for v in vals[1:]:
                        if v == "":
                            newrow.append(0)
                        else:
                            try:
                                v = float(v)
                            except ValueError:
                                pass
                            else:
                                newrow.append(v)
                lines.append(newrow)
            else:
                pass
        return lines


class CashflowStatementReader(BaseFinancialsReader):

    def __init__(self, symbols, freq, **kwargs):
        super().__init__(symbols=symbols, report="cf", freq=freq, **kwargs)

    def _parse_response(self, resp, symbol):
        resp = str(resp).splitlines()
        lines = []
        for l in resp[1:]:
            vals = l.split(",")
            if self._detect_date_row(row=vals):
                if len(self.data_buffer) > 0:
                    if "Symbols" in self.data_buffer:
                        pass
                else:
                    self._set_header(row=vals)
            elif len(vals) > 2:
                if l.find('"Investments in property') > -1:

                    newrow = [symbol, "Investments in PPE"]
                    for v in vals[3:]:
                        if v == "":
                            newrow.append(0)
                        else:
                            try:
                                v = float(v)
                            except ValueError:
                                pass
                            else:
                                newrow.append(v)
                elif vals[2].find(' and equipment reductions"') > -1:
                    newrow = [symbol, "PPE reductions"]
                    for v in vals[3:]:
                        if v == "":
                            newrow.append(0)
                        else:
                            try:
                                v = float(v)
                            except ValueError:
                                pass
                            else:
                                newrow.append(v)

                elif vals[1].find(' net"') > -1:
                    newrow = [symbol, "Net Acquisitions"]
                    for v in vals[2:]:
                        if v == "":
                            newrow.append(0)
                        else:
                            newrow.append(float(v))
                else:
                    newrow = [symbol, vals[0]]
                    for v in vals[1:]:
                        if v == "":
                            newrow.append(0)
                        else:
                            try:
                                v = float(v)
                            except ValueError:
                                pass
                            else:
                                newrow.append(v)
                lines.append(newrow)

            else:
                pass

        return lines


class KeyRatiosReader(object):
    """
    Endpoint and parser for Financial Key Ratios values provided by Morningstar
    """

    def __init__(self, symbols, **kwargs):
        """
        Initialize KeyRatios Reader

        :param symbols: str or iterable
        :return:

        """
        self._baseurl = "http://financials.morningstar.com/ajax/" \
                        "exportKR2CSV.html?"
        self.session = kwargs.get("session", requests.session())
        self.data_buffer = []
        if type(symbols) == list:
            self.symbols = symbols
        elif type(symbols) == str:
            self.symbols = [symbols]
        else:
            raise TypeError("'symbols' invalid must be iterable or string")
        self.header = []

    def _request(self, params):
        """
        Builds and sends http request using specified parameters then
        returns response object as a string

        :param params: dict
        :return: str
        """
        req = self.session.request(method="GET", url=self._baseurl,
                                   params=params)
        resp = self.session.get(url=req.url)
        if resp.status_code == 200:
            if len(resp.text) > 0:
                return resp.text
            else:
                print("Symbol %s Invalid! Skipping..." % params["t"])

    def _detect_date_row(self, row):
        """


        :param row: list
        :return: boolean
        """
        date_pattern = re.compile(r'[0-9].*\-[0-9].')
        matched = False
        for item in row:
            if date_pattern.match(item) is not None:
                matched = True
            else:
                pass
        return matched

    def _format_currency(self, row, units=None):
        thou_start = re.compile('"\S*')
        thou_end = re.compile('\d+"')
        ttl = row[0]
        for i in ["USD", "Mil", "*", "Thou", "Bil", "Bln", "$"]:
            ttl = ttl.replace(i, "")
        newrow = [ttl]
        vals = row[1:]
        for v in range(len(vals)):
            if vals[v] == "":
                newrow.append(None)
            else:
                if re.match(thou_start, vals[v]) is None:
                    if re.match(thou_end, vals[v]) is None:
                        newrow.append(float(vals[v]))
                else:
                    val = "".join([vals[v], vals[v + 1]])
                    val = re.sub('"', "", val)
                    val = float(val)
                    if units == "millions":
                        v = val * 1000000
                    elif units == "billions":
                        v = val * 1000000000
                    elif units == "thousands":
                        v = val * 1000
                    else:
                        v = val
                    newrow.append(round(v, 2))
        return newrow

    def _format_percent(self, row):
        ttl = row[0]
        for i in ["USD", "Mil", "*", "Thou", "Bil", "Bln", "$"]:
            ttl = ttl.replace(i, "")
        newrow = [ttl]
        for r in row[1:]:
            if r == "":
                newrow.append(None)
            else:
                newrow.append(round(float(r) / 100, 4))
        return newrow

    def _format_value(self, row):
        newrow = [row[0]]
        for v in row[1:]:
            if v == "":
                newrow.append(None)
            else:
                newrow.append(float(v))
        return newrow

    def _detect_row_type(self, row):
        row = ",".join(row)
        if row.find("%") > -1:
            row_type = "percent"
            units = "actual"
        elif row.lower().find("usd") > -1:
            row_type = "currency"
            units = "actual"
        elif row.lower().find("mil") > -1:
            row_type = "currency"
            units = "millions"
        elif row.lower().find("bil") > -1 or row.lower().find("bln") > -1:
            row_type = "currency"
            units = "billions"
        elif row.lower().find("thou") > -1:
            row_type = "currency"
            units = "thousands"
        else:
            row_type = "value"
            units = "ratio/index"

        return row_type, units

    def _parse_response(self, resp, symbol):
        """
        Parses http response for symbol into an array of nested lists

        :param resp: str
        :param symbol: str
        :return: list
        """
        splitlines = resp.split("\n")
        lines = []
        current_category = None
        for i in range(len(splitlines)):
            lx = splitlines[i].split(",")

            if i == 2:
                if len(self.data_buffer) > 0:
                    if "Symbol" in self.data_buffer[0]:
                        pass
                else:
                    lx = splitlines[i].split(",")
                    vals = [datetime.strptime(v, "%Y-%m").strftime("%Y-%m-%d")
                            for v in
                            lx[1:len(lx) - 1]]
                    self.header.extend(vals)
                    self.header.append("TTM")

            else:
                if splitlines[i].find("Key Ratios") > -1:
                    current_category = splitlines[i].replace("Key Ratios -> ",
                                                             "")
                elif current_category is None:
                    current_category = "Financials - Summary"
                else:
                    growth_groups = ["Year over Year", "3-Year Average",
                                     "5-Year Average",
                                     "10-Year Average"]
                    if lx[0] in growth_groups:
                        idx = growth_groups.index(lx[0])
                        group = splitlines[i - (idx + 1)]
                        lx.insert(0, ' -- '.join([group, lx[0]]))
                        lx.pop(1)
                    else:
                        if splitlines[i].find(",") > -1:
                            lx = splitlines[i].split(",")
                            if self._detect_date_row(row=lx):
                                pass
                            else:
                                row_type, units = self._detect_row_type(lx)

                                if row_type is "percent":
                                    formattedline = self._format_percent(
                                        row=lx)
                                elif row_type == "currency":
                                    formattedline = self._format_currency(
                                        row=lx, units=units)
                                elif row_type == "value":
                                    formattedline = self._format_value(row=lx)
                                else:
                                    raise ValueError("Invalid row type")

                                formattedline.insert(0, current_category)
                                formattedline.insert(0, symbol)
                                lines.append(formattedline)
        return lines

    def _process(self):
        """
        Applies reponse parser to all symbols specified by user and stores
        the inforation in data buffer

        :return:
        """
        if type(self.symbols) is list:
            for s in self.symbols:
                resp = self._request(params={"t": s})
                resp = self._parse_response(resp=resp, symbol=s)
                self.data_buffer.extend(resp)
        elif type(self.symbols) is str:
            resp = self._request(params={"t": self.symbols})
            resp = self._parse_response(resp=resp, symbol=self.symbols)
            self.data_buffer.extend(resp)
        else:
            raise TypeError("Invalid symbols! must be list or string")

    def _gen_index_tuples(self):
        return MultiIndex.from_tuples(
            [(d[0], d[1], d[2]) for d in self.data_buffer],
            names=["Symbol", "Group", "Metric"])

    def read(self):
        """
        loads response object buffer into DataFrame and resets indices to
        "Symbols" and "Groups"

        :return: pandas.DataFrame
        """
        self._process()
        if len(self.data_buffer) == 0:
            raise IndexError("No Data to Return!")
        else:
            df = DataFrame(data=[d[3:] for d in self.data_buffer],
                           index=self._gen_index_tuples(),
                           columns=self.header)

            return df
