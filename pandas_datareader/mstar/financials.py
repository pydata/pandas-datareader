import requests
from pandas import DataFrame
from abc import abstractmethod
import re
from datetime import datetime


class BaseFinancialsReader(object):

    def __init__(self, symbols, freq, npds, report, **kwargs):
        self._baseurl = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?"
        self.units = kwargs.get("units", 3)
        self.session = requests.session()
        self.data_buffer = []

        # request arguments

        # Argument value assertions
        assert (kwargs.get("dtype", "A") in ["R", "A"]), ValueError(
            "'dtype' must be either 'R' (R)estated or 'A' (A)ctual")
        assert (kwargs.get("view", "raw") in ["raw", "decimal", "percentage"]), ValueError("invalid value method")
        assert (kwargs.get("units", 3) <= 4), ValueError("units must be value between 1-4. \nAccepted Values:\n"
                                             "1=None, 2=Thousands, 3=Millions, 4=Billions")
        assert (freq in ["A", "Y", "Q"]), ValueError("freq must be 'A'/'Y' or 'Q'")

        # Set required arguments
        if hasattr(symbols, "__iter__"):
            self.symbols = symbols
        elif type(symbols) is str:
            self.symbols = [symbols]
        else:
            raise TypeError("Invalid symbols, must be iterable or string")
        self.report = report
        self.order = kwargs.get("order", "asc")
        self.dtype = kwargs.get("dtype", "A")
        if npds <= 5:
            self.period = 5
            self.npds = npds
            # npds represents the number of columns returned - although API only accepts 5 (or 10 with a paid account)\
            # the end result will be truncated by npds value

        else:
            raise ValueError("invalid number of periods")

        if freq in ["A", "Y"]:
            self.freq = 12
        else:
            self.freq = 3

        # Optional Arguments
        self.view = kwargs.get("view", "raw")
        self.rounding = kwargs.get("rounding", 3)
        self.currency = kwargs.get('curry', "usd")
        self.order = kwargs.get("order", "asc")



    @property
    def baseurl(self):
        return self._baseurl

    def _get_params(self):
        globalp = {"reportType": self.report, "cur":self.currency, "rounding": self.rounding,
                   "view": self.view, "units": self.units, "period": self.freq,
                   "dataType":self.dtype, "columnYear": self.period, "order": self.order}

        return globalp

    def _request(self, params):
        req = self.session.request(method="GET", url=self._baseurl, params=params)
        resp = self.session.get(url=req.url)
        return resp.text

    @abstractmethod
    def _parse_response(self, resp, symbol):
        raise NotImplementedError()

    def _process(self):
        params = self._get_params()
        for s in self.symbols:
            params.update({"t":s})

            resp = self._request(params=params)
            resp = self._parse_response(resp=resp, symbol=s)

            self.data_buffer.extend(resp)


    def _detect_date_row(self, row):
        date_pattern = re.compile(r'[0-9].*\-[0-9].')
        matched = False
        for item in row:
            if date_pattern.match(item) is not None:
                matched = True
            else:
                pass
        return matched

    @abstractmethod
    def read(self):
        raise NotImplementedError

class IncomeStatementReader(BaseFinancialsReader):

    def __init__(self, symbols, freq, npds, **kwargs):
        super().__init__(symbols=symbols, report="is", freq=freq, npds=npds, kwargs=kwargs)
        self.params = self._get_params()

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
        n=0
        for row in splitresp[1:]:
            vals = row.split(",")
            if self._detect_date_row(row=vals):
                if len(self.data_buffer) > 0:
                    if "Symbols" in self.data_buffer[0]:
                        pass
                else:
                    header = ["Symbols","Accounts"]
                    vals = [datetime.strptime(v, "%Y-%m").strftime("%Y-%m-%d") for v in row.split(",")[1:len(row.split(","))-1]]
                    header.extend(vals)
                    header.append("TTM")
                    lines.append(header)
            elif len(vals) == 1:
                pass
            else:

                if vals[0] == "Basic":
                    if n == 0:
                        row = self._special_case_formatter(group="EPS", symbol=symbol, vals=vals)
                        lines.append(row)
                        n += 1
                    elif n == 2:
                        row = self._special_case_formatter(group="Wt Avg ShsOut", symbol=symbol, vals=vals)
                        lines.append(row)
                        n += 1
                elif vals[0] == "Diluted":
                    if n == 1:
                        row = self._special_case_formatter(group="EPS", symbol=symbol, vals=vals)
                        lines.append(row)
                        n += 1
                    elif n == 3:
                        row = self._special_case_formatter(group="Wt Avg ShsOut", symbol=symbol, vals=vals)
                        lines.append(row)
                        n+=1
                else:
                    newrow = [symbol]
                    if vals[0] == '"Sales':
                        newrow.append("SG&A")
                        for v in vals[2:]:
                            if v == "":
                                newrow.append(None)
                            else:
                                newrow.append(float(v))
                    else:
                        newrow.append(vals[0])
                        for v in vals[1:]:
                            if v == "":
                                newrow.append(None)
                            else:
                                newrow.append(float(v))
                    lines.append(newrow)
        return lines

    def read(self):
        d = self._process()
        df = DataFrame(d, columns=self.data_buffer[0]).drop(0)
        df = df.set_index(["Symbols", "Accounts"])
        return df

class BalanceSheetReader(BaseFinancialsReader):

    def __init__(self, symbols, freq, npds, **kwargs):
        super().__init__(symbols=symbols, report="bs", freq=freq, npds=npds, kwargs=kwargs)

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
                    header = ["Symbols", "Accounts"]
                    vals = [datetime.strptime(v, "%Y-%m").strftime("%Y-%m-%d") for v in vals[1:len(vals) - 1]]
                    header.extend(vals)
                    header.append("TTM")
                    lines.append(header)

            elif len(vals) > 2:
                if vals[1].find(' plant and equipment"') > -1:
                    if vals[0].find("Gross") > -1:
                        newrow = [symbol, "Gross PPE"]
                    elif vals[0].find("Net") > -1:
                        newrow = [symbol, "Net PPE"]

                    for v in vals[2:]:
                        if v == "":
                            newrow.append(None)
                        else:
                            newrow.append(float(v))
                else:
                    newrow = [symbol, vals[0]]
                    for v in vals[1:]:
                        if v == "":
                            newrow.append(None)
                        else:
                            newrow.append(float(v))
                lines.append(newrow)
            else:
                pass
        return lines


    def read(self):
        df = DataFrame(data=self.data_buffer[1:], columns=self.data_buffer[0]).set_index(["Symbols", "Accounts"])
        return df

class CashflowStatementReader(BaseFinancialsReader):

    def __init__(self, symbols, freq, npds, **kwargs):
        super().__init__(symbols=symbols, report="cf", freq=freq, npds=npds, kwargs=kwargs)

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
                    header = ["Symbols", "Accounts"]
                    vals = [datetime.strptime(v, "%Y-%m").strftime("%Y-%m-%d") for v in vals[1:len(vals) - 1]]
                    header.extend(vals)
                    header.append("TTM")
                    lines.append(header)
            elif len(vals) > 2:
                if vals[2].find(' and equipment"') > -1:

                    newrow = [symbol, "Investments in PPE"]
                    for v in vals[3:]:
                        if v == "":
                            newrow.append(None)
                        else:
                            newrow.append(float(v))
                elif vals[1].find(' net"') > -1:
                    newrow = [symbol, "Net Acquisitions"]
                    for v in vals[2:]:
                        if v == "":
                            newrow.append(None)
                        else:
                            newrow.append(float(v))
                else:
                    newrow = [symbol, vals[0]]
                    for v in vals[1:]:
                        if v == "":
                            newrow.append(None)
                        else:
                            newrow.append(float(v))
                lines.append(newrow)

            else:
                pass

        return lines


    def read(self):
        df = DataFrame(data=self.data_buffer[1:], columns=self.data_buffer[0]).set_index(["Symbols", "Accounts"])
        return df

class KeyRatiosReader(object):
    """
    Endpoint and parser for Financial Key Ratios values provided by Morningstar
    """

    def __init__(self, symbols):
        """
        Initialize KeyRatios Reader
        
        :param symbols: str or iterable
        :return:
        
        """
        self._baseurl = "http://financials.morningstar.com/ajax/exportKR2CSV.html?"
        self.session = requests.session()
        self.data_buffer = []
        if hasattr(symbols, "__iter__"):
            self.symbols = symbols
        elif type(symbols) == str:
            self.symbols = [symbols]
        else:
            raise TypeError("'symbols' invalid must be iterable or string")

    def _request(self, params):
        """
        Builds and sends http request using specified parameters then 
        returns response object as a string
        
        :param params: dict
        :return: str
        """
        req = self.session.request(method="GET", url=self._baseurl, params=params)
        resp = self.session.get(url=req.url)
        return resp.text

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

    def _format_currency(self, row):
        thou_start = re.compile('"\S*')
        thou_end = re.compile('\d+"')

        newrow = [row[0]]
        vals = row[1:]
        for v in range(len(vals)):
            if vals[v] == "":
                newrow.append(None)
            else:
                if re.match(thou_start, vals[v]) is None:
                    if re.match(thou_end, vals[v]) is None:
                        newrow.append(float(vals[v]))
                else:
                    val = "".join([vals[v], vals[v+1]])
                    val = re.sub('"', "", val)

                    newrow.append(float(val))

        return newrow

    def _format_percent(self, row):
        newrow = [row[0]]
        for r in row[1:]:
            if r == "":
                newrow.append(None)
            else:
                newrow.append(float(r)/100)
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
        row_type = "value"
        if row[0].find("%") > -1:
            row_type = "percent"
        elif row[0].lower().find("usd") > -1 or row[0].lower().find("mil") > -1:
            row_type = "currency"
        return row_type


    def _parse_response(self, resp, symbol):
        """
        Parses http response for symbol and compiles it into an array of nested lists
        
        :param resp: str
        :param symbol: str
        :return: list
        """
        splitlines = resp.split("\n")

        lines = []
        current_category = None
        for l in range(len(splitlines)):
            if l == 2:
                if len(self.data_buffer) > 0:
                    if "Symbols" in self.data_buffer[0]:
                        pass
                else:
                    header = ["Groups","Symbols","Metrics"]
                    vals = [datetime.strptime(v, "%Y-%m").strftime("%Y-%m-%d") for v in splitlines.split(",")[2:len(splitlines.split(","))-1]]
                    header.extend(vals)
                    header.append("TTM")
                    lines.append(header)

            else:
                lx = splitlines[l].split(",")
                if splitlines[l].find("Key Ratios") > -1:
                    current_category = splitlines[l].replace("Key Ratios -> ", "")
                elif current_category is None:
                    current_category = "Financials"
                else:
                    growth_groups = ["Year over Year", "3-Year Average", "5-Year Average", "10-Year Average"]
                    if lx[0] in growth_groups:
                        idx = growth_groups.index(lx[0])
                        group = splitlines[l-(idx+1)]
                        lx.insert(0, ' -- '.join([group, lx[0]]))
                        lx.pop(1)

                    if splitlines[l].find(",") > -1:

                        if self._detect_date_row(row=lx):
                            pass
                        else:

                            if self._detect_row_type(lx) is "percent":
                                formattedline = self._format_percent(row=lx)
                                formattedline.insert(0, current_category)
                                formattedline.insert(0, symbol)
                                lines.append(formattedline)
                            elif self._detect_row_type(lx) == "currency":
                                formattedline = self._format_currency(row=lx)
                                formattedline.insert(0, current_category)
                                formattedline.insert(0, symbol)
                                lines.append(formattedline)
                            elif self._detect_row_type(lx) == "value":
                                formattedline = self._format_value(row=lx)
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
        for s in self.symbols:
            resp = self._request(params={"t":s})
            resp = self._parse_response(resp=resp, symbol=s)
            self.data_buffer.extend(resp)

    def read(self):
        """
        loads response object buffer into DataFrame object and resets indices to 
        "Symbols" and "Groups"
        
        :return: pandas.DataFrame
        """
        df = DataFrame(data=self.data_buffer)
        df.columns = df.loc[0]
        df = df.drop(0, axis=0)
        df = df.set_index(["Symbols", "Groups"])
        return df
