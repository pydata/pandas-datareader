# -*- coding: utf-8 -*-

import warnings

import numpy as np
import pandas as pd

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import lrange, reduce, string_types

# This list of country codes was pulled from wikipedia during October 2014.
# While some exceptions do exist, it is the best proxy for countries supported
# by World Bank.  It is an aggregation of the 2-digit ISO 3166-1 alpha-2, and
# 3-digit ISO 3166-1 alpha-3, codes, with 'all', 'ALL', and 'All' appended ot
# the end.

WB_API_URL = "https://api.worldbank.org/v2"

country_codes = [
    "AD",
    "AE",
    "AF",
    "AG",
    "AI",
    "AL",
    "AM",
    "AO",
    "AQ",
    "AR",
    "AS",
    "AT",
    "AU",
    "AW",
    "AX",
    "AZ",
    "BA",
    "BB",
    "BD",
    "BE",
    "BF",
    "BG",
    "BH",
    "BI",
    "BJ",
    "BL",
    "BM",
    "BN",
    "BO",
    "BQ",
    "BR",
    "BS",
    "BT",
    "BV",
    "BW",
    "BY",
    "BZ",
    "CA",
    "CC",
    "CD",
    "CF",
    "CG",
    "CH",
    "CI",
    "CK",
    "CL",
    "CM",
    "CN",
    "CO",
    "CR",
    "CU",
    "CV",
    "CW",
    "CX",
    "CY",
    "CZ",
    "DE",
    "DJ",
    "DK",
    "DM",
    "DO",
    "DZ",
    "EC",
    "EE",
    "EG",
    "EH",
    "ER",
    "ES",
    "ET",
    "FI",
    "FJ",
    "FK",
    "FM",
    "FO",
    "FR",
    "GA",
    "GB",
    "GD",
    "GE",
    "GF",
    "GG",
    "GH",
    "GI",
    "GL",
    "GM",
    "GN",
    "GP",
    "GQ",
    "GR",
    "GS",
    "GT",
    "GU",
    "GW",
    "GY",
    "HK",
    "HM",
    "HN",
    "HR",
    "HT",
    "HU",
    "ID",
    "IE",
    "IL",
    "IM",
    "IN",
    "IO",
    "IQ",
    "IR",
    "IS",
    "IT",
    "JE",
    "JM",
    "JO",
    "JP",
    "KE",
    "KG",
    "KH",
    "KI",
    "KM",
    "KN",
    "KP",
    "KR",
    "KW",
    "KY",
    "KZ",
    "LA",
    "LB",
    "LC",
    "LI",
    "LK",
    "LR",
    "LS",
    "LT",
    "LU",
    "LV",
    "LY",
    "MA",
    "MC",
    "MD",
    "ME",
    "MF",
    "MG",
    "MH",
    "MK",
    "ML",
    "MM",
    "MN",
    "MO",
    "MP",
    "MQ",
    "MR",
    "MS",
    "MT",
    "MU",
    "MV",
    "MW",
    "MX",
    "MY",
    "MZ",
    "NA",
    "NC",
    "NE",
    "NF",
    "NG",
    "NI",
    "NL",
    "NO",
    "NP",
    "NR",
    "NU",
    "NZ",
    "OM",
    "PA",
    "PE",
    "PF",
    "PG",
    "PH",
    "PK",
    "PL",
    "PM",
    "PN",
    "PR",
    "PS",
    "PT",
    "PW",
    "PY",
    "QA",
    "RE",
    "RO",
    "RS",
    "RU",
    "RW",
    "SA",
    "SB",
    "SC",
    "SD",
    "SE",
    "SG",
    "SH",
    "SI",
    "SJ",
    "SK",
    "SL",
    "SM",
    "SN",
    "SO",
    "SR",
    "SS",
    "ST",
    "SV",
    "SX",
    "SY",
    "SZ",
    "TC",
    "TD",
    "TF",
    "TG",
    "TH",
    "TJ",
    "TK",
    "TL",
    "TM",
    "TN",
    "TO",
    "TR",
    "TT",
    "TV",
    "TW",
    "TZ",
    "UA",
    "UG",
    "UM",
    "US",
    "UY",
    "UZ",
    "VA",
    "VC",
    "VE",
    "VG",
    "VI",
    "VN",
    "VU",
    "WF",
    "WS",
    "YE",
    "YT",
    "ZA",
    "ZM",
    "ZW",
    "ABW",
    "AFG",
    "AGO",
    "AIA",
    "ALA",
    "ALB",
    "AND",
    "ARE",
    "ARG",
    "ARM",
    "ASM",
    "ATA",
    "ATF",
    "ATG",
    "AUS",
    "AUT",
    "AZE",
    "BDI",
    "BEL",
    "BEN",
    "BES",
    "BFA",
    "BGD",
    "BGR",
    "BHR",
    "BHS",
    "BIH",
    "BLM",
    "BLR",
    "BLZ",
    "BMU",
    "BOL",
    "BRA",
    "BRB",
    "BRN",
    "BTN",
    "BVT",
    "BWA",
    "CAF",
    "CAN",
    "CCK",
    "CHE",
    "CHL",
    "CHN",
    "CIV",
    "CMR",
    "COD",
    "COG",
    "COK",
    "COL",
    "COM",
    "CPV",
    "CRI",
    "CUB",
    "CUW",
    "CXR",
    "CYM",
    "CYP",
    "CZE",
    "DEU",
    "DJI",
    "DMA",
    "DNK",
    "DOM",
    "DZA",
    "ECU",
    "EGY",
    "ERI",
    "ESH",
    "ESP",
    "EST",
    "ETH",
    "FIN",
    "FJI",
    "FLK",
    "FRA",
    "FRO",
    "FSM",
    "GAB",
    "GBR",
    "GEO",
    "GGY",
    "GHA",
    "GIB",
    "GIN",
    "GLP",
    "GMB",
    "GNB",
    "GNQ",
    "GRC",
    "GRD",
    "GRL",
    "GTM",
    "GUF",
    "GUM",
    "GUY",
    "HKG",
    "HMD",
    "HND",
    "HRV",
    "HTI",
    "HUN",
    "IDN",
    "IMN",
    "IND",
    "IOT",
    "IRL",
    "IRN",
    "IRQ",
    "ISL",
    "ISR",
    "ITA",
    "JAM",
    "JEY",
    "JOR",
    "JPN",
    "KAZ",
    "KEN",
    "KGZ",
    "KHM",
    "KIR",
    "KNA",
    "KOR",
    "KWT",
    "LAO",
    "LBN",
    "LBR",
    "LBY",
    "LCA",
    "LIE",
    "LKA",
    "LSO",
    "LTU",
    "LUX",
    "LVA",
    "MAC",
    "MAF",
    "MAR",
    "MCO",
    "MDA",
    "MDG",
    "MDV",
    "MEX",
    "MHL",
    "MKD",
    "MLI",
    "MLT",
    "MMR",
    "MNE",
    "MNG",
    "MNP",
    "MOZ",
    "MRT",
    "MSR",
    "MTQ",
    "MUS",
    "MWI",
    "MYS",
    "MYT",
    "NAM",
    "NCL",
    "NER",
    "NFK",
    "NGA",
    "NIC",
    "NIU",
    "NLD",
    "NOR",
    "NPL",
    "NRU",
    "NZL",
    "OMN",
    "PAK",
    "PAN",
    "PCN",
    "PER",
    "PHL",
    "PLW",
    "PNG",
    "POL",
    "PRI",
    "PRK",
    "PRT",
    "PRY",
    "PSE",
    "PYF",
    "QAT",
    "REU",
    "ROU",
    "RUS",
    "RWA",
    "SAU",
    "SDN",
    "SEN",
    "SGP",
    "SGS",
    "SHN",
    "SJM",
    "SLB",
    "SLE",
    "SLV",
    "SMR",
    "SOM",
    "SPM",
    "SRB",
    "SSD",
    "STP",
    "SUR",
    "SVK",
    "SVN",
    "SWE",
    "SWZ",
    "SXM",
    "SYC",
    "SYR",
    "TCA",
    "TCD",
    "TGO",
    "THA",
    "TJK",
    "TKL",
    "TKM",
    "TLS",
    "TON",
    "TTO",
    "TUN",
    "TUR",
    "TUV",
    "TWN",
    "TZA",
    "UGA",
    "UKR",
    "UMI",
    "URY",
    "USA",
    "UZB",
    "VAT",
    "VCT",
    "VEN",
    "VGB",
    "VIR",
    "VNM",
    "VUT",
    "WLF",
    "WSM",
    "YEM",
    "ZAF",
    "ZMB",
    "ZWE",
    "all",
    "ALL",
    "All",
]


class WorldBankReader(_BaseReader):

    """
    Download data series from the World Bank's World Development Indicators

    Parameters
    ----------
    symbols: WorldBank indicator string or list of strings
        taken from the ``id`` field in ``WDIsearch()``
    countries: string or list of strings.
        ``all`` downloads data for all countries
        2 or 3 character ISO country codes select individual
        countries (e.g.``US``,``CA``) or (e.g.``USA``,``CAN``).  The codes
        can be mixed.
        The two ISO lists of countries, provided by wikipedia, are hardcoded
        into pandas as of 11/10/2014.
    start : string, int, date, datetime, Timestamp
        First year of the data series. Month and day are ignored.
    end : string, int, date, datetime, Timestamp
        Last year of the data series (inclusive). Month and day are ignored.
    errors: str {'ignore', 'warn', 'raise'}, default 'warn'
        Country codes are validated against a hardcoded list.  This controls
        the outcome of that validation, and attempts to also apply
        to the results from world bank.
        errors='raise', will raise a ValueError on a bad country code.
    """

    _format = "json"

    def __init__(
        self,
        symbols=None,
        countries=None,
        start=None,
        end=None,
        freq=None,
        retry_count=3,
        pause=0.1,
        session=None,
        errors="warn",
    ):

        if symbols is None:
            symbols = ["NY.GDP.MKTP.CD", "NY.GNS.ICTR.ZS"]
        elif isinstance(symbols, string_types):
            symbols = [symbols]

        super(WorldBankReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )

        if countries is None:
            countries = ["MX", "CA", "US"]
        elif isinstance(countries, string_types):
            countries = [countries]

        bad_countries = np.setdiff1d(countries, country_codes)
        # Validate the input
        if len(bad_countries) > 0:
            tmp = ", ".join(bad_countries)
            if errors == "raise":
                raise ValueError("Invalid Country Code(s): %s" % tmp)
            if errors == "warn":
                warnings.warn(
                    "Non-standard ISO " "country codes: %s" % tmp, UserWarning
                )

        freq_symbols = ["M", "Q", "A", None]

        if freq not in freq_symbols:
            msg = "The frequency `{0}` is not in the accepted " "list.".format(freq)
            raise ValueError(msg)

        self.freq = freq
        self.countries = countries
        self.errors = errors

    @property
    def url(self):
        """API URL"""
        countries = ";".join(self.countries)
        return WB_API_URL + "/countries/" + countries + "/indicators/"

    @property
    def params(self):
        """Parameters to use in API calls"""
        if self.freq == "M":
            return {
                "date": "{0}M{1:02d}:{2}M{3:02d}".format(
                    self.start.year, self.start.month, self.end.year, self.end.month
                ),
                "per_page": 25000,
                "format": "json",
            }
        elif self.freq == "Q":
            return {
                "date": "{0}Q{1}:{2}Q{3}".format(
                    self.start.year, self.start.quarter, self.end.year, self.end.quarter
                ),
                "per_page": 25000,
                "format": "json",
            }
        else:
            return {
                "date": "{0}:{1}".format(self.start.year, self.end.year),
                "per_page": 25000,
                "format": "json",
            }

    def read(self):
        """Read data"""
        try:
            return self._read()
        finally:
            self.close()

    def _read(self):
        data = []
        for indicator in self.symbols:
            # Build URL for api call
            try:
                df = self._read_one_data(self.url + indicator, self.params)
                df.columns = ["country", "iso_code", "year", indicator]
                data.append(df)

            except ValueError as e:
                msg = str(e) + " Indicator: " + indicator
                if self.errors == "raise":
                    raise ValueError(msg)
                elif self.errors == "warn":
                    warnings.warn(msg)

        # Confirm we actually got some data, and build Dataframe
        if len(data) > 0:
            out = reduce(lambda x, y: x.merge(y, how="outer"), data)
            out = out.drop("iso_code", axis=1)
            out = out.set_index(["country", "year"])
            out = out.apply(pd.to_numeric, errors="ignore")

            return out
        else:
            msg = "No indicators returned data."
            raise ValueError(msg)

    def _read_lines(self, out):
        # Check to see if there is a possible problem
        possible_message = out[0]

        if "message" in possible_message.keys():
            msg = possible_message["message"][0]
            try:
                msg = msg["key"].split() + ["\n "] + msg["value"].split()
                wb_err = " ".join(msg)
            except Exception:
                wb_err = ""
                if "key" in msg.keys():
                    wb_err = msg["key"] + "\n "
                if "value" in msg.keys():
                    wb_err += msg["value"]

            msg = "Problem with a World Bank Query \n %s." % wb_err
            raise ValueError(msg)

        if "total" in possible_message.keys():
            if possible_message["total"] == 0:
                msg = "No results found from world bank."
                raise ValueError(msg)

        # Parse JSON file
        data = out[1]
        country = [x["country"]["value"] for x in data]
        iso_code = [x["country"]["id"] for x in data]
        year = [x["date"] for x in data]
        value = [x["value"] for x in data]
        # Prepare output
        df = pd.DataFrame([country, iso_code, year, value]).T
        return df

    def get_countries(self):
        """Query information about countries

        Notes
        -----
        Provides information such as:

          * country code
          * region
          * income level
          * capital city
          * latitude
          * and longitude

        """
        url = WB_API_URL + "/countries/?per_page=1000&format=json"

        resp = self._get_response(url)
        data = resp.json()[1]

        data = pd.DataFrame(data)
        data.adminregion = [x["value"] for x in data.adminregion]
        data.incomeLevel = [x["value"] for x in data.incomeLevel]
        data.lendingType = [x["value"] for x in data.lendingType]
        data.region = [x["value"] for x in data.region]
        data.latitude = [float(x) if x != "" else np.nan for x in data.latitude]
        data.longitude = [float(x) if x != "" else np.nan for x in data.longitude]
        data = data.rename(columns={"id": "iso3c", "iso2Code": "iso2c"})
        return data

    def get_indicators(self):
        """Download information about all World Bank data series"""
        global _cached_series
        if isinstance(_cached_series, pd.DataFrame):
            return _cached_series.copy()

        url = WB_API_URL + "/indicators?per_page=50000&format=json"

        resp = self._get_response(url)
        data = resp.json()[1]

        data = pd.DataFrame(data)
        # Clean fields
        data.source = [x["value"] for x in data.source]

        def encode_ascii(x):
            return x.encode("ascii", "ignore")

        data.sourceOrganization = data.sourceOrganization.apply(encode_ascii)
        # Clean topic field

        def get_value(x):
            try:
                return x["value"]
            except Exception:
                return ""

        def get_list_of_values(x):
            return [get_value(y) for y in x]

        data.topics = data.topics.apply(get_list_of_values)
        data.topics = data.topics.apply(lambda x: " ; ".join(x))

        # Clean output
        data = data.sort_values(by="id")
        data.index = pd.Index(lrange(data.shape[0]))

        # cache
        _cached_series = data.copy()

        return data

    def search(self, string="gdp.*capi", field="name", case=False):
        """
        Search available data series from the world bank

        Parameters
        ----------
        string: string
            regular expression
        field: string
            id, name, source, sourceNote, sourceOrganization, topics
            See notes below
        case: bool
            case sensitive search?

        Notes
        -----
        The first time this function is run it will download and cache the full
        list of available series. Depending on the speed of your network
        connection, this can take time. Subsequent searches will use the cached
        copy, so they should be much faster.

        id : Data series indicator (for use with the ``indicator`` argument of
        ``WDI()``) e.g. NY.GNS.ICTR.GN.ZS"
        name: Short description of the data series
        source: Data collection project
        sourceOrganization: Data collection organization
        note:
        sourceNote:
        topics:
        """
        indicators = self.get_indicators()
        data = indicators[field]
        idx = data.str.contains(string, case=case)
        out = indicators.loc[idx].dropna()
        return out


def download(
    country=None,
    indicator=None,
    start=2003,
    end=2005,
    freq=None,
    errors="warn",
    **kwargs
):
    """
    Download data series from the World Bank's World Development Indicators

    Parameters
    ----------
    indicator: string or list of strings
        taken from the ``id`` field in ``WDIsearch()``
    country: string or list of strings.
        ``all`` downloads data for all countries
        2 or 3 character ISO country codes select individual
        countries (e.g.``US``,``CA``) or (e.g.``USA``,``CAN``).  The codes
        can be mixed.

        The two ISO lists of countries, provided by wikipedia, are hardcoded
        into pandas as of 11/10/2014.
    start: int
        First year of the data series
    end: int
        Last year of the data series (inclusive)
    freq: str
        frequency or periodicity of the data to be retrieved (e.g. 'M' for
        monthly, 'Q' for quarterly, and 'A' for annual). None defaults to
        annual.
    errors: str {'ignore', 'warn', 'raise'}, default 'warn'
        Country codes are validated against a hardcoded list.  This controls
        the outcome of that validation, and attempts to also apply
        to the results from world bank.
        errors='raise', will raise a ValueError on a bad country code.
    kwargs:
        keywords passed to WorldBankReader

    Returns
    -------
    data : DataFrame
        DataFrame with columns country, year, indicator value
    """
    return WorldBankReader(
        symbols=indicator,
        countries=country,
        start=start,
        end=end,
        freq=freq,
        errors=errors,
        **kwargs
    ).read()


def get_countries(**kwargs):
    """Query information about countries

    Provides information such as:
        country code, region, income level,
        capital city, latitude, and longitude

    Parameters
    ----------

    kwargs:
        keywords passed to WorldBankReader

    """
    return WorldBankReader(**kwargs).get_countries()


def get_indicators(**kwargs):
    """Download information about all World Bank data series

    Parameters
    ----------

    kwargs:
        keywords passed to WorldBankReader

    """
    return WorldBankReader(**kwargs).get_indicators()


_cached_series = None


def search(string="gdp.*capi", field="name", case=False, **kwargs):
    """
    Search available data series from the world bank

    Parameters
    ----------
    string: string
        regular expression
    field: string
        id, name, source, sourceNote, sourceOrganization, topics. See notes
    case: bool
        case sensitive search?
    kwargs:
        keywords passed to WorldBankReader

    Notes
    -----
    The first time this function is run it will download and cache the full
    list of available series. Depending on the speed of your network
    connection, this can take time. Subsequent searches will use the cached
    copy, so they should be much faster.

    id : Data series indicator (for use with the ``indicator`` argument of
    ``WDI()``) e.g. NY.GNS.ICTR.GN.ZS"

      * name: Short description of the data series
      * source: Data collection project
      * sourceOrganization: Data collection organization
      * note:
      * sourceNote:
      * topics:
    """

    return WorldBankReader(**kwargs).search(string=string, field=field, case=case)
