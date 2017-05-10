import pandas as pd
from pandas.compat import StringIO, string_types
from ._utils import _init_session, _sanitize_dates


def reverse_pair(s, sep="/"):
    lst = s.split(sep)
    return sep.join([lst[1], lst[0]])


def get_oanda_currency_historical_rates(start, end, quote_currency="USD",
                                        base_currency=None, reversed=True,
                                        session=None):
    session = _init_session(session)
    start, end = _sanitize_dates(start, end)

    url = "http://www.oanda.com/currency/historical-rates/download"
    bam = "bid"  # "ask", "mid"

    if base_currency is None:
        base_currency = ["EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]
    elif isinstance(base_currency, string_types):
        base_currency = [base_currency]

    params = {
        "quote_currency": quote_currency,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "period": "daily",
        "display": "absolute",
        "rate": 0,
        "data_range": "c",
        "price": bam,
        "view": "table",
        "download": "csv"
    }
    for i, _base_currency in enumerate(base_currency):
        params["base_currency_%d" % i] = _base_currency

    response = session.get(url, params=params)
    skiprows = 4
    skipfooter = 4
    usecols = range(len(base_currency) + 1)
    df = pd.read_csv(StringIO(response.text), parse_dates=[0],
                     skiprows=skiprows, skipfooter=skipfooter,
                     usecols=usecols, engine='python')
    df = df.rename(columns={
        "End Date": "Date",
    })
    df = df.set_index("Date")
    df = df[::-1]
    if reversed:
        df.columns = pd.Index(df.columns.map(reverse_pair))
        df = 1 / df
    if len(base_currency) == 1:
        return df.iloc[:, 0]
    else:
        return df
