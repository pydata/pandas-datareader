from pandas import DataFrame
from pandas.io.common import urlopen

from pandas_datareader.exceptions import (
    DEP_ERROR_MSG,
    ImmediateDeprecationError,
)

_URL = "http://download.finance.yahoo.com/d/quotes.csv?"


def _get_data(idx_sym):  # pragma: no cover
    """
    Returns DataFrame containing list of component information for
    index represented in idx_sym from yahoo. Includes component symbol
    (ticker), exchange, and name.

    Parameters
    ----------
    idx_sym : str
        Stock index symbol
        Examples:
        '^DJI' (Dow Jones Industrial Average)
        '^NYA' (NYSE Composite)
        '^IXIC' (NASDAQ Composite)

        See: http://finance.yahoo.com/indices for other index symbols

    Returns
    -------
    idx_df : DataFrame
    """
    raise ImmediateDeprecationError(DEP_ERROR_MSG.format("Yahoo Components"))
    stats = "snx"
    # URL of form:
    # http://download.finance.yahoo.com/d/quotes.csv?s=@%5EIXIC&f=snxl1d1t1c1ohgv
    url = _URL + "s={0}&f={1}&e=.csv&h={2}"

    idx_mod = idx_sym.replace("^", "@%5E")
    url_str = url.format(idx_mod, stats, 1)

    idx_df = DataFrame()
    mask = [True]
    comp_idx = 1

    # LOOP across component index structure,
    # break when no new components are found
    while True in mask:
        url_str = url.format(idx_mod, stats, comp_idx)
        with urlopen(url_str) as resp:
            raw = resp.read()
        lines = raw.decode("utf-8").strip().strip('"').split('"\r\n"')
        lines = [line.strip().split('","') for line in lines]

        temp_df = DataFrame(lines, columns=["ticker", "name", "exchange"])
        temp_df = temp_df.drop_duplicates()
        temp_df = temp_df.set_index("ticker")
        mask = ~temp_df.index.isin(idx_df.index)

        comp_idx = comp_idx + 50
        idx_df = idx_df.append(temp_df[mask])

    return idx_df
