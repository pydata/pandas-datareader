.. _remote_data:

.. currentmodule:: pandas_datareader

.. ipython:: python
   :suppress:

   import pandas as pd

   import numpy as np
   np.set_printoptions(precision=4, suppress=True)

   pd.options.display.max_rows=15


******************
Remote Data Access
******************

.. warning::

    The ``access_key`` keyword argument of ``DataReader`` has been deprecated in favor of ``api_key``.

.. warning::

    Robinhood has been immediately deprecated. Endpoints from this provider have been retired.

.. _remote_data.data_reader:

Functions from :mod:`pandas_datareader.data` and :mod:`pandas_datareader.wb`
extract data from various Internet sources into a pandas DataFrame.
Currently the following sources are supported:

    - :ref:`Tiingo<remote_data.tiingo>`
    - :ref:`IEX<remote_data.iex>`
    - :ref:`Alpha Vantage<remote_data.alphavantage>`
    - :ref:`Enigma<remote_data.enigma>`
    - :ref:`Quandl<remote_data.quandl>`
    - :ref:`St.Louis FED (FRED)<remote_data.fred>`
    - :ref:`Kenneth French's data library<remote_data.ff>`
    - :ref:`World Bank<remote_data.wb>`
    - :ref:`OECD<remote_data.oecd>`
    - :ref:`Eurostat<remote_data.eurostat>`
    - :ref:`Thrift Savings Plan<remote_data.tsp>`
    - :ref:`Nasdaq Trader symbol definitions<remote_data.nasdaq_symbols>`
    - :ref:`Stooq<remote_data.stooq>`
    - :ref:`MOEX<remote_data.moex>`

It should be noted, that various sources support different kinds of data, so not all sources implement the same methods and the data elements returned might also differ.

.. _remote_data.tiingo:

Tiingo
======
`Tiingo <https://www.tiingo.com>`__ is a tracing platform that provides a data
api with historical end-of-day prices on equities, mutual funds and ETFs.
Free registration is required to get an API key.  Free accounts are rate
limited and can access a limited number of symbols (500 at the time of
writing).

.. code-block:: ipython

   In [1]: import os
   In [2]: import pandas_datareader as pdr

   In [3]: df = pdr.get_data_tiingo('GOOG', api_key=os.getenv('TIINGO_API_KEY'))
   In [4]: df.head()
                                   close    high     low     open  volume  adjClose  adjHigh  adjLow  adjOpen  adjVolume  divCash  splitFactor
   symbol date
   GOOG   2014-03-27 00:00:00+00:00  558.46  568.00  552.92  568.000   13100    558.46   568.00  552.92  568.000      13100      0.0          1.0
          2014-03-28 00:00:00+00:00  559.99  566.43  558.67  561.200   41100    559.99   566.43  558.67  561.200      41100      0.0          1.0
          2014-03-31 00:00:00+00:00  556.97  567.00  556.93  566.890   10800    556.97   567.00  556.93  566.890      10800      0.0          1.0
          2014-04-01 00:00:00+00:00  567.16  568.45  558.71  558.710    7900    567.16   568.45  558.71  558.710       7900      0.0          1.0
          2014-04-02 00:00:00+00:00  567.00  604.83  562.19  565.106  146700    567.00   604.83  562.19  565.106     146700      0.0          1.0

.. _remote_data.iex:

IEX
===

.. warning:: Usage of all IEX readers now requires an API key. See
             below for additional information.

The Investors Exchange (IEX) provides a wide range of data through an
`API <https://iexcloud.io/api/docs/>`__.  Historical stock
prices are available for up to 15 years. The usage of these readers requires the publishable API key from IEX Cloud Console, which can be stored in the ``IEX_API_KEY`` environment variable.

.. code-block:: ipython

    In [1]: import pandas_datareader.data as web

    In [2]: from datetime import datetime

    In [3]: start = datetime(2016, 9, 1)

    In [4]: end = datetime(2018, 9, 1)

    In [5]: f = web.DataReader('F', 'iex', start, end)

    In [6]: f.loc['2018-08-31']
    Out[6]:
    open             9.64
    high             9.68
    low              9.40
    close            9.48
    volume    76424884.00
    Name: 2018-08-31, dtype: float64

.. note::

   You must provide an API Key when using IEX. You can do this using
   ``os.environ["IEX_API_KEY"] = "pk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"``
   or by exporting the key before starting the IPython session.

There are additional interfaces to this API that are
directly exposed: tops (`'iex-tops'`) and last (`'iex-lasts'`).
A third interface to the deep API is exposed through
`Deep` class or the `get_iex_book` function.

.. todo:: Execute block when markets are open

.. code-block:: ipython

    import pandas_datareader.data as web
    f = web.DataReader('gs', 'iex-tops')
    f[:10]


.. _remote_data.alphavantage:

Alpha Vantage
=============

`Alpha Vantage <https://www.alphavantage.co/documentation>`__ provides realtime
equities and forex data. Free registration is required to get an API key.

Historical Time Series Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Through the
`Alpha Vantage <https://www.alphavantage.co/documentation>`__ Time Series
endpoints, it is possible to obtain historical equities data for individual
symbols. For daily, weekly, and monthly frequencies, 20+ years of historical data is available. The past 3-5 days of intraday data is also available.

The following endpoints are available:

* ``av-intraday`` - Intraday Time Series
* ``av-daily`` - Daily Time Series
* ``av-daily-adjusted`` - Daily Time Series (Adjusted)
* ``av-weekly`` - Weekly Time Series
* ``av-weekly-adjusted`` - Weekly Time Series (Adjusted)
* ``av-monthly`` - Monthly Time Series
* ``av-monthly-adjusted`` - Monthly Time Series (Adjusted)

.. code-block:: ipython

    In [1]: import os

    In [2]: from datetime import datetime

    In [3]: import pandas_datareader.data as web

    In [4]: f = web.DataReader("AAPL", "av-daily", start=datetime(2017, 2, 9),
       ...:                    end=datetime(2017, 5, 24),
       ...:                    api_key=os.getenv('ALPHAVANTAGE_API_KEY'))

    In [5]: f.loc["2017-02-09"]
    Out[5]:
    open      1.316500e+02
    high      1.324450e+02
    low       1.311200e+02
    close     1.324200e+02
    volume    2.834990e+07
    Name: 2017-02-09, dtype: float64


The top-level function ``get_data_alphavantage`` is also provided. This
function will
return the ``TIME_SERIES_DAILY`` endpoint for the symbol and date range
provided.

Quotes
^^^^^^

`Alpha Vantage <https://www.alphavantage.co/documentation>`__ Batch Stock Quotes
endpoint allows the retrieval of realtime stock quotes for up to 100 symbols at
once. These quotes are accessible through the top-level function
``get_quote_av``.

.. code-block:: ipython

    In [1]: import os

    In [2]: from datetime import datetime

    In [3]: import pandas_datareader.data as web

    In [4]: web.get_quote_av(["AAPL", "TSLA"])
    Out[4]:
             price  volume            timestamp
    symbol
    AAPL    219.87     NaN  2019-09-16 15:59:59
    TSLA    242.80     NaN  2019-09-16 15:59:57



.. note:: Most quotes are only available during market hours.

Forex
^^^^^

`Alpha Vantage <https://www.alphavantage.co/documentation>`__ provides realtime
currency exchange rates (for physical and digital currencies).

To request the exchange rate of physical or digital currencies, simply format
as "FROM/TO" as in "USD/JPY".

.. code-block:: ipython

    In [1]: import os

    In [2]: import pandas_datareader.data as web

    In [3]: f = web.DataReader("USD/JPY", "av-forex",
        ...:                    api_key=os.getenv('ALPHAVANTAGE_API_KEY'))

    In [4]: f
    Out[4]:
                                     USD/JPY
    From_Currency Code                   USD
    From_Currency Name  United States Dollar
    To_Currency Code                     JPY
    To_Currency Name            Japanese Yen
    Exchange Rate               108.17000000
    Last Refreshed       2019-09-17 10:43:36
    Time Zone                            UTC
    Bid Price                   108.17000000
    Ask Price                   108.17000000


Multiple pairs are are allowable:

.. code-block:: ipython

    In [1]: import os

    In [2]: import pandas_datareader.data as web

    In [3]: f = web.DataReader(["USD/JPY", "BTC/CNY"], "av-forex",
       ...:                    api_key=os.getenv('ALPHAVANTAGE_API_KEY'))


    In [4]: f
    Out[4]:
                                     USD/JPY              BTC/CNY
    From_Currency Code                   USD                  BTC
    From_Currency Name  United States Dollar              Bitcoin
    To_Currency Code                     JPY                  CNY
    To_Currency Name            Japanese Yen         Chinese Yuan
    Exchange Rate               108.17000000       72230.38039500
    Last Refreshed       2019-09-17 10:44:35  2019-09-17 10:44:01
    Time Zone                            UTC                  UTC
    Bid Price                   108.17000000       72226.26407700
    Ask Price                   108.17000000       72230.02554000



Sector Performance
^^^^^^^^^^^^^^^^^^

`Alpha Vantage <https://www.alphavantage.co/documentation>`__ provides sector
performances through the top-level function ``get_sector_performance_av``.

.. code-block:: ipython

    In [1]: import os

    In [2]: import pandas_datareader.data as web

    In [3]: web.get_sector_performance_av().head()
    Out[4]:
                     RT      1D      5D      1M     3M     YTD       1Y      3Y       5Y      10Y
    Energy        3.29%   3.29%   4.82%  11.69%  3.37%   9.07%  -15.26%  -7.69%  -32.31%   12.15%
    Real Estate   1.02%   1.02%  -1.39%   1.26%  3.49%  24.95%   16.55%     NaN      NaN      NaN
    Utilities     0.08%   0.08%   0.72%   2.77%  3.72%  18.16%   16.09%  27.95%   48.41%  113.09%
    Industrials  -0.15%  -0.15%   2.42%   8.59%  5.10%  22.70%    0.50%  34.50%   43.53%  183.47%
    Health Care  -0.23%  -0.23%   0.88%   1.91%  0.09%   5.20%   -2.38%  26.37%   43.43%  216.01%



.. _remote_data.enigma:

Econdb
======

`Econdb <https://www.econdb.com>`__ provides economic data from 90+
official statistical agencies. Free API allows access to the complete
Econdb database of time series aggregated into datasets.

.. ipython:: python

    import os
    import pandas_datareader.data as web

    f = web.DataReader('ticker=RGDPUS', 'econdb')
    f.head()

.. _remote_data.econdb:

Enigma
======

Access datasets from `Enigma <https://public.enigma.com>`__,
the world's largest repository of structured public data. Note that the Enigma
URL has changed from `app.enigma.io <https://app.enigma.io>`__ as of release
``0.6.0``, as the old API deprecated.

Datasets are unique identified by the ``uuid4`` at the end of a dataset's web address.
For example, the following code downloads from  `USDA Food Recalls 1996 Data <https://public.enigma.com/datasets/292129b0-1275-44c8-a6a3-2a0881f24fe1>`__.

.. code-block:: ipython

    In [1]: import os

    In [2]: import pandas_datareader as pdr

    In [3]: df = pdr.get_data_enigma('292129b0-1275-44c8-a6a3-2a0881f24fe1', os.getenv('ENIGMA_API_KEY'))

    In [4]: df.columns
    Out[4]:
    Index(['case_number', 'recall_notification_report_number',
           'recall_notification_report_url', 'date_opened', 'date_closed',
           'recall_class', 'press_release', 'domestic_est_number', 'company_name',
           'imported_product', 'foreign_estab_number', 'city', 'state', 'country',
           'product', 'problem', 'description', 'total_pounds_recalled',
           'pounds_recovered'],
          dtype='object')


.. _remote_data.quandl:

Quandl
======

Daily financial data (prices of stocks, ETFs etc.) from
`Quandl <https://www.quandl.com/>`__.
The symbol names consist of two parts: DB name and symbol name.
DB names can be all the
`free ones listed on the Quandl website <https://blog.quandl.com/free-data-on-quandl>`__.
Symbol names vary with DB name; for WIKI (US stocks), they are the common
ticker symbols, in some other cases (such as FSE) they can be a bit strange.
Some sources are also mapped to suitable ISO country codes in the dot suffix
style shown above, currently available for
`BE, CN, DE, FR, IN, JP, NL, PT, UK, US <https://www.quandl.com/search?query=>`__.

As of June 2017, each DB has a different data schema,
the coverage in terms of time range is sometimes surprisingly small, and
the data quality is not always good.

.. code-block:: ipython

    In [1]: import pandas_datareader.data as web

    In [2]: symbol = 'WIKI/AAPL'  # or 'AAPL.US'

    In [3]: df = web.DataReader(symbol, 'quandl', '2015-01-01', '2015-01-05')

    In [4]: df.loc['2015-01-02']
    Out[4]:
                  Open    High     Low   Close      Volume  ...     AdjOpen     AdjHigh      AdjLow    AdjClose   AdjVolume
    Date                                                    ...
    2015-01-02  111.39  111.44  107.35  109.33  53204626.0  ...  105.820966  105.868466  101.982949  103.863957  53204626.0


.. _remote_data.fred:

FRED
====

.. ipython:: python

    import pandas_datareader.data as web
    import datetime
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime(2013, 1, 27)
    gdp = web.DataReader('GDP', 'fred', start, end)
    gdp.loc['2013-01-01']

    # Multiple series:
    inflation = web.DataReader(['CPIAUCSL', 'CPILFESL'], 'fred', start, end)
    inflation.head()


.. _remote_data.ff:

Fama/French
===========

Access datasets from the `Fama/French Data Library
<http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html>`__.
The ``get_available_datasets`` function returns a list of all available datasets.

.. ipython:: python

    from pandas_datareader.famafrench import get_available_datasets
    import pandas_datareader.data as web
    len(get_available_datasets())
    ds = web.DataReader('5_Industry_Portfolios', 'famafrench')
    print(ds['DESCR'])
    ds[4].head()

.. _remote_data.wb:

World Bank
==========

``pandas`` users can easily access thousands of panel data series from the
`World Bank's World Development Indicators <http://data.worldbank.org>`__
by using the ``wb`` I/O functions.

Indicators
^^^^^^^^^^

Either from exploring the World Bank site, or using the search function included,
every world bank indicator is accessible.

For example, if you wanted to compare the Gross Domestic Products per capita in
constant dollars in North America, you would use the ``search`` function:

.. code-block:: python

    In [1]: from pandas_datareader import wb
    In [2]: matches = wb.search('gdp.*capita.*const')


Then you would use the ``download`` function to acquire the data from the World
Bank's servers:

.. code-block:: python

    In [3]: dat = wb.download(indicator='NY.GDP.PCAP.KD', country=['US', 'CA', 'MX'], start=2005, end=2008)

    In [4]: print(dat)
                          NY.GDP.PCAP.KD
    country       year
    Canada        2008  36005.5004978584
                  2007  36182.9138439757
                  2006  35785.9698172849
                  2005  35087.8925933298
    Mexico        2008  8113.10219480083
                  2007  8119.21298908649
                  2006  7961.96818458178
                  2005  7666.69796097264
    United States 2008  43069.5819857208
                  2007  43635.5852068142
                  2006   43228.111147107
                  2005  42516.3934699993

The resulting dataset is a properly formatted ``DataFrame`` with a hierarchical
index, so it is easy to apply ``.groupby`` transformations to it:

.. code-block:: python

    In [6]: dat['NY.GDP.PCAP.KD'].groupby(level=0).mean()
    Out[6]:
    country
    Canada           35765.569188
    Mexico            7965.245332
    United States    43112.417952
    dtype: float64

Now imagine you want to compare GDP to the share of people with cellphone
contracts around the world.

.. code-block:: python

    In [7]: wb.search('cell.*%').iloc[:,:2]
    Out[7]:
                         id                                               name
    3990  IT.CEL.SETS.FE.ZS  Mobile cellular telephone users, female (% of ...
    3991  IT.CEL.SETS.MA.ZS  Mobile cellular telephone users, male (% of po...
    4027      IT.MOB.COV.ZS  Population coverage of mobile cellular telepho...

Notice that this second search was much faster than the first one because
``pandas`` now has a cached list of available data series.

.. code-block:: python

    In [13]: ind = ['NY.GDP.PCAP.KD', 'IT.MOB.COV.ZS']
    In [14]: dat = wb.download(indicator=ind, country='all', start=2011, end=2011).dropna()
    In [15]: dat.columns = ['gdp', 'cellphone']
    In [16]: print(dat.tail())
                            gdp  cellphone
    country   year
    Swaziland 2011  2413.952853       94.9
    Tunisia   2011  3687.340170      100.0
    Uganda    2011   405.332501      100.0
    Zambia    2011   767.911290       62.0
    Zimbabwe  2011   419.236086       72.4

Finally, we use the ``statsmodels`` package to assess the relationship between
our two variables using ordinary least squares regression. Unsurprisingly,
populations in rich countries tend to use cellphones at a higher rate:

.. code-block:: python

    In [17]: import numpy as np
    In [18]: import statsmodels.formula.api as smf
    In [19]: mod = smf.ols('cellphone ~ np.log(gdp)', dat).fit()
    In [20]: print(mod.summary())
                                OLS Regression Results
    ==============================================================================
    Dep. Variable:              cellphone   R-squared:                       0.297
    Model:                            OLS   Adj. R-squared:                  0.274
    Method:                 Least Squares   F-statistic:                     13.08
    Date:                Thu, 25 Jul 2013   Prob (F-statistic):            0.00105
    Time:                        15:24:42   Log-Likelihood:                -139.16
    No. Observations:                  33   AIC:                             282.3
    Df Residuals:                      31   BIC:                             285.3
    Df Model:                           1
    ===============================================================================
                      coef    std err          t      P>|t|      [95.0% Conf. Int.]
    -------------------------------------------------------------------------------
    Intercept      16.5110     19.071      0.866      0.393       -22.384    55.406
    np.log(gdp)     9.9333      2.747      3.616      0.001         4.331    15.535
    ==============================================================================
    Omnibus:                       36.054   Durbin-Watson:                   2.071
    Prob(Omnibus):                  0.000   Jarque-Bera (JB):              119.133
    Skew:                          -2.314   Prob(JB):                     1.35e-26
    Kurtosis:                      11.077   Cond. No.                         45.8
    ==============================================================================

Country Codes
^^^^^^^^^^^^^

The ``country`` argument accepts a string or list of mixed
`two <http://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>`__ or `three <http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3>`__ character
ISO country codes, as well as dynamic `World Bank exceptions <https://datahelpdesk.worldbank.org/knowledgebase/articles/898590-api-country-queries>`__ to the ISO standards.

For a list of the the hard-coded country codes (used solely for error handling logic) see ``pandas_datareader.wb.country_codes``.

Problematic Country Codes & Indicators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

   The World Bank's country list and indicators are dynamic. As of 0.15.1,
   :func:`wb.download()` is more flexible.  To achieve this, the warning
   and exception logic changed.

The world bank converts some country codes,
in their response, which makes error checking by pandas difficult.
Retired indicators still persist in the search.

Given the new flexibility of 0.15.1, improved error handling by the user
may be necessary for fringe cases.

To help identify issues:

There are at least 4 kinds of country codes:

1. Standard (2/3 digit ISO) - returns data, will warn and error properly.
2. Non-standard (WB Exceptions) - returns data, but will falsely warn.
3. Blank - silently missing from the response.
4. Bad - causes the entire response from WB to fail, always exception inducing.

There are at least 3 kinds of indicators:

1. Current - Returns data.
2. Retired - Appears in search results, yet won't return data.
3. Bad - Will not return data.

Use the ``errors`` argument to control warnings and exceptions.  Setting
errors to ignore or warn, won't stop failed responses.  (ie, 100% bad
indicators, or a single 'bad' (#4 above) country code).

See docstrings for more info.

.. _remote_data.oecd:

OECD
====

`OECD Statistics <http://stats.oecd.org/>`__ are available via ``DataReader``.
You have to specify OECD's data set code.

To confirm data set code, access to ``each data -> Export -> SDMX Query``. Following
example is to download 'Trade Union Density' data which set code is 'TUD'.


.. ipython:: python

    import pandas_datareader.data as web
    import datetime

    df = web.DataReader('TUD', 'oecd')

    df.columns

    df[['Japan', 'United States']]

.. _remote_data.eurostat:

Eurostat
========

`Eurostat <http://ec.europa.eu/eurostat/>`__ are available via ``DataReader``.

Get `Rail accidents by type of accident (ERA data) <http://appsso.eurostat.ec.europa.eu/nui/show.do?dataset=tran_sf_railac&lang=en>`_ data. The result will be a ``DataFrame`` which has ``DatetimeIndex`` as index and ``MultiIndex`` of attributes or countries as column. The target URL is:

* http://appsso.eurostat.ec.europa.eu/nui/show.do?dataset=tran_sf_railac&lang=en

You can specify dataset ID 'tran_sf_railac' to get corresponding data via ``DataReader``.

.. ipython:: python

    import pandas_datareader.data as web

    df = web.DataReader('tran_sf_railac', 'eurostat')
    df

.. _remote_data.tsp:

Thrift Savings Plan (TSP) Fund Data
===================================

Download mutual fund index prices for the Thrift Savings Plan (TSP).

.. ipython:: python

    import pandas_datareader.tsp as tsp
    tspreader = tsp.TSPReader(start='2015-10-1', end='2015-12-31')
    tspreader.read()


.. _remote_data.nasdaq_symbols:

Nasdaq Trader Symbol Definitions
================================

Download the latest symbols from `Nasdaq <ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt>`_.

Note that Nasdaq updates this file daily, and historical versions are not
available. More information on the `field <http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs>`_ definitions.

.. code-block:: python

    In [12]: from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
    In [13]: symbols = get_nasdaq_symbols()
    In [14]: print(symbols.loc['IBM'])
        Nasdaq Traded                                                    True
        Security Name       International Business Machines Corporation Co...
        Listing Exchange                                                    N
        Market Category
        ETF                                                             False
        Round Lot Size                                                    100
        Test Issue                                                      False
        Financial Status                                                  NaN
        CQS Symbol                                                        IBM
        NASDAQ Symbol                                                     IBM
        NextShares                                                      False
        Name: IBM, dtype: object


.. _remote_data.stooq:

Stooq Index Data
================
Google finance doesn't provide common index data download. The Stooq site has the data for download.

.. ipython:: python

    import pandas_datareader.data as web
    f = web.DataReader('^DJI', 'stooq')
    f[:10]

.. _remote_data.moex:

MOEX Data
=========
The Moscow Exchange (MOEX) provides historical data.


.. ipython:: python

   import pandas_datareader.data as web
   f = web.DataReader('USD000UTSTOM', 'moex', start='2017-07-01', end='2017-07-31')
   f.head()
