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

.. _remote_data.data_reader:

Functions from :mod:`pandas_datareader.data` and :mod:`pandas_datareader.wb`
extract data from various Internet sources into a pandas DataFrame.
Currently the following sources are supported:

    - :ref:`Yahoo! Finance<remote_data.yahoo>`
    - :ref:`Google Finance<remote_data.google>`
    - :ref:`Enigma<remote_data.enigma>`
    - :ref:`Quandl<remote_data.quandl>`
    - :ref:`St.Louis FED (FRED)<remote_data.fred>`
    - :ref:`Kenneth French's data library<remote_data.ff>`
    - :ref:`World Bank<remote_data.wb>`
    - :ref:`OECD<remote_data.oecd>`
    - :ref:`Eurostat<remote_data.eurostat>`
    - :ref:`Thrift Savings Plan<remote_data.tsp>`
    - :ref:`Nasdaq Trader symbol definitions<remote_data.nasdaq_symbols>`

It should be noted, that various sources support different kinds of data, so not all sources implement the same methods and the data elements returned might also differ.

.. _remote_data.yahoo:

Yahoo! Finance
==============

Historical stock prices from Yahoo! Finance.

.. ipython:: python

    import pandas_datareader.data as web
    import datetime
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime(2013, 1, 27)
    f = web.DataReader("F", 'yahoo', start, end)
    f.ix['2010-01-04']

Historical corporate actions (Dividends and Stock Splits) with ex-dates from Yahoo! Finance.

.. ipython:: python

  import pandas_datareader.data as web
  import datetime

  start = datetime.datetime(2010, 1, 1)
  end = datetime.datetime(2015, 5, 9)

  web.DataReader('AAPL', 'yahoo-actions', start, end)

Historical dividends from Yahoo! Finance.

.. ipython:: python

    import pandas_datareader.data as web
    import datetime
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime(2013, 1, 27)
    f = web.DataReader("F", 'yahoo-dividends', start, end)
    f

.. _remote_data.yahoo_quotes:

Yahoo! Finance Quotes
---------------------
***Experimental***

The YahooQuotesReader class allows to get quotes data from Yahoo! Finance.

.. ipython:: python

    import pandas_datareader.data as web
    amzn = web.get_quote_yahoo('AMZN')
    amzn

.. _remote_data.yahoo_options:

Yahoo! Finance Options
----------------------
***Experimental***

The Options class allows the download of options data from Yahoo! Finance.

The ``get_all_data`` method downloads and caches option data for all expiry months
and provides a formatted ``DataFrame`` with a hierarchical index, so its easy to get
to the specific option you want.

.. ipython:: python

      from pandas_datareader.data import Options
      aapl = Options('aapl', 'yahoo')
      data = aapl.get_all_data()
      data.iloc[0:5, 0:5]

      #Show the $100 strike puts at all expiry dates:
      data.loc[(100, slice(None), 'put'),:].iloc[0:5, 0:5]

      #Show the volume traded of $100 strike puts at all expiry dates:
      data.loc[(100, slice(None), 'put'),'Vol'].head()

If you don't want to download all the data, more specific requests can be made.

.. ipython:: python

      import datetime
      expiry = datetime.date(2016, 1, 1)
      data = aapl.get_call_data(expiry=expiry)
      data.iloc[0:5:, 0:5]

Note that if you call ``get_all_data`` first, this second call will happen much faster,
as the data is cached.

If a given expiry date is not available, data for the next available expiry will be
returned (January 15, 2015 in the above example).

Available expiry dates can be accessed from the ``expiry_dates`` property.

.. ipython:: python

      aapl.expiry_dates
      data = aapl.get_call_data(expiry=aapl.expiry_dates[0])
      data.iloc[0:5:, 0:5]

A list-like object containing dates can also be passed to the expiry parameter,
returning options data for all expiry dates in the list.

.. ipython:: python

      data = aapl.get_near_stock_price(expiry=aapl.expiry_dates[0:3])
      data.iloc[0:5:, 0:5]

The ``month`` and ``year`` parameters can be used to get all options data for a given month.

.. _remote_data.google:

Google Finance
==============

.. ipython:: python

    import pandas_datareader.data as web
    import datetime
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime(2013, 1, 27)
    f = web.DataReader("F", 'google', start, end)
    f.ix['2010-01-04']

.. _remote_data.google_quotes:

Google Finance Quotes
---------------------
***Experimental***

The GoogleQuotesReader class allows to get quotes data from Google Finance.

OFFLINE AS OF OCT 1, 2017

.. .. ipython:: python

..     import pandas_datareader.data as web
..     q = web.get_quote_google(['AMZN', 'GOOG'])
..     q

.. _remote_data.google_options:

Google Finance Options
----------------------
***Experimental***

The Options class allows the download of options data from Google Finance.

The ``get_options_data`` method downloads options data for specified expiry date
and provides a formatted ``DataFrame`` with a hierarchical index, so its easy to get
to the specific option you want.

Available expiry dates can be accessed from the ``expiry_dates`` property.

.. ipython:: python

      from pandas_datareader.data import Options
      goog = Options('goog', 'google')
      data = goog.get_options_data(expiry=goog.expiry_dates[0])
      data.iloc[0:5, 0:5]

.. _remote_data.enigma:

Enigma
======

Access datasets from `Enigma <https://public.enigma.com>`__,
the world's largest repository of structured public data. Note that the Enigma
URL has changed from `app.enigma.io <https://app.enigma.io>`__ as of release 
``0.6.0``, as the old API deprecated.

Datasets are unique identified by the ``uuid4`` at the end of a dataset's web address.
For example, the following code downloads from  `USDA Food Recalls 1996 Data <https://public.enigma.com/datasets/292129b0-1275-44c8-a6a3-2a0881f24fe1>`__.

.. ipython:: python

    import os
    import pandas_datareader as pdr

    df = pdr.get_data_enigma('292129b0-1275-44c8-a6a3-2a0881f24fe1', os.getenv('ENIGMA_API_KEY'))
    df.columns

.. _remote_data.quandl:

Quandl
======

Daily financial data (prices of stocks, ETFs etc.) from
`Quandl <https://www.quandl.com/>`__.
The symbol names consist of two parts: DB name and symbol name.
DB names can be all the
`free ones listed on the Quandl website <https://blog.quandl.com/free-data-on-quandl>__.
Symbol names vary with DB name; for WIKI (US stocks), they are the common
ticker symbols, in some other cases (such as FSE) they can be a bit strange.
Some sources are also mapped to suitable ISO country codes in the dot suffix
style shown above, currently available for
`BE, CN, DE, FR, IN, JP, NL, PT, UK, US <https://www.quandl.com/search?query=>`__.

As of June 2017, each DB has a different data schema,
the coverage in terms of time range is sometimes surprisingly small, and
the data quality is not always good.

.. ipython:: python

    import pandas_datareader.data as web
    symbol = 'WIKI/AAPL'  # or 'AAPL.US'
    df = web.DataReader(symbol, 'quandl', "2015-01-01", "2015-01-05")
    df.loc['2015-01-02']

.. _remote_data.fred:

FRED
====

.. ipython:: python

    import pandas_datareader.data as web
    import datetime
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime(2013, 1, 27)
    gdp = web.DataReader("GDP", "fred", start, end)
    gdp.ix['2013-01-01']

    # Multiple series:
    inflation = web.DataReader(["CPIAUCSL", "CPILFESL"], "fred", start, end)
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
    ds = web.DataReader("5_Industry_Portfolios", "famafrench")
    print(ds['DESCR'])
    ds[4].ix['1926-07']

.. _remote_data.wb:

World Bank
==========

``pandas`` users can easily access thousands of panel data series from the
`World Bank's World Development Indicators <http://data.worldbank.org>`__
by using the ``wb`` I/O functions.

Indicators
----------

Either from exploring the World Bank site, or using the search function included,
every world bank indicator is accessible.

For example, if you wanted to compare the Gross Domestic Products per capita in
constant dollars in North America, you would use the ``search`` function:

.. code-block:: python

    In [1]: from pandas_datareader import wb

    In [2]: wb.search('gdp.*capita.*const').iloc[:,:2]
    Out[2]:
                         id                                               name
    3242            GDPPCKD             GDP per Capita, constant US$, millions
    5143     NY.GDP.PCAP.KD                 GDP per capita (constant 2005 US$)
    5145     NY.GDP.PCAP.KN                      GDP per capita (constant LCU)
    5147  NY.GDP.PCAP.PP.KD  GDP per capita, PPP (constant 2005 internation...

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
    In [19]: mod = smf.ols("cellphone ~ np.log(gdp)", dat).fit()
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
-------------

The ``country`` argument accepts a string or list of mixed
`two <http://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>`__ or `three <http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3>`__ character
ISO country codes, as well as dynamic `World Bank exceptions <https://datahelpdesk.worldbank.org/knowledgebase/articles/898590-api-country-queries>`__ to the ISO standards.

For a list of the the hard-coded country codes (used solely for error handling logic) see ``pandas_datareader.wb.country_codes``.

Problematic Country Codes & Indicators
--------------------------------------

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
indicators, or a single "bad" (#4 above) country code).

See docstrings for more info.

.. _remote_data.oecd:

OECD
====

`OECD Statistics <http://stats.oecd.org/>`__ are avaliable via ``DataReader``.
You have to specify OECD's data set code.

To confirm data set code, access to ``each data -> Export -> SDMX Query``. Following
example is to download "Trade Union Density" data which set code is "UN_DEN".


.. ipython:: python

    import pandas_datareader.data as web
    import datetime

    df = web.DataReader('UN_DEN', 'oecd', end=datetime.datetime(2012, 1, 1))

    df.columns

    df[['Japan', 'United States']]

.. _remote_data.eurostat:

Eurostat
========

`Eurostat <http://ec.europa.eu/eurostat/>`__ are avaliable via ``DataReader``.

Get `Rail accidents by type of accident (ERA data) <http://appsso.eurostat.ec.europa.eu/nui/show.do?dataset=tran_sf_railac&lang=en>`_ data. The result will be a ``DataFrame`` which has ``DatetimeIndex`` as index and ``MultiIndex`` of attributes or countries as column. The target URL is:

* http://appsso.eurostat.ec.europa.eu/nui/show.do?dataset=tran_sf_railac&lang=en

You can specify dataset ID "tran_sf_railac" to get corresponding data via ``DataReader``.

.. ipython:: python

    import pandas_datareader.data as web

    df = web.DataReader("tran_sf_railac", 'eurostat')
    df

.. _remote_data.edgar:

EDGAR Index
===========

** As of December 31st, the SEC disabled access via FTP. EDGAR support
currently broken until re-write to use HTTPS. **

Company filing index from EDGAR (SEC).

The daily indices get large quickly (i.e. the set of daily indices from 1994
to 2015 is 1.5GB), and the FTP server will close the connection past some
downloading threshold . In testing, pulling one year at a time works well.
If the FTP server starts refusing your connections, you should be able to
reconnect after waiting a few minutes.


.. .. ipython:: python

..     import pandas_datareader.data as web
..     ed = web.DataReader('full', 'edgar-index')
..     ed[:5]

.. .. ipython:: python

..     import pandas_datareader.data as web
..     ed = web.DataReader('daily', 'edgar-index', '1998-05-18', '1998-05-18')
..     ed[:5]

.. _remote_data.tsp:

TSP Fund Data
=============

Download mutual fund index prices for the TSP.

.. ipython:: python

    import pandas_datareader.tsp as tsp
    tspreader = tsp.TSPReader(start='2015-10-1', end='2015-12-31')
    tspreader.read()


.. _remote_data.nasdaq_symbols:

Nasdaq Trader Symbol Definitions
================================

Download the latest symbols from `Nasdaq <ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt>`_.

Note that Nasdaq updates this file daily, and historical versions are not
available. More information on the `field <http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs/>`_ definitions.

.. code-block:: python

    In [12]: from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
    In [13]: symbols = get_nasdaq_symbols()
    In [14]: print(symbols.ix['IBM'])
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
