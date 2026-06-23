.. _remote_data:

.. currentmodule:: pandas_datareader

.. ipython:: python
   :suppress:

   import pandas as pd
   import numpy as np
   np.set_printoptions(precision=4, suppress=True)
   pd.options.display.max_rows = 15


******************
Remote Data Access
******************

.. warning::

    The ``access_key`` keyword argument of ``DataReader`` has been deprecated in favor of ``api_key``.

.. _remote_data.data_reader:

The maintained public API is focused on macroeconomic, policy, central-bank,
and factor-style data.

Functions from :mod:`pandas_datareader.data`, :mod:`pandas_datareader.wb`, and
the newer :mod:`pandas_datareader.macro` module provide access to the
maintained public surface.

Currently documented public sources:

- :ref:`Federal Reserve Economic Data (FRED)<remote_data.fred>`
- :ref:`Fama/French data library<remote_data.ff>`
- :ref:`Bank of Canada<remote_data.bankofcanada>`
- :ref:`Econdb<remote_data.econdb>`
- :ref:`OECD<remote_data.oecd>`
- :ref:`Eurostat<remote_data.eurostat>`
- :ref:`World Bank<remote_data.wb>`

It should be noted that different sources expose different dimensions,
metadata, update cadences, and query semantics.

Package compatibility with pandas 3 means the library imports, runs, and passes
its maintained compatibility tests on pandas 3. Individual macro providers may
still change schemas, labels, or coverage over time.

The new macro module provides provider-specific clients and a unified routing
layer for maintained macro sources:

.. code-block:: python

   from pandas_datareader.macro import (
       describe_macro_dataset,
       read_macro,
       search_macro_datasets,
   )

   euro = read_macro("eurostat", "ert_h_eur_a", start="2009-01-01", end="2010-01-01")
   flows = search_macro_datasets("oecd", query="trade union")
   meta = describe_macro_dataset("eurostat", "ert_h_eur_a")


.. _remote_data.fred:

Federal Reserve Economic Data (FRED)
====================================

`FRED <https://fred.stlouisfed.org/>`__ provides public macroeconomic and
financial time series from the Federal Reserve Bank of St. Louis.

.. code-block:: ipython

   In [1]: import pandas_datareader as pdr
   In [2]: pdr.get_data_fred("GS10").head()


.. _remote_data.ff:

Fama/French
===========

Kenneth French's data library provides factor data and portfolio sorts.

.. code-block:: ipython

   In [1]: import pandas_datareader.data as web
   In [2]: ff = web.DataReader("F-F_Research_Data_Factors", "famafrench")
   In [3]: ff[0].head()


.. _remote_data.bankofcanada:

Bank of Canada
==============

The Bank of Canada valet API provides exchange-rate and other official series.

.. code-block:: ipython

   In [1]: import pandas_datareader.data as web
   In [2]: web.DataReader("FXUSDCAD", "bankofcanada", "2024-01-01", "2024-01-10").head()


.. _remote_data.econdb:

Econdb
======

`Econdb <https://www.econdb.com/>`__ aggregates macroeconomic data from many
official providers.

The current API requires authentication. Provide a token either through the
``api_key`` argument, the ``ECONDB_API_KEY`` environment variable, or an
explicit ``token=...`` query parameter.

.. code-block:: ipython

   In [1]: import pandas_datareader as pdr
   In [2]: import os
   In [3]: pdr.get_data_econdb("ticker=RGDPUS", api_key=os.getenv("ECONDB_API_KEY")).head()


.. _remote_data.oecd:

OECD
====

The legacy OECD reader remains available for SDMX-based OECD datasets, while
the new macro API exposes discovery and metadata helpers for the current OECD
SDMX endpoints.

.. code-block:: ipython

   In [1]: import pandas_datareader.data as web
   In [2]: web.DataReader("TUD", "oecd", start="2010-01-01", end="2012-01-01").head()

.. code-block:: ipython

   In [3]: from pandas_datareader.macro import read_macro
   In [4]: result = read_macro("oecd", "OECD.ELS.SAE,DSD_TUD_CBC@DF_CBC,1.0")
   In [5]: result.data.head()


.. _remote_data.eurostat:

Eurostat
========

The legacy Eurostat reader is backed by current Eurostat SDMX 2.1 endpoints,
and the macro API exposes discovery and dataset-description helpers.

.. code-block:: ipython

   In [1]: import pandas_datareader.data as web
   In [2]: web.DataReader("ert_h_eur_a", "eurostat", "2009-01-01", "2010-01-01").head()

.. code-block:: ipython

   In [3]: from pandas_datareader.macro import describe_macro_dataset
   In [4]: describe_macro_dataset("eurostat", "ert_h_eur_a")["dimensions"][:2]


.. _remote_data.wb:

World Bank
==========

The World Bank helpers provide indicator search, country metadata, and data
downloads.

.. code-block:: ipython

   In [1]: from pandas_datareader import wb
   In [2]: wb.search("gdp.*capita.*constant").head()
   In [3]: wb.get_countries().head()


.. _remote_data.removed:

Readers Removed
===============

.. warning:: Reader Removed

    Readers that are no longer maintained or have been removed from the public API include:

    - Enigma
    - Nasdaq
    - AlphaVantage
    - Thrift Savings Plan
    - Quandl
