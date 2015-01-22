===== 
pandas-datareader
===== 

Up to date remote data access for pandas, works for multiple versions of pandas.

.. image:: https://travis-ci.org/pydata/pandas-datareader.svg?branch=master
    :target: https://travis-ci.org/pydata/pandas-datareader

.. image:: https://coveralls.io/repos/pydata/pandas-datareader/badge.svg?branch=master
    :target: https://coveralls.io/r/pydata/pandas-datareader

Installation
------------

Install via pip

.. code-block:: shell

   $ pip install pandas-datareader

Usage 
--------

Rather than import from ``pandas.io`` you should import from ``pandas-datareader``:

.. code-block:: python

   from pandas.io import data, wb  # becomes
   from pandas_datareader import data, wb
