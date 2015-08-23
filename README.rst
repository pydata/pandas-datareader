pandas-datareader
=================

Up to date remote data access for pandas, works for multiple versions of pandas.

.. image:: https://travis-ci.org/pydata/pandas-datareader.svg?branch=master
    :target: https://travis-ci.org/pydata/pandas-datareader

.. image:: https://coveralls.io/repos/pydata/pandas-datareader/badge.svg?branch=master
    :target: https://coveralls.io/r/pydata/pandas-datareader

.. image:: https://readthedocs.org/projects/pandas-datareader/badge/?version=latest
    :target: http://pandas-datareader.readthedocs.org/en/latest/

Installation
------------

Install via pip

.. code-block:: shell

   $ pip install pandas-datareader

Usage
-----

*In future pandas releases (0.17+) pandas-datareader will become a dependancy and using* ``pandas.io.data``
*will be equivalent to using* ``pandas_datareader.data``.

For now, you must replace your imports from ``pandas.io`` with ``pandas_datareader``:

.. code-block:: python

   from pandas.io import data, wb # becomes
   from pandas_datareader import data, wb


See the `pandas-datareader documentation <http://pandas-datareader.readthedocs.org/>`_ for more details.
