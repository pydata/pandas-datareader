.. pandas-datareader documentation master file, created by
   sphinx-quickstart on Mon Jan 26 20:32:50 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


pandas-datareader
=================

.. include:: _version.txt

Up-to-date remote data access for pandas. Works for multiple versions of pandas.


Quick Start
-----------

Install using ``pip``

.. code-block:: shell

   pip install pandas-datareader

and then import and use one of the data readers. This example reads 5-years
of 10-year constant maturity yields on U.S. government bonds.

.. code-block:: python

   import pandas_datareader as pdr
   pdr.get_data_fred('GS10')


Contents
--------

Contents:

.. toctree::
   :maxdepth: 1

   remote_data.rst
   cache.rst
   see-also.rst
   readers/index
   whatsnew.rst

Documentation
-------------

`Stable documentation <https://pydata.github.io/pandas-datareader/>`__
is available on
`github.io <https://pydata.github.io/pandas-datareader/>`__.
A second copy of the stable documentation is hosted on
`read the docs <https://pandas-datareader.readthedocs.io/>`_ for more details.

Recent developments
-------------------
You can install the latest development version using

.. code-block:: shell

   pip install git+https://github.com/pydata/pandas-datareader.git

or

.. code-block:: shell

   git clone https://github.com/pydata/pandas-datareader.git
   cd pandas-datareader
   python setup.py install

`Development documentation <https://pydata.github.io/pandas-datareader/devel/>`__
is available for the latest changes in master.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
