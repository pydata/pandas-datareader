# pandas-datareader

Up to date remote data access for pandas, works for multiple versions of
pandas.

[![image][]][1]

[![image][2]][3]

[![image][4]][5]

[![image][6]][7]

[![image][8]][9]

[![image][10]][11]

## Installation

Install using `pip`

``` shell
pip install pandas-datareader
```

## Usage

``` python
import pandas_datareader as pdr
pdr.get_data_fred('GS10')
```

## Documentation

[Stable documentation][] is available on
[github.io][Stable documentation]. A second copy of the stable
documentation is hosted on [read the docs][] for more details.

[Development documentation][] is available for the latest changes in
master.

### Requirements

Using pandas datareader requires the following packages:

-   pandas>=0.23
-   lxml
-   requests>=2.19.0

Building the documentation additionally requires:

-   matplotlib
-   ipython
-   requests_cache
-   sphinx
-   pydata_sphinx_theme

Development and testing additionally requires:

-   black
-   coverage
-   codecov
-   coveralls
-   flake8
-   pytest
-   pytest-cov
-   wrapt

### Install latest development version

``` shell
pip install git+https://github.com/pydata/pandas-datareader.git
```

or

``` shell
git clone https://github.com/pydata/pandas-datareader.git
cd pandas-datareader
python setup.py install
```

  [image]: https://img.shields.io/pypi/v/pandas-datareader.svg
  [1]: https://pypi.python.org/pypi/pandas-datareader/
  [2]: https://travis-ci.org/pydata/pandas-datareader.svg?branch=master
  [3]: https://travis-ci.org/pydata/pandas-datareader
  [4]: https://coveralls.io/repos/pydata/pandas-datareader/badge.svg?branch=master
  [5]: https://coveralls.io/r/pydata/pandas-datareader
  [6]: https://codecov.io/gh/pydata/pandas-datareader/branch/master/graph/badge.svg
  [7]: https://codecov.io/gh/pydata/pandas-datareader
  [8]: https://readthedocs.org/projects/pandas-datareader/badge/?version=latest
  [9]: https://pandas-datareader.readthedocs.io/en/latest/
  [10]: https://img.shields.io/badge/code%20style-black-000000.svg
  [11]: https://github.com/psf/black
  [Stable documentation]: https://pydata.github.io/pandas-datareader/
  [read the docs]: https://pandas-datareader.readthedocs.io/
  [Development documentation]: https://pydata.github.io/pandas-datareader/devel/