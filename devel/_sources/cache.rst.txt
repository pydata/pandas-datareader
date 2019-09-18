.. _cache:

.. currentmodule:: pandas-datareader

.. ipython:: python
   :suppress:

   import numpy as np
   from pandas import *
   import pandas as pd
   import requests_cache
   randn = np.random.randn
   np.set_printoptions(precision=4, suppress=True)
   options.display.max_rows = 15

***************
Caching queries
***************

Making the same request repeatedly can use a lot of bandwidth, slow down your
code and may result in your IP being banned.

``pandas-datareader`` allows you to cache queries using ``requests_cache`` by
passing a ``requests_cache.Session`` to ``DataReader`` or ``Options`` using the
``session`` parameter.

Below is an example with Yahoo! Finance. The session parameter is implemented for all datareaders.

.. ipython:: python

    import pandas_datareader.data as web
    import datetime
    import requests_cache
    expire_after = datetime.timedelta(days=3)
    session = requests_cache.CachedSession(cache_name='cache', backend='sqlite', expire_after=expire_after)
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime(2013, 1, 27)
    f = web.DataReader("F", 'yahoo', start, end, session=session)
    f.loc['2010-01-04']

A `SQLite <https://www.sqlite.org/>`_ file named ``cache.sqlite`` will be created in the working
directory, storing the request until the expiry date.

For additional information on using requests-cache, see the
`documentation <https://requests-cache.readthedocs.io/>`_.
