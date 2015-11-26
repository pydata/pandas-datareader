.. _cache:

.. currentmodule:: pandas-datareader

.. ipython:: python
   :suppress:

   import numpy as np
   from pandas import *
   import pandas as pd
   randn = np.random.randn
   np.set_printoptions(precision=4, suppress=True)
   options.display.max_rows = 15

***************
Caching queries
***************

It's generally a bad idea to make several requests to a server 
because it costs a lot of bandwidth.

Moreover, if you perform too much queries in a short time, your IP could be banned.

So it's a good practice to cache queries.

A ``requests_cache.Session`` can be passed to ``DataReader`` using ``session`` parameter.


Here is an example with Yahoo! Finance but it can also apply to other Remote Data Access.

.. ipython:: python

    import pandas_datareader.data as web
    import datetime
    import requests_cache
    expire_after = datetime.timedelta(days=3)
    session = requests_cache.CachedSession(cache_name='cache', backend='sqlite', expire_after=expire_after)    
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime(2013, 1, 27)
    f = web.DataReader("F", 'yahoo', start, end, session=session)
    f.ix['2010-01-04']

A `SQLite <https://www.sqlite.org/>`_ file named ``cache.sqlite`` should be created.


If you want to know more about how caching is performed, how to change backend... 
you can have a look at `requests-cache documentation <http://requests-cache.readthedocs.org/>`_.
