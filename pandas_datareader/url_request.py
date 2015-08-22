import time
from pandas.io.common import urlopen
from pandas import read_csv
from pandas.compat import StringIO, bytes_to_str
from pandas.util.testing import _network_error_classes

def _retry_read_url(url, retry_count, pause, name):
    """
    Open url (and retry)
    """
    for _ in range(retry_count):
        time.sleep(pause)

        # kludge to close the socket ASAP
        try:
            with urlopen(url) as resp:
                lines = resp.read()
        except _network_error_classes:
            pass
        else:
            rs = read_csv(StringIO(bytes_to_str(lines)), index_col=0,
                          parse_dates=True, na_values='-')[::-1]
            # Yahoo! Finance sometimes does this awesome thing where they
            # return 2 rows for the most recent business day
            if len(rs) > 2 and rs.index[-1] == rs.index[-2]:  # pragma: no cover
                rs = rs[:-1]

            #Get rid of unicode characters in index name.
            try:
                rs.index.name = rs.index.name.decode('unicode_escape').encode('ascii', 'ignore')
            except AttributeError:
                #Python 3 string has no decode method.
                rs.index.name = rs.index.name.encode('ascii', 'ignore').decode()

            return rs

    raise IOError("after %d tries, %s did not "
                  "return a 200 for url %r" % (retry_count, name, url))
