import datetime as dt

import pandas_datareader.tsp as tsp


class TestTSPFunds(object):
    def test_get_allfunds(self):
        tspdata = tsp.TSPReader(start='2015-11-2', end='2015-11-2').read()

        assert len(tspdata == 1)

        assert round(tspdata['I Fund'][dt.date(2015, 11, 2)], 5) == 25.0058

    def test_sanitize_response(self):
        class response(object):
            pass
        r = response()
        r.text = ' , '
        ret = tsp.TSPReader._sanitize_response(r)
        assert ret == ''
        r.text = ' a,b '
        ret = tsp.TSPReader._sanitize_response(r)
        assert ret == 'a,b'
