import nose
import pandas.util.testing as tm

import pandas_datareader.data as web


class TestEdgarIndex(tm.TestCase):
    def test_get_index(self):
        ed = web.DataReader('full', 'edgar-index')
        assert len(ed > 1000)

if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
