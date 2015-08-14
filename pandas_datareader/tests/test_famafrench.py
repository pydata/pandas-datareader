import nose
import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader.famafrench import get_available_datasets


class TestFamaFrench(tm.TestCase):
    def test_get_data(self):
        keys = [
            'F-F_Research_Data_Factors', 'F-F_ST_Reversal_Factor',
            '6_Portfolios_2x3', 'Portfolios_Formed_on_ME',
            'Prior_2-12_Breakpoints', 'ME_Breakpoints',
        ]
        for name in keys:
            ff = web.DataReader(name, 'famafrench')
            assert 'DESCR' in ff
            assert len(ff) > 1

    def test_get_available_datasets(self):
        # _skip_if_no_lxml()
        l = get_available_datasets()
        assert len(l) > 100

    def test_index(self):
        ff = web.DataReader('F-F_Research_Data_Factors', 'famafrench')
        assert ff[0].index.freq == 'M'
        assert ff[1].index.freq == 'A-DEC'



if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
