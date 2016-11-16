import nose
import pandas.util.testing as tm
import pandas_datareader.base as base


class TestBaseReader(tm.TestCase):
    def test_valid_retry_count(self):
        with tm.assertRaises(ValueError):
            base._BaseReader([], retry_count='stuff')
        with tm.assertRaises(ValueError):
            base._BaseReader([], retry_count=-1)

    def test_invalid_url(self):
        with tm.assertRaises(NotImplementedError):
            base._BaseReader([]).url

    def test_invalid_format(self):
        with tm.assertRaises(NotImplementedError):
            b = base._BaseReader([])
            b._format = 'IM_NOT_AN_IMPLEMENTED_TYPE'
            b._read_one_data('a', None)


class TestDailyBaseReader(tm.TestCase):
    def test_get_params(self):
        with tm.assertRaises(NotImplementedError):
            b = base._DailyBaseReader()
            b._get_params()


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
