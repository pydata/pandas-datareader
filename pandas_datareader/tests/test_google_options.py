import nose
import pandas as pd
import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError


class TestGoogleOptions(tm.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestGoogleOptions, cls).setUpClass()

        # goog has monthlies
        cls.goog = web.Options('GOOG', 'google')

    def test_get_options_data(self):
        try:
            options = self.goog.get_options_data(expiry=self.goog.expiry_dates[0])
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(options) > 10)
        tm.assert_index_equal(options.columns,
                              pd.Index(['Last', 'Bid', 'Ask', 'Chg', 'PctChg', 'Vol',
                                        'Open_Int', 'Root', 'Underlying_Price', 'Quote_Time']))
        tm.assert_equal(options.index.names, [u'Strike', u'Expiry', u'Type', u'Symbol'])

    def test_expiry_dates(self):
        try:
            dates = self.goog.expiry_dates
        except RemoteDataError as e:  # pragma: no cover
            raise nose.SkipTest(e)
        self.assertTrue(len(dates) > 10)

    def test_get_all_data(self):
        self.assertRaises(NotImplementedError, self.goog.get_all_data)

    def test_get_options_data_with_year(self):
        self.assertRaises(NotImplementedError, self.goog.get_options_data, year=2016)

if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)  # pragma: no cover
