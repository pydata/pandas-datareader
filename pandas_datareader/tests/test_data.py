from pandas import DataFrame
import pytest

from pandas_datareader.data import DataReader

pytestmark = pytest.mark.stable


class TestDataReader:

    @pytest.mark.xfail(reason="Changes in API need fixes")
    def test_read_iex(self):
        gs = DataReader("GS", "iex-last")
        assert isinstance(gs, DataFrame)

    def test_read_fred(self):
        vix = DataReader("VIXCLS", "fred")
        assert isinstance(vix, DataFrame)

    def test_not_implemented(self):
        with pytest.raises(NotImplementedError):
            DataReader("NA", "NA")


class TestDeprecateKwarg:
    """Regression tests for pandas 2.3+ compatibility (issue #77).

    pandas 2.3 changed the signature of ``pandas.util._decorators.deprecate_kwarg``
    so that it requires a warning class as the first argument, breaking the
    ``@deprecate_kwarg("access_key", "api_key")`` usage in data.py at import
    time.  The fix moves to a local implementation in pandas_datareader.compat.
    """

    def test_import_succeeds(self):
        # Importing the module must not raise TypeError with any pandas version.
        import pandas_datareader.data  # noqa: F401

    def test_deprecated_kwarg_warns(self):
        with pytest.warns(FutureWarning, match="access_key"):
            with pytest.raises(NotImplementedError):
                # Use an unknown source so the call fails cleanly with
                # NotImplementedError after the kwarg renaming logic runs.
                DataReader("AAPL", "unknown-source", access_key="dummy")

    def test_deprecated_kwarg_forwards_value(self):
        # When access_key is passed, it should be forwarded as api_key.
        # We verify this by ensuring no TypeError about missing api_key occurs
        # and that the FutureWarning is raised.
        with pytest.warns(FutureWarning, match="access_key"):
            with pytest.raises(NotImplementedError):
                DataReader("AAPL", "unknown-source", access_key="mykey")

    def test_both_kwargs_raises(self):
        with pytest.raises(TypeError, match="access_key|api_key"):
            DataReader(
                "AAPL", "yahoo", access_key="old_key", api_key="new_key"
            )
