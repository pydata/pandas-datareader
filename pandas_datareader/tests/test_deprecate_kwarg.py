"""
Regression tests for issue #1005:
pandas-datareader imported deprecate_kwarg from the private internal API
pandas.util._decorators, which is being removed in pandas 3.0.

Fix: vendor the implementation in pandas_datareader.compat so there is no
dependency on a private pandas API.
"""
import warnings
import pytest
from pandas_datareader.compat import deprecate_kwarg


class TestDeprecateKwarg:

    def test_old_kwarg_warns_and_maps_to_new(self):
        @deprecate_kwarg("access_key", "api_key")
        def f(api_key=None):
            return api_key

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = f(access_key="abc")

        assert result == "abc"
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "access_key" in str(w[0].message)
        assert "api_key" in str(w[0].message)

    def test_new_kwarg_passes_without_warning(self):
        @deprecate_kwarg("old", "new")
        def f(new=None):
            return new

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = f(new="value")

        assert result == "value"
        assert len(w) == 0

    def test_both_old_and_new_raises_type_error(self):
        @deprecate_kwarg("old", "new")
        def f(new=None):
            return new

        with pytest.raises(TypeError, match="Can only specify"):
            f(old="a", new="b")

    def test_mapping_dict_translates_value(self):
        @deprecate_kwarg("old", "new", mapping={"yes": True, "no": False})
        def f(new=None):
            return new

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = f(old="yes")

        assert result is True
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)

    def test_mapping_callable_translates_value(self):
        @deprecate_kwarg("old", "new", mapping=lambda x: x.upper())
        def f(new=None):
            return new

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = f(old="hello")

        assert result == "HELLO"
        assert len(w) == 1

    def test_new_arg_name_none_warns_and_keeps_old(self):
        @deprecate_kwarg("old_param", None)
        def f(old_param=None):
            return old_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = f(old_param="val")

        assert result == "val"
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)

    def test_invalid_mapping_type_raises(self):
        with pytest.raises(TypeError, match="mapping from old to new"):
            deprecate_kwarg("old", "new", mapping="invalid")

    def test_import_does_not_use_private_pandas_api(self):
        """Ensure data.py no longer imports from pandas.util._decorators."""
        import ast
        import pathlib

        data_src = (
            pathlib.Path(__file__).parent.parent / "data.py"
        ).read_text()
        tree = ast.parse(data_src)

        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                src = ast.unparse(node)
                assert "pandas.util._decorators" not in src, (
                    f"data.py still imports from private pandas API: {src}"
                )
