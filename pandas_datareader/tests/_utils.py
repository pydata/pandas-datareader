import nose


def _skip_if_no_lxml():
    try:
        import lxml  # noqa
    except ImportError:  # pragma: no cover
        raise nose.SkipTest("no lxml")
