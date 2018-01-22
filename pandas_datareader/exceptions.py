"""Custom warnings and exceptions"""


class UnstableAPIWarning(Warning):
    pass


DEP_ERROR_MSG = """
{0} has been immediately deprecated due to large breaks in the API without the
introduction of a stable replacement. Pull Requests to re-enable these data
connectors are welcome.

See https://github.com/pydata/pandas-datareader/issues
"""


class ImmediateDeprecationError(Exception):
    pass
