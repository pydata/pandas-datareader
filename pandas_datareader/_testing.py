"""
Utilities for testing purposes.
"""

from functools import wraps


def skip_on_exception(exp):
    """
    Skip a test if a specific Exception is raised. This is because
    the Exception is raised for reasons beyond our control (e.g.
    flakey 3rd-party API).

    Parameters
    ----------
    exp : The Exception under which to execute try-except.
    """

    from pytest import skip

    def outer_wrapper(f):

        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                f(*args, **kwargs)
            except exp as e:
                skip(e)

        return wrapper

    return outer_wrapper
