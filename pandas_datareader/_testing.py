"""
Utilities for testing purposes.
"""

import wrapt


def skip_on_exception(exp):
    """
    Skip a test if a specific Exception is raised. This is because
    the Exception is raised for reasons beyond our control (e.g.
    flakey 3rd-party API).

    a signature-preserving decorator

    Parameters
    ----------
    exp : The Exception under which to execute try-except.
    """

    from pytest import skip

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        try:
            return wrapped(*args, **kwargs)
        except exp as e:
            skip(str(e))

    return wrapper
