from functools import wraps
import warnings


def deprecate_kwarg(old_arg_name, new_arg_name):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if old_arg_name in kwargs:
                if new_arg_name in kwargs:
                    raise TypeError(
                        f"Can only specify '{old_arg_name}' or '{new_arg_name}', not both."
                    )
                warnings.warn(
                    f"the {old_arg_name} keyword is deprecated, use {new_arg_name} instead.",
                    FutureWarning,
                    stacklevel=2,
                )
                kwargs[new_arg_name] = kwargs.pop(old_arg_name)
            return func(*args, **kwargs)

        return wrapper

    return decorator
