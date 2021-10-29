from .internals import functions


def __getattr__(name):
    return getattr(functions, name)
