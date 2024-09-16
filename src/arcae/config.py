from contextlib import contextmanager

_DELETE_MARKER = object()


@contextmanager
def set(**kw):
    from arcae.lib.arrow_tables import Configuration

    config = Configuration()
    saved = {}

    for k, v in kw.items():
        try:
            old = config[k]
        except KeyError:
            saved[k], config[k] = _DELETE_MARKER, v
        else:
            saved[k], config[k] = old, v

    yield

    for k, v in saved.items():
        if v is _DELETE_MARKER:
            del config[k]
        else:
            config[k] = v
