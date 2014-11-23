def dropResult(f, *args, **kwargs):
    return lambda ignore: f(*args, **kwargs)
