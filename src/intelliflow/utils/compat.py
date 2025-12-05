def path(pkg, res):
    from importlib import resources as importlib_resources

    try:
        handle = importlib_resources.files(pkg).joinpath(res)
    except AttributeError:
        handle = importlib_resources.path(pkg, res)

    return handle
