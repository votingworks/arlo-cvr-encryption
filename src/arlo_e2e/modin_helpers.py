import os


def suppress_modin_ray_init() -> None:
    """
    Modin wants to call ray.init() on its own, and that's a problem for us,
    since we want to call it ourselves and avoid Modin doing it in a way that
    doesn't work with newer versions of Ray.
    """
    os.environ["MODIN_ENGINE"] = "ray"

    # The solution? Do the import, which will fail, and then monkey-patch
    # the side-effects. This is seriously gross stuff.
    try:
        import modin.pandas
    except Exception:
        pass
    modin.pandas.num_cpus = os.cpu_count()
    modin.pandas._is_first_update["Ray"] = False
