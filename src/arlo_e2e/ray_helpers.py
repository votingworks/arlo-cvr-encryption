import os

import ray


def ray_init_localhost(num_cpus: int = -1) -> None:
    """
    Initializes Ray for computation on the local computer. If num_cpus are specified,
    that's how many CPUs will be used. Otherwise, uses `os.cpu_count()`. Don't use
    this if you're running a giant Ray cluster. Instead, use `ray_init_cluster`.
    """
    if not ray.is_initialized():
        ray.init(num_cpus=num_cpus if num_cpus > 0 else os.cpu_count())


def ray_init_cluster() -> None:  # pragma: no cover
    """
    Initializes Ray for computation on a big cluster.
    """
    if not ray.is_initialized():
        ray.init(address="auto")
