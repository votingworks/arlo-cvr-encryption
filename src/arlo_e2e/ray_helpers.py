import os
import ray
from time import sleep


_ray_is_local = True


def ray_init_localhost(num_cpus: int = -1) -> None:
    """
    Initializes Ray for computation on the local computer. If num_cpus are specified,
    that's how many CPUs will be used. Otherwise, uses `os.cpu_count()`. Don't use
    this if you're running a giant Ray cluster. Instead, use `ray_init_cluster`.
    """
    global _ray_is_local
    if not ray.is_initialized():
        ray.init(num_cpus=num_cpus if num_cpus > 0 else os.cpu_count())
        _ray_is_local = True


def ray_init_cluster() -> None:  # pragma: no cover
    """
    Initializes Ray for computation on a big cluster.
    """
    global _ray_is_local
    if not ray.is_initialized():
        ray.init(address="auto")
        _ray_is_local = False


def ray_wait_for_workers(min_workers: int = 1) -> None:
    """
    We don't want to dispatch any work until we have workers nodes ready to go.
    This function is a no-op when running on localhost.
    """
    global _ray_is_local
    carriage_return_needed = False

    if _ray_is_local:
        return

    while True:
        nodes = ray.nodes()
        if len(nodes) >= min_workers:
            if carriage_return_needed:
                print(".", flush=True)
            return
        else:
            if not carriage_return_needed:
                print("Waiting for Ray worker nodes.", end="", flush=True)
                carriage_return_needed = True
            else:
                print(".", end="", flush=True)
        sleep(1)
