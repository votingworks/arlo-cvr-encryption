import os
import ray
from time import sleep

from arlo_e2e.ray_write_retry import (
    set_failure_probability_for_testing,
    init_status_actor,
)

_ray_is_local = True


def ray_init_localhost(
    num_cpus: int = -1, write_failure_probability: float = 0.0
) -> None:
    """
    Initializes Ray for computation on the local computer. If num_cpus are specified,
    that's how many CPUs will be used. Otherwise, uses `os.cpu_count()`. Don't use
    this if you're running a giant Ray cluster. Instead, use `ray_init_cluster`.

    The `write_failure_probability` argument should only be used for unit testing,
    to validate that our write retry mechanisms work.
    """
    global _ray_is_local
    if not ray.is_initialized():
        ray.init(num_cpus=num_cpus if num_cpus > 0 else os.cpu_count())
        _ray_is_local = True
        ray_wait_for_workers()
        ray_post_init(write_failure_probability)


def ray_init_cluster(
    write_failure_probability: float = 0.0,
) -> None:  # pragma: no cover
    """
    Initializes Ray for computation on a big cluster.
    """
    global _ray_is_local
    if not ray.is_initialized():
        ray.init(address="auto")
        _ray_is_local = False
        ray_wait_for_workers()
        ray_post_init(write_failure_probability)


def ray_post_init(write_failure_probability: float = 0.0) -> None:
    """
    If you've already called ray.init() yourself and you just need to initialize
    the things that arlo-e2e cares about, call this method instead.
    """
    init_status_actor()
    set_failure_probability_for_testing(write_failure_probability)


def ray_wait_for_workers(min_workers: int = 1) -> None:  # pragma: no cover
    """
    We don't want to dispatch any work until we have workers nodes ready to go.
    This function is a no-op when running on localhost.
    """
    global _ray_is_local
    carriage_return_needed = False

    if _ray_is_local:
        return

    while True:
        # ray.workers() is indeed defined in Ray 1.1 or later, but for whatever
        # reason we're getting errors from mypy, so the solution is to suppress
        # that specific error on this line. This shouldn't be necessary.
        nodes = ray.workers()  # type: ignore[attr-defined]

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
