import os

import ray
from electionguard.group import (
    ElementModP,
    int_to_p_unchecked,
    ElementModQ,
    int_to_q_unchecked,
)


def ray_init_localhost(num_cpus: int = -1) -> None:
    """
    Initializes Ray for computation on the local computer. If num_cpus are specified,
    that's how many CPUs will be used. Otherwise, uses `os.cpu_count()`. Don't use
    this if you're running a giant Ray cluster. Instead, use `ray_init_cluster`.
    """
    if not ray.is_initialized():
        ray.init(num_cpus=num_cpus if num_cpus > 0 else os.cpu_count())
        ray_init_serializers()


def ray_init_cluster() -> None:  # pragma: no cover
    """
    Initializes Ray for computation on a big cluster.
    """
    if not ray.is_initialized():
        # for a million ballots, we'll need 1TB of storage, so we're doubling that for now
        ray.init(address="auto", driver_object_store_memory=2 * 1024 * 1024 * 1024)
        ray_init_serializers()


def ray_init_serializers() -> None:
    """
    Configures Ray's serialization to work properly with ElectionGuard. Note that
    this is completely unrelated to the JSON serialization features that ElectionGuard
    supports for writing its data structures to disk.
    """
    ray.register_custom_serializer(
        ElementModP, lambda p: p.to_int(), lambda i: int_to_p_unchecked(i)
    )
    ray.register_custom_serializer(
        ElementModQ, lambda q: q.to_int(), lambda i: int_to_q_unchecked(i)
    )
