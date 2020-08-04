import os
from typing import Sequence, TypeVar

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
    this if you're running a giant Ray cluster.
    """
    if not ray.is_initialized():
        ray.init(num_cpus=num_cpus if num_cpus > 0 else os.cpu_count())
        ray_init_serializers()


def ray_shutdown_localhost() -> None:
    """
    Shuts down Ray. Opposite of `ray_init_localhost`.
    """
    ray.shutdown()


def ray_init_serializers() -> None:
    """
    Configures Ray's serialization to work properly with ElectionGuard.
    """
    ray.register_custom_serializer(
        ElementModP, lambda p: p.to_int(), lambda i: int_to_p_unchecked(i)
    )
    ray.register_custom_serializer(
        ElementModQ, lambda q: q.to_int(), lambda i: int_to_q_unchecked(i)
    )


T = TypeVar("T")


def shard_list(input: Sequence[T], num_per_group: int) -> Sequence[Sequence[T]]:
    """
    Breaks a list up into a list of lists, with `num_per_group` entries in each group,
    except for the final group which might be smaller. Useful for many things, including
    dividing up work units for parallel dispatch.
    """
    assert num_per_group >= 1, "need a positive number of list elements per group"
    length = len(input)
    return [input[i : i + num_per_group] for i in range(0, length, num_per_group)]
