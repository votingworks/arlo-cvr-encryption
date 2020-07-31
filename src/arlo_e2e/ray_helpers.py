import os
from typing import Sequence, TypeVar

import ray
from electionguard.group import (
    ElementModP,
    int_to_p_unchecked,
    ElementModQ,
    int_to_q_unchecked,
)


def ray_init_localhost() -> None:
    """
    Initializes Ray for computation on the local computer.
    """
    ray.init(num_cpus=os.cpu_count())
    ray_init_serializers()


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
