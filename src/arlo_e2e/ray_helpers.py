from typing import Iterator, Sequence, List

import ray
import os

from electionguard.group import ElementModP, int_to_p_unchecked, ElementModQ, int_to_q_unchecked
from ray.util.iter import ParallelIterator
from typing import Type


def ray_init_localhost():
    """
    Initializes Ray for computation on the local computer.
    """
    ray.init(num_cpus=os.cpu_count())
    ray_init_serializers()


def ray_init_serializers():
    """
    Configures Ray's serialization to work properly with ElectionGuard.
    """
    ray.register_custom_serializer(ElementModP, lambda p: p.to_int(), lambda i: int_to_p_unchecked(i))
    ray.register_custom_serializer(ElementModQ, lambda q: q.to_int(), lambda i: int_to_q_unchecked(i))


T = Type["T"]


def par_iterator(iter: Iterator[T]) -> ParallelIterator[T]:
    """
    Given a regular iterator, returns a Ray parallel iterator.
    """
    # TODO: what goes here for num_shards when we're on an autoscaling cluster?
    return ray.util.iter.from_items(iter, num_shards=os.cpu_count())


def shard_list(input: List[T], num_per_group: int) -> List[List[T]]:
    """
    Breaks a list up into a list of lists, with `num_per_group` entries in each group,
    except for the final group which might be smaller.
    """
    assert num_per_group >= 1, "need a positive number of list elements per group"
    return [input[i:i + num_per_group] for i in range(0, len(input), num_per_group)]