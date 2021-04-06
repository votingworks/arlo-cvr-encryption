from base64 import b64encode
from hashlib import sha256
from math import ceil, floor
from typing import (
    TypeVar,
    Callable,
    Sequence,
    List,
    Iterable,
    AnyStr,
)

from more_itertools import peekable

T = TypeVar("T")
R = TypeVar("R")


def first_n(input: Iterable[T], n: int) -> Sequence[T]:
    """
    Assuming the iterable has at least `n` elements, then that many will be
    returned and consumed from the `input`. If there are fewer than `n` elements,
    then that's how many you get. So, an empty input will yield an empty output.
    """

    # We could ostensibly generate our output lazily, rather than computing
    # it all at once, but when the underlying input is iterable, that seems
    # like a recipe for incomprehensible bugs, so we're instead doing this
    # computation eagerly.

    output: List[T] = []
    input = iter(input)  # convert Iterable -> Iterator

    while n > 0:
        try:
            output.append(next(input))
        except StopIteration:
            break
        n = n - 1

    return output


def flatmap(f: Callable[[T], Iterable[R]], li: Iterable[T]) -> Iterable[R]:
    """
    General-purpose flatmapping on sequences/lists/iterables. The lambda is
    expected to return a list of items for each input in the original list `li`,
    and all those lists are concatenated together.

    Note: This computation executes *lazily* on the input, and the output
    is ephemeral (i.e., you iterate it once and it's gone). Convert the output
    to a list to have a persistent result.
    """
    mapped = map(f, li)

    for item in mapped:
        for subitem in item:
            yield subitem


def shard_iterable(input: Iterable[T], num_per_group: int) -> Iterable[Sequence[T]]:
    """
    Breaks a list up into a list of lists, with `num_per_group` entries in
    each group, except for the final group which might be smaller. Useful
    for many things, including dividing up work units for parallel dispatch.

    Note: This computation executes *lazily* on the input, and the output
    is ephemeral (i.e., you iterate it once and it's gone). Convert the output
    to a list to have a persistent result. Also, while the output iterable is
    generated lazily, each list within it is generated eagerly and will be persistent.
    """
    assert num_per_group >= 1, "need a positive number of list elements per group"
    input_list = list(input)
    length = len(input_list)
    return (input_list[i : i + num_per_group] for i in range(0, length, num_per_group))


def shard_list_uniform(input: Sequence[T], num_per_group: int) -> Iterable[Sequence[T]]:
    """
    Similar to `shard_iterable`, but using a error residual propagation technique
    to ensure that the minimum and maximum number of elements per shard differ
    by at most one. The maximum number of elements per group will be less than
    or equal to `num_per_group`.

    Note: This computation executes *lazily* on the input, and the output
    is ephemeral (i.e., you iterate it once and it's gone). Convert the output
    to a list to have a persistent result. Also, while the output iterable is
    generated lazily, each list within it is generated eagerly and will be persistent.
    """

    return shard_iterable_uniform(input, num_per_group, len(input))


def shard_iterable_uniform(
    input: Iterable[T], num_per_group: int, num_inputs: int
) -> Iterable[Sequence[T]]:
    """
    Similar to `shard_iterable`, but using a error residual propagation technique
    to ensure that the minimum and maximum number of elements per shard differ
    by at most one. The maximum number of elements per group will be less than
    or equal to `num_per_group`. Since the input is an iterable, we don't know
    its size, which is necessary for the uniform distribution, so you need to
    pass the size explicitly in `num_inputs`. If you have a list or something
    else that knows its size, you can use `shard_list_uniform` instead.

    Note: This computation executes *lazily* on the input, and the output
    is ephemeral (i.e., you iterate it once and it's gone). Convert the output
    to a list to have a persistent result. Also, while the output iterable is generated
    lazily, each list within it is generated eagerly and will be persistent.
    """
    assert num_per_group >= 1, "need a positive number of list elements per group"
    assert num_inputs >= 0, "can't have negative number of inputs"

    if num_inputs <= 0:
        return []

    num_groups = int(ceil(num_inputs / num_per_group))

    # this will be a floating point number, not an integer
    num_per_group_revised = num_inputs / num_groups

    # leftover error, akin to Floyd-Steinberg dithering
    # (https://en.wikipedia.org/wiki/Floyd%E2%80%93Steinberg_dithering)
    residual = 0.0

    input_iter = peekable(iter(input))

    while input_iter:
        current_num_per_group_float = num_per_group_revised + residual
        current_num_per_group_int = int(floor(num_per_group_revised + residual))
        residual = current_num_per_group_float - current_num_per_group_int

        # With floating point approximations, we might get really close but not quite, so we'll compensate
        # for this by saying if we're within epsilon of adding one more, we'll treat it as good enough.
        if residual > 0.99:
            current_num_per_group_int += 1
            residual = current_num_per_group_float - current_num_per_group_int

        yield [next(input_iter) for _ in range(0, current_num_per_group_int)]


def sha256_hash(input: AnyStr) -> str:
    """
    Given a string or array of bytes, returns a base64-encoded representation of the
    256-bit SHA2-256 hash of that input string, first encoding the input string as UTF8.
    """
    h = sha256()
    if isinstance(input, str):
        encoded = input.encode("utf-8")
        h.update(encoded)
    else:
        h.update(input)

    hash_str = b64encode(h.digest()).decode("utf-8")
    return hash_str
