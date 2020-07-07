import functools
from multiprocessing import Pool, cpu_count
from typing import (
    TypeVar,
    Callable,
    Sequence,
    List,
    Iterable,
    Optional,
    NamedTuple,
    Any,
)

from electionguard.logs import log_warning
from tqdm import tqdm

T = TypeVar("T")
U = TypeVar("U")


def flatmap(f: Callable[[T], Iterable[U]], li: Iterable[T]) -> Sequence[U]:
    """
    General-purpose flatmapping on sequences/lists/iterables. The lambda is
    expected to return a list of items for each input in the original list `li`,
    and all those lists are concatenated together.
    """
    mapped = map(f, li)

    # This function eagerly computes its result. The below link tries to do it in a
    # more lazy fashion using Python's yield keyword. Might be worth investigating.
    # https://stackoverflow.com/questions/53509826/equivalent-to-pyspark-flatmap-in-python

    result: List[U] = []
    for item in mapped:
        for subitem in item:
            result.append(subitem)
    return result


def parallel_map_with_progress(f: Callable[[T], U], i: Iterable[T]) -> List[U]:
    """
    Applies the function `func` to the input in parallel and returns the results in the
    same order as the input. This behaves a lot like the `pool.map` method in `multiprocessing`,
    but it will also show a progress meter for the user.
    """

    # tqdm does our progress bar, and it wraps the input in an iterable. Unfortunately, the
    # bar will advance at the *start* when something is drawn out of the iterable, rather than
    # at the end when the work unit is complete. This means the progress bar hits 100% too
    # early, but at least it advances. Trying to set up callbacks would require using apply_async
    # rather than map, which is a lot more work, since you have to collect the results yourself.
    # map_async() has a callback function, but it doesn't seem to do what we want, which is to say,
    # getting called once every time things are done. No idea why.

    # Why are we wrapping the iterable in a list? This seems to fix a problem where the parallel
    # mapper wants to measure how much input is in the iterable.
    pbar = tqdm(list(i))
    cpus = cpu_count()
    pool = Pool(cpu_count())

    result = pool.map(func=f, iterable=pbar)
    pool.close()

    return result
