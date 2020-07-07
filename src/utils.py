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


def _callback_func(pbar: tqdm, result: U) -> None:
    pbar.update()


def parallel_map_with_progress(f: Callable[[T], U], i: Iterable[T]) -> List[U]:
    """
    Applies the function `func` to the input in parallel and returns the results in the
    same order as the input. This behaves a lot like the `pool.map` method in `multiprocessing`,
    but it will also show a progress meter for the user.
    """
    list_copy = list(i)  # not strictly necessary, but seems to work well
    pbar = tqdm(total=len(list_copy))
    cpus = cpu_count()
    pool = Pool(cpu_count())

    callback = functools.partial(_callback_func, pbar=pbar)
    result = pool.map_async(func=f, iterable=list_copy, callback=callback).get()
    pool.close()

    return result
