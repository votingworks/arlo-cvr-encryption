from typing import (
    TypeVar,
    Callable,
    Sequence,
    List,
    Iterable,
)

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
