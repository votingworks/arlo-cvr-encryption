from typing import TypeVar, Callable, Sequence, Tuple

T = TypeVar("T")
U = TypeVar("U")


# Source:
# https://stackoverflow.com/questions/53509826/equivalent-to-pyspark-flatmap-in-python
def flatmap(f: Callable[[T], Sequence[U]], li: Sequence[T]) -> Sequence[U]:
    """
    General-purpose flatmapping on sequences/lists/iterables. The lambda is
    expected to return a list of items for each input in the original list `li`,
    and all those lists are concatenated together.
    """
    mapped = map(f, li)
    flattened = _flatten_single_dim(mapped)
    yield from flattened


def _flatten_single_dim(mapped: Sequence[Sequence[U]]) -> Sequence[U]:
    for item in mapped:
        for subitem in item:
            yield subitem


class UidMaker:
    """
    We're going to need a lot object-ids for the ElectionGuard library, so
    the easiest thing is to have a tool that will wrap up a "prefix" with
    a "counter", letting us ask for the "next" UID any time we want one.
    """

    prefix: str = ""
    counter: int = 0

    def __init__(self, prefix: str):
        self.prefix = prefix
        self.counter = 0

    def next_int(self) -> Tuple[int, str]:
        """
        Fetches a tuple of the current counter and a UID string built from
        that counter. Also increments the internal counter.
        """
        c = self.counter
        self.counter += 1
        return c, f"{self.prefix}{c}"

    def next(self) -> str:
        """
        Fetches a UID string and increments the internal counter.
        """
        return self.next_int()[1]
