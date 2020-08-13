from dataclasses import dataclass
from typing import TypeVar, Callable, Optional, Any, Generic

T = TypeVar("T")


@dataclass
class Memo(Generic[T]):
    """
    Support for lazy evaluation. See `make_memo_value` and `make_memo_lambda`.
    """

    _func: Optional[Callable[[], Optional[T]]]
    _value: Optional[T]

    @property
    def is_evaluated(self) -> bool:
        """
        Allows the caller to determine if this memo's lambda has been evaluated or not.
        """
        return self._func is None

    @property
    def contents(self) -> Optional[T]:
        """
        Will evaluate the internal lambda at most once, caching the resulting value,
        which is then always returned here. If something went wrong in the lambda,
        you would then get `None` as a result here.
        """
        if self._func is not None:
            self._value = self._func()
            self._func = None

        return self._value

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Memo):
            return False

        return self.contents == other.contents

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return self.contents.__hash__()

    def __str__(self) -> str:
        # It's useful, when looking through a debugger, for the stringified version of the memo
        # to indicate that the memo has not yet been evaluated.

        if self.is_evaluated:
            return f"Memo({str(self.contents)})"
        else:
            return "Memo(?)"


def make_memo_value(value: Optional[T]) -> Memo[T]:
    """
    Makes a Memo object wrapper around an existing value. The resulting Memo object has a `contents` property,
    which will return the value.

    For the lazy version, see `make_memo_lambda`.
    """
    return Memo(None, value)


def make_memo_lambda(func: Callable[[], Optional[T]]) -> Memo[T]:
    """
    Makes a Memo object (for lazy evaluation). The argument is a lambda, which will later return
    the desired value. The resulting Memo object has a `contents` property, which will evaluate
    the lambda only once, and thereafter cache the desired value internally.

    For the eager version, see `make_memo_value`.

    Warning: Python's lambdas don't capture values from their environment in the way that normal functional
    programming languages work. Here's an example of how this can go wrong. The lambdas are all capturing
    the same object for `i` rather than each individual value.
    >>> memos = [make_memo_lambda(lambda: i) for i in range(0, 10)]
    >>> [memo.contents for memo in memos]
    [9, 9, 9, 9, 9, 9, 9, 9, 9, 9]

    Here's how you might make copies, to ensure everything works how you would expect.
    >>> memos = [(lambda i: make_memo_lambda(lambda: i))(i) for i in range(0, 10)]
    >>> [memo.contents for memo in memos]
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    Additional details: https://louisabraham.github.io/articles/python-lambda-closures.html
    """
    return Memo(func, None)
