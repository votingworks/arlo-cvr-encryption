# This is an attempt to build a general-purpose Ray map-reduce feature. It tries to limit the number
# of tasks in flight to be only a small multiple of the number of active workers, and it tries to
# run reduction tasks quickly to minimize the lifetimes of intermediate results.
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, List

import ray
from dataclasses import dataclass
from ray.actor import ActorHandle

from arlo_e2e.ray_progress import ProgressBar

T = TypeVar("T")
"""
The input type for the map methods.
"""

R = TypeVar("R")
"""
The output type for the map methods and the input/output type for the reduce methods.
"""

RUNNING_MAPS_STR = "Running Maps"
RUNNING_REDUCES_STR = "Running Reduces"
DEFAULT_INPUT_STR = "Inputs"
DEFAULT_REDUCTION_STR = "Reduction"


class MapReduceContext(ABC, Generic[T, R]):
    """
    When you wish to create a map-reduce computation, you need to define both a mapping
    function and a reduction function. Inevitably, you'll also want to have some sort
    of environment that assists you in doing these computations. To do this, you'll
    create an instance of this Python interface class, in which you can place any
    read-only context you want. The map-reduce library will replicate this to all of
    the remote nodes, and arrange to call the map and reduce methods.
    """

    @abstractmethod
    def map(self, input: T) -> R:
        """
        A function that is given one value of type T and returns one value of type R.
        """
        pass

    @abstractmethod
    def reduce(self, *input: R) -> R:
        """
        A function that is given one or more values of type R and returns one value of type R.
        *This function must be commutative*, since no guarantees are made about the order
        in which computations will complete, and they'll be reduced eagerly when available.
        You will never get zero inputs, but you may get one input.
        """
        pass

    @abstractmethod
    def zero(self) -> R:
        """
        A function of no arguments that returns the "zero" or "identity" over the type R.
        In the unlikely case that we're running a map-reduce over exactly zero inputs, this
        is what will be returned. In other cases, the zero is ignored and the other results
        are reduced with each other.
        """
        pass


@ray.remote
def r_map(
    context: MapReduceContext[T, R],
    progress_actor: Optional[ActorHandle],
    input_description: str,
    reduction_description: str,
    *inputs: T
) -> R:
    num_inputs = len(inputs)
    if num_inputs == 0:
        return context.zero()

    if progress_actor is not None:
        progress_actor.update_completed.remote(RUNNING_MAPS_STR, 1)

    map_outputs: List[R] = []

    for i in inputs:
        map_outputs.append(context.map(i))
        if progress_actor:
            progress_actor.update_completed.remote(input_description, 1)

    # Even though we're changing gears from running a "map" to running a "reduction",
    # we're not going to monkey with the user-visible progressbars, because we're still
    # in the middle of a "map task", not a "reduce task".

    result = context.reduce(*map_outputs)
    if progress_actor:
        progress_actor.update_completed.remote(reduction_description, num_inputs)

    if progress_actor is not None:
        progress_actor.update_completed.remote(RUNNING_MAPS_STR, -1)

    return result


@ray.remote
def r_reduce(
    context: MapReduceContext[T, R],
    progress_actor: Optional[ActorHandle],
    reduction_description: str,
    *inputs: R
) -> R:
    num_inputs = len(inputs)
    if num_inputs == 0:
        return context.zero()

    if progress_actor is not None:
        progress_actor.update_completed.remote(RUNNING_REDUCES_STR, 1)

    result = context.reduce(*inputs)

    if progress_actor:
        progress_actor.update_completed.remote(reduction_description, num_inputs)

    if progress_actor is not None:
        progress_actor.update_completed.remote(RUNNING_REDUCES_STR, -1)

    return result


@dataclass(eq=True, unsafe_hash=True)
class RayMapReducer(Generic[T, R]):
    """
    This is the handle for launching a map-reduce computation. You create one of these
    using `make_ray_map_reducer`, and afterward you use the defined methods to operate it.
    """
    _context: MapReduceContext[T, R]
    _use_progressbar: bool
    _input_description: str
    _reduction_description: str
    _max_tasks: int
    _map_shard_size: int
    _reduce_shard_size: int

    def run_map_reduce(self, *input: T) -> R:
        """
        Given zero or more inputs, passed vararg style, runs the map and reduce tasks.
        """
        num_inputs = len(input)
        if num_inputs == 0:
            return self._context.zero()

        progressbar: Optional[ProgressBar] = None

        if self._use_progressbar:
            progressbar = ProgressBar({
                self._input_description: num_inputs,
                self._reduction_description: num_inputs,
                RUNNING_MAPS_STR: 0,
                RUNNING_REDUCES_STR: 0
            })

        return self._context.zero()

    def __init__(
        self,
        context: MapReduceContext[T, R],
        use_progressbar: bool = False,
        input_description: str = DEFAULT_INPUT_STR,
        reduction_description: str = DEFAULT_REDUCTION_STR,
        max_tasks=5000,
        map_shard_size=4,
        reduce_shard_size=10,
    ):
        """
        This method creates a RayMapReducer, which is used for subsequent map-reduce computations.
        @param context: An instance of your own class which implements `MapReduceContext`, providing the three methods needed for mapping, reducing, and getting a suitable zero.
        @param use_progressbar: If true, a progressbar will be created to give updates as the computation runs
        @param input_description: A textual description of the input type (e.g., "Ballots"), used by the progressbar
        @param reduction_description: A textual description of the output type (e.g., "Tallies"), used by the progressbar
        @param max_tasks: The maximum number of concurrent tasks to send to the Ray cluster. Should be at least as large as the number of workers.
        @param map_shard_size: The number of map computations to run as part of the same task. The results will then be immediately reduced at the same time, saving bandwidth.
        @param reduce_shard_size: The number of reduce computations to run at once.
        @return: a single instance of the result type, representing the completion of the reduction process
        """
        self._context = context
        self._input_description = input_description
        self._reduction_description = reduction_description
        self._max_tasks = max_tasks
        self._map_shard_size = map_shard_size
        self._reduce_shard_size = reduce_shard_size
        self._use_progressbar = use_progressbar
        self._progressbar = None