# This is an attempt to build a general-purpose Ray map-reduce feature. It tries to limit the number
# of tasks in flight to be only a small multiple of the number of active workers, and it tries to
# run reduction tasks quickly to minimize the lifetimes of intermediate results.
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, List, Tuple, Iterable, Sequence
from more_itertools import peekable

import ray
from dataclasses import dataclass
from ray import ObjectRef
from ray.actor import ActorHandle

from arlo_e2e.ray_progress import ProgressBar
from arlo_e2e.utils import shard_iterable_uniform, first_n, shard_list_uniform

T = TypeVar("T")
"""The input type for the map methods."""

R = TypeVar("R")
"""The output type for the map methods and the input/output type for the reduce methods."""

DEFAULT_INPUT_STR = "Inputs"
DEFAULT_REDUCTION_STR = "Reduction"


class MapReduceContext(ABC, Generic[T, R]):
    """
    When you wish to create a map-reduce computation, you need to define both a mapping
    function and a reduction function. Inevitably, you'll also want to have some sort
    of environment that assists you in doing these computations. To do this, you'll
    create an instance of this Python interface class, in which you can place any
    context you want (i.e., it's like the closure aspect of a lambda). The map-reduce
    library will replicate this to all of the remote nodes, and arrange to call the
    map and reduce methods. If somebody tries to run this on an input of zero-length,
    then the zero method will be called so there's at least *something* to return.

    Note: your instance of this class will be serialized and copied to all of the
    remote computation nodes. That means you can put whatever values you want in here,
    but you shouldn't mutate them and expect those changes to be persistent or visible.
    Also, Ray uses Python's "pickle" serializer, so anything you put in here needs
    to be compatible with that.

    When you create an instance of this abstract base class, you'll be specifying
    two types: `T` and `R`. The former is the type of the _input_ to the map function.
    The latter is the type of the _output_ of the `map` method and as well both the
    input and output types of the `reduce` method.

    For example, if you were converting strings to integers (via the `len` function)
    and then adding the lengths, you might define your `MapReduceContext` with::
      class StringLengthContext(MapReduceContext[str, int]):
        def map(self, input: str) -> int:
          return len(input)

        def reduce(self, input: List[int]) -> int:
          return sum(input)

        def zero(self) -> int:
          return 0

    And then run it with::
      rmr = RayMapReducer(context=StringLengthContext())
      total1 = rmr.map_reduce_iterable(["ABC", "DEFGH", "I", "", "J"]))  # --> 10
      total2 = rmr.map_reduce_iterable([]))  # --> 0
    """

    @abstractmethod
    def map(self, input: T) -> R:
        """
        A function that is given one value of type T and returns one value of type R.
        """
        pass

    @abstractmethod
    def reduce(self, inputs: List[R]) -> R:
        """
        A function that is given one or more values of type R and returns one value of type R.
        *This function must be commutative*, since no guarantees are made about the order
        in which computations will complete. You will never get zero inputs. In the degenerate
        case where map-reduce is run with exactly one input, this method might not be called.
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
def _ray_mr_map(
    context: MapReduceContext[T, R],
    progress_actor: Optional[ActorHandle],
    input_description: str,
    reduction_description: str,
    *inputs: T,
) -> R:  # pragma: no cover
    num_inputs = len(inputs)
    if num_inputs == 0:
        return context.zero()

    if progress_actor:
        progress_actor.update_num_concurrent.remote(input_description, 1)

    map_outputs: List[R] = []

    for i in inputs:
        map_outputs.append(context.map(i))
        if progress_actor:
            progress_actor.update_completed.remote(input_description, 1)

    # Even though we're changing gears from running a "map" to running a "reduction",
    # we're not going to monkey with the user-visible progressbars, because we're still
    # in the middle of a "map task", not a "reduce task". That said, we will increment
    # the total number of reductions that happened.

    result = context.reduce(map_outputs)
    if progress_actor:
        progress_actor.update_completed.remote(reduction_description, num_inputs)
        progress_actor.update_num_concurrent.remote(input_description, -1)

    return result


@ray.remote
def _ray_mr_reduce(
    context: MapReduceContext[T, R],
    progress_actor: Optional[ActorHandle],
    reduction_description: str,
    *inputs: R,
) -> R:  # pragma: no cover
    num_inputs = len(inputs)

    if num_inputs == 0:
        return context.zero()

    if progress_actor:
        progress_actor.update_num_concurrent.remote(reduction_description, 1)

    result = context.reduce(list(inputs))

    if progress_actor:
        progress_actor.update_completed.remote(reduction_description, num_inputs)
        progress_actor.update_num_concurrent.remote(reduction_description, -1)

    return result


@dataclass(eq=True, unsafe_hash=True)
class RayMapReducer(Generic[T, R]):
    """
    This is the handle for launching a map-reduce computation. You create one of these
    using the constructor, and afterward you call the `map_reduce_iterable` or `map_reduce_list`
    method with your inputs. All of the parallelism is handled inside, and all the calls
    into your custom code, supplied via the `MapReduceContext` instance you'll create,
    will run in parallel.

    A useful way to think about this: your instance of `MapReduceContext` provides the environment
    in which you're evaluating the map and reduce functions, but knows absolutely nothing about
    the Ray library or about the ordering in which it's executed.

    Your instance of `RayMapReducer` includes this context as well as variables that define
    exactly how the computation will be sharded up and distributed, whether there will be a
    progress bar, and so forth.
    """

    _context: MapReduceContext[T, R]
    _use_progressbar: bool
    _input_description: str
    _reduction_description: str
    _max_tasks: int
    _map_shard_size: int
    _reduce_shard_size: int

    def map_reduce_list(self, input: Sequence[T]) -> R:
        """
        Given zero or more inputs, passed as a Python list or other "sized" input sequence, runs
        the map and reduce tasks.
        """

        return self.map_reduce_iterable(input, len(input))

    def map_reduce_iterable(self, input: Iterable[T], num_inputs: int) -> R:
        """
        Given zero or more inputs, passed as a Python iterable, runs the map and reduce tasks.
        Since the input can be *any* iterable, we can't just directly ask it for its length,
        so the length needs to be passed here as another parameter. If your input knows its
        size, then you can use `map_reduce_list` instead.
        """

        input = peekable(input)
        if not input:
            return self._context.zero()

        progressbar: Optional[ProgressBar] = None
        progress_actor: Optional[ActorHandle] = None

        if self._use_progressbar:
            progressbar = ProgressBar(
                {
                    self._input_description: num_inputs,
                    self._reduction_description: num_inputs,
                }
            )
            progress_actor = progressbar.actor

        pending_map_shards = peekable(
            shard_iterable_uniform(input, self._map_shard_size, num_inputs)
        )
        running_jobs: List[ObjectRef] = []
        ready_refs: List[ObjectRef] = []

        context_ref = ray.put(self._context)
        input_desc_ref = ray.put(self._input_description)
        reduction_desc_ref = ray.put(self._reduction_description)
        iterations = 0
        result: Optional[ObjectRef] = None

        while pending_map_shards or len(running_jobs) > 0 or len(ready_refs) > 0:
            if progressbar:
                progressbar.print_update()
            iterations += 1

            # print(
            #     f"{iterations:4d}: Pending map shards? {bool(pending_map_shards)}, running jobs: {len(running_jobs)}"
            # )

            # first, see if we can add anything new to the job queue
            if (len(running_jobs) < self._max_tasks) and pending_map_shards:
                num_new_jobs = self._max_tasks - len(running_jobs)
                shards_to_launch = first_n(pending_map_shards, num_new_jobs)

                new_job_refs = [
                    _ray_mr_map.remote(
                        context_ref,
                        progress_actor,
                        input_desc_ref,
                        reduction_desc_ref,
                        *shard,
                    )
                    for shard in shards_to_launch
                ]
                running_jobs = new_job_refs + running_jobs

            # Next, we'll wait for half a second and see how we're doing.
            tmp: Tuple[List[ObjectRef], List[ObjectRef]] = ray.wait(
                running_jobs,
                num_returns=len(running_jobs),
                timeout=0.5,
                fetch_local=False,
            )

            new_ready_refs, pending_refs = tmp
            ready_refs = ready_refs + new_ready_refs
            num_ready_refs = len(ready_refs)
            num_pending_refs = len(pending_refs)

            if (
                num_ready_refs == 1
                and num_pending_refs == 0
                and (not pending_map_shards)
            ):
                # terminal case: we have one result and nothing pending; we're done!
                result = ready_refs[0]
                break

            if num_ready_refs >= 2:
                # general case: we have some results to reduce

                reduce_shards = list(
                    shard_list_uniform(ready_refs, self._reduce_shard_size)
                )
                size_one_shards = [s for s in reduce_shards if len(s) == 1]
                usable_shards = [s for s in reduce_shards if len(s) > 1]
                total_usable = sum(len(s) for s in usable_shards)

                if progress_actor:
                    progress_actor.update_total.remote(
                        self._reduction_description, total_usable
                    )

                partial_reductions = [
                    _ray_mr_reduce.remote(
                        context_ref, progress_actor, reduction_desc_ref, *rs
                    )
                    for rs in usable_shards
                ]

                running_jobs = partial_reductions + pending_refs
                ready_refs = [s[0] for s in size_one_shards]
            else:
                # Annoying case: we have exactly one ready result, one or more pending
                # results, and nothing to reduce, so we'll just go back and ray.wait
                # again for more results to show up.
                running_jobs = pending_refs
                pass

        assert result is not None, "reducer fail: exited loop with no result!"

        if progressbar:
            progressbar.close()
        result_local: R = ray.get(result)
        return result_local

    def __init__(
        self,
        context: MapReduceContext[T, R],
        use_progressbar: bool = False,
        input_description: str = DEFAULT_INPUT_STR,
        reduction_description: str = DEFAULT_REDUCTION_STR,
        max_tasks: int = 5000,
        map_shard_size: int = 4,
        reduce_shard_size: int = 10,
    ):
        """
        This is the constructor for a RayMapReducer, which is used for subsequent map-reduce computations.
        @param context: An instance of your own class which implements `MapReduceContext`,
          providing the three methods needed for mapping, reducing, and getting a suitable zero.
        @param use_progressbar: If true, a progressbar will be created to give updates as the computation runs.
        @param input_description: A textual description of the input type (e.g., "Ballots"), used by the
          progressbar.
        @param reduction_description: A textual description of the output type (e.g., "Tallies"), used by the
          progressbar.
        @param max_tasks: The maximum number of concurrent tasks to send to the Ray cluster. Should be at least
          as large as the number of workers.
        @param map_shard_size: The number of map computations to run as part of the same task. The results will
          be immediately reduced within the same task, saving network bandwidth.
        @param reduce_shard_size: The number of reduce inputs to run as a single task.
        """
        assert reduce_shard_size > 1, "reduce_shard_size must be greater than 1"
        assert map_shard_size >= 1, "map_shard_size must be at least 1"
        assert max_tasks >= 1, "max_tasks must be at least 1"

        self._context = context
        self._input_description = input_description
        self._reduction_description = reduction_description
        self._max_tasks = max_tasks
        self._map_shard_size = map_shard_size
        self._reduce_shard_size = reduce_shard_size
        self._use_progressbar = use_progressbar
        self._progressbar = None
