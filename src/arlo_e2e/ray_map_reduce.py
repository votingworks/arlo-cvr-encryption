# This is an attempt to build a general-purpose Ray map-reduce feature. It tries to limit the number
# of tasks in flight to be only a small multiple of the number of active workers, and it tries to
# run reduction tasks quickly to minimize the lifetimes of intermediate results.
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, List, Tuple
from timeit import default_timer as timer

import ray
from dataclasses import dataclass
from ray import ObjectRef
from ray.actor import ActorHandle

from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.ray_progress import ProgressBar
from arlo_e2e.utils import shard_list_uniform

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
    *inputs: T,
) -> R:
    num_inputs = len(inputs)
    if num_inputs == 0:
        return context.zero()

    if progress_actor:
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

    if progress_actor:
        progress_actor.update_completed.remote(RUNNING_MAPS_STR, -1)

    return result


@ray.remote
def r_reduce(
    context: MapReduceContext[T, R],
    progress_actor: Optional[ActorHandle],
    reduction_description: str,
    *inputs: R,
) -> R:
    num_inputs = len(inputs)
    if num_inputs == 0:
        return context.zero()

    if progress_actor:
        progress_actor.update_completed.remote(RUNNING_REDUCES_STR, 1)

    result = context.reduce(*inputs)

    if progress_actor:
        progress_actor.update_completed.remote(reduction_description, num_inputs)
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

    def map_reduce(self, *input: T) -> R:
        """
        Given zero or more inputs, passed vararg style, runs the map and reduce tasks.
        """
        num_inputs = len(input)
        if num_inputs == 0:
            return self._context.zero()

        progressbar: Optional[ProgressBar] = None
        progress_actor: Optional[ActorHandle] = None

        log_and_print(f"Launching map-reduce task with {num_inputs} inputs")
        start_time = timer()

        if self._use_progressbar:
            progressbar = ProgressBar(
                {
                    self._input_description: num_inputs,
                    self._reduction_description: num_inputs,
                    RUNNING_MAPS_STR: 0,
                    RUNNING_REDUCES_STR: 0,
                }
            )
            progress_actor = progressbar.actor

        pending_map_shards = shard_list_uniform(input, self._map_shard_size)
        running_jobs: List[ObjectRef] = []

        context_ref = ray.put(self._context)
        input_desc_ref = ray.put(self._input_description)
        reduction_desc_ref = ray.put(self._reduction_description)
        iterations = 0
        result: Optional[ObjectRef] = None

        while len(pending_map_shards) > 0 or len(running_jobs) > 0:
            if progressbar:
                progressbar.print_update()
            iterations += 1

            # first, see if we can add anything new to the job queue
            if len(running_jobs) < self._max_tasks and len(pending_map_shards) > 0:
                num_new_jobs = self._max_tasks - len(running_jobs)
                shards_to_launch = pending_map_shards[0:num_new_jobs]
                pending_map_shards = pending_map_shards[num_new_jobs:]
                new_job_refs = [
                    r_map.remote(
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
                running_jobs, num_returns=len(running_jobs), timeout=0.5
            )
            ready_refs, pending_refs = tmp
            num_ready_refs = len(ready_refs)
            num_pending_refs = len(pending_refs)
            assert (
                len(running_jobs) == num_pending_refs + num_ready_refs
            ), "ray.wait fail: we lost some inputs!"
            # TODO: Ray version 2 (dev builds) add an option here, fetch_local=False,
            #  which seems like it would be relevant to us. It's not available in the
            #  version 1.1 release. Unclear whether that fetching is happening in
            #  the release versions of Ray or not. Hopefully not.

            if (
                num_ready_refs == 1
                and num_pending_refs == 0
                and len(pending_map_shards) == 0
            ):
                # terminal case: we have one result and nothing pending; we're done!
                result = ready_refs[0]
                break

            if num_ready_refs >= 2:
                # general case: we have some results to reduce

                reduce_shards = shard_list_uniform(ready_refs, self._reduce_shard_size)
                size_one_shards = [s for s in reduce_shards if len(s) == 1]
                usable_shards = [s for s in reduce_shards if len(s) > 1]
                total_usable = sum(len(s) for s in usable_shards)

                if progress_actor:
                    progress_actor.update_total.remote(
                        self._reduction_description, total_usable
                    )

                partial_reductions = [
                    r_reduce.remote(
                        context_ref, progress_actor, reduction_desc_ref, *rs
                    )
                    for rs in usable_shards
                ]

                running_jobs = list(
                    partial_reductions + pending_refs + [x[0] for x in size_one_shards]
                )
            else:
                # annoying case: we have exactly one result and nothing useful to do with it,
                # so we'll just go back and ray.wait again for more results to show up.
                pass

        assert (
            result is not None
        ), "reducer fail: somehow exited the loop with no result!"

        end_time = timer()
        log_and_print(
            f"Map-reduce task completed {num_inputs} maps, with {iterations} iterations, in {num_inputs / (end_time - start_time):0.3f} inputs/sec"
        )
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
