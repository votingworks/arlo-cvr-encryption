# This is an attempt to build a general-purpose Ray map-reduce feature. It tries to limit the number
# of tasks in flight to be only a small multiple of the number of active workers, and it tries to
# run reduction tasks quickly to minimize the lifetimes of intermediate results.
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, List, Tuple

import ray
from dataclasses import dataclass
from ray import ObjectRef
from ray.actor import ActorHandle

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

    Note: your instance of this class will be serialized and copied to all of the
    remote computation nodes. That means you can put whatever values you want in here,
    but you shouldn't mutate them and expect those changes to be persistent or visible.
    """

    @abstractmethod
    def map(self, input: T) -> R:
        """
        A function that is given one value of type T and returns one value of type R.
        """
        pass

    @abstractmethod
    def reduce(self, *inputs: R) -> R:
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
) -> R:
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

    result = context.reduce(*map_outputs)
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
) -> R:
    num_inputs = len(inputs)

    if num_inputs == 0:
        return context.zero()

    if progress_actor:
        progress_actor.update_num_concurrent.remote(reduction_description, 1)

    result = context.reduce(*inputs)

    if progress_actor:
        progress_actor.update_completed.remote(reduction_description, num_inputs)
        progress_actor.update_num_concurrent.remote(reduction_description, -1)

    return result


@dataclass(eq=True, unsafe_hash=True)
class RayMapReducer(Generic[T, R]):
    """
    This is the handle for launching a map-reduce computation. You create one of these
    using the constructor, and afterward you call the `map_reduce` method with your inputs.
    All of the parallelism is handled inside, and all the calls into your custom code,
    supplied via the `MapReduceContext` instance you'll create, will run in parallel.
    """

    _context: MapReduceContext[T, R]
    _use_progressbar: bool
    _input_description: str
    _reduction_description: str
    _max_tasks: int
    _map_shard_size: int
    _reduce_shard_size: int

    def map_reduce(self, input: List[T]) -> R:
        """
        Given zero or more inputs, passed as a Python list, runs the map and reduce tasks.
        """
        return self.map_reduce_vararg(*input)

    def map_reduce_vararg(self, *input: T) -> R:
        """
        Given zero or more inputs, passed vararg style, runs the map and reduce tasks.

        Note: if you have a list of inputs, you should call `my_map_reducer.map_reduce(*inputs)`,
        to ensure that the list is treated in vararg style.
        """
        num_inputs = len(input)
        # log_and_print(f"Launching map-reduce task with {num_inputs} inputs")
        if num_inputs == 0:
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

        pending_map_shards = shard_list_uniform(input, self._map_shard_size)
        running_jobs: List[ObjectRef] = []
        ready_refs: List[ObjectRef] = []

        context_ref = ray.put(self._context)
        input_desc_ref = ray.put(self._input_description)
        reduction_desc_ref = ray.put(self._reduction_description)
        iterations = 0
        result: Optional[ObjectRef] = None

        while (
            len(pending_map_shards) > 0 or len(running_jobs) > 0 or len(ready_refs) > 0
        ):
            if progressbar:
                progressbar.print_update()
            iterations += 1
            # print(
            #     f"{iterations:4d}: Pending map shards: {len(pending_map_shards)}, running jobs: {len(running_jobs)}"
            # )

            # first, see if we can add anything new to the job queue
            if len(running_jobs) < self._max_tasks and len(pending_map_shards) > 0:
                num_new_jobs = self._max_tasks - len(running_jobs)
                shards_to_launch = pending_map_shards[0:num_new_jobs]
                pending_map_shards = pending_map_shards[num_new_jobs:]
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
                running_jobs, num_returns=len(running_jobs), timeout=0.5
            )
            # TODO: Ray version 2 (dev builds) add an option here, fetch_local=False,
            #  which seems like it would be relevant to us. It's not available in the
            #  version 1.1 release. Unclear whether that fetching is happening in
            #  the 1.1 release of Ray or not. Hopefully not.

            new_ready_refs, pending_refs = tmp
            ready_refs = ready_refs + new_ready_refs
            num_ready_refs = len(ready_refs)
            num_pending_refs = len(pending_refs)

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
