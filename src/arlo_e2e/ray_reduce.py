from typing import TypeVar, Iterable, Callable, Optional, List, Tuple

import ray
from mypy_extensions import VarArg
from ray import ObjectRef

from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.ray_progress import ProgressBar
from arlo_e2e.utils import shard_list_uniform

T = TypeVar("T")
RFA = TypeVar("RFA")


def ray_reduce(
    inputs: Iterable[ObjectRef[T]],
    shard_size: int,
    reducer_first_arg: RFA,
    reducer: Callable[[RFA, VarArg(ObjectRef[T])], ObjectRef[T]],
    progressbar: Optional[ProgressBar] = None,
    progressbar_key: Optional[str] = None,
    timeout: float = None,
    verbose: bool = False,
) -> ObjectRef[T]:
    """
    Given a list of inputs and a Ray remote reducer, manages the Ray cluster to wait for the values
    when they're ready, and call the reducer to ultimately get down to a single value.

    The `shard_size` parameter specifies how many inputs should be fed to each call to the reducer.
    Since the available data will vary, the actual number fed to the reducer will be at least two
    and at most `shard_size`.

    The `timeout` specifies the number of seconds to wait for results to become available. If
    `shard_size*shard_size` results are available earlier, that will take precedence. Otherwise, as long as
    at least two results are available when the timeout happens, at least one reducer will be dispatched.

    (Why `shard_size*shard_size`? If `shard_size` was 10, this means we'll dispatch ten calls to the
    reducer with ten inputs each, which means fewer trips through `ray.wait`. The timeout takes
    precedence over this, guaranteeing a minimum dispatch rate.)

    The `reducer` is a Ray remote method reference that takes a given first argument of whatever
    type and then a varargs sequence of objectrefs, and returns an objectref. So, if you had
    code that looked like:

    ```
    @ray.remote
    def my_reducer(config: Config, *inputs: MyDataType) -> MyDataType:
        ...
    ```

    And let's say you're mapping some remote function to generate those values and later want
    to reduce them. That code might look like this:
    ```
    @ray.remote
    def my_mapper(input: SomethingElse) -> MyDataType:
        ...

    def run_everything(config: Config, inputs: Iterable[SomethingElse]) -> MyDataType:
        map_refs = [my_mapper.remote(i) for i in inputs]
        return ray.get(ray_reduce(map_refs, 10, config, my_reducer.remote))
    ```

    If your `reducer_first_arg` corresponds to some large object that you don't want to serialize
    over and over, you could of course call `ray_put` on it first and pass that along.

    Important assumption: the `reducer` function needs to be *associative* and *commutative*.
    Ordering from the original list of inputs is *not* maintained.

    Optional feature: integration with the progressbar in `ray_progress`. Just pass in the
    ProgressBar as well as the `key` string that you want to use. Whenever more work
    is being dispatched, the progressbar's total amount of work is updated by the dispatcher here.
    The work completion notification is *not* handled here. That needs to be done by the remote
    reducer. (Why? Because it might want to update the progressbar for each element in the shard
    while here we could only see when the whole shard is completed.)
    """

    # TODO: generalize this code so the `reducer_first_arg` is wrapped up in the reducer.
    #   This seems like a job for `kwargs`. Deal with that after everything else works.

    assert (
        progressbar_key and progressbar
    ) or not progressbar, "progress bar requires a key string"

    assert shard_size > 1, "shard_size must be greater than one"

    iteration_count = 0

    inputs = list(inputs)

    result: Optional[ObjectRef[T]] = None

    while inputs:
        iteration_count += 1
        num_inputs = len(inputs)
        tmp: Tuple[List[ObjectRef[T]], List[ObjectRef[T]]] = ray.wait(
            inputs, num_returns=shard_size * shard_size, timeout=timeout
        )
        ready_refs, pending_refs = tmp
        num_ready_refs = len(ready_refs)
        num_pending_refs = len(pending_refs)
        assert (
            num_inputs == num_pending_refs + num_ready_refs
        ), "ray.wait fail: we lost some inputs!"

        log_and_print(
            f"Reducer iteration {iteration_count}: ready({num_ready_refs}), pending({num_pending_refs})",
            verbose=verbose,
        )

        if num_ready_refs == 1 and num_pending_refs == 0:
            # terminal case: we have one result ready and nothing pending; we're done!
            result = ready_refs[0]
            break
        if num_ready_refs >= 2:
            # general case: we have at least two results ready

            shards = shard_list_uniform(ready_refs, shard_size)
            size_one_shards = [s for s in shards if len(s) == 1]
            usable_shards = [s for s in shards if len(s) > 1]
            total_usable = sum(len(s) for s in usable_shards)

            if progressbar:
                progressbar.actor.remote.update_total(progressbar_key, total_usable)

            # dispatches jobs to remote workers, returns immediately with ObjectRefs
            partial_results = [reducer(reducer_first_arg, *s) for s in shards]

            if progressbar:
                progressbar.print_update()

            inputs = list(
                partial_results + pending_refs + [x[0] for x in size_one_shards]
            )

            assert len(inputs) < num_inputs, "reducer fail: we didn't shrink the inputs"
        else:
            # annoying case: we have exactly one result and nothing useful to do with it
            pass

    assert result is not None, "reducer fail: somehow exited the loop with no result"
    return result