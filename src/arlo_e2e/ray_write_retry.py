import random
from asyncio import Event
from pathlib import PurePath
from time import sleep
from typing import Union, AnyStr, Optional

from electionguard.logs import log_warning, log_error
from ray.actor import ActorHandle

from arlo_e2e.eg_helpers import log_and_print

import ray


@ray.remote
class WriteRetryStatusActor:  # pragma: no cover
    # sadly, code coverage testing can't trace into Ray remote methods and actors
    num_failures: int
    num_pending: int
    event: Event
    failure_probability: float

    def __init__(self) -> None:
        self.event = Event()
        self.reset_to_zero()

    def reset_to_zero(self) -> None:
        self.num_failures = 0
        self.num_pending = 0
        self.failure_probability = 0.0

    def increment_pending(self) -> None:
        """
        Increments the internal number of pending writes.
        """
        self.num_pending += 1

    def decrement_pending(self, failure_happened: bool) -> None:
        """
        Decrements the internal number of pending writes and also tracks whether
        the write ultimately succeeded (pass `failure_happened=False`) or failed
        (pass `failure_happened=True`).
        """
        if failure_happened:
            self.num_failures += 1

        self.num_pending -= 1
        if self.num_pending == 0:
            self.event.set()

    def set_failure_probability(self, failure_probability: float) -> None:
        """
        ONLY USED FOR TESTING. DO NOT USE IN PRODUCTION.
        """
        if failure_probability < 0.0 or failure_probability > 1.0:
            log_error(f"totally bogus failure probability: {failure_probability:0.3f}")
        elif failure_probability > 0.0:
            log_warning(
                f"setting failure probability to {failure_probability:0.3f}, DO NOT USE IN PRODUCTION"
            )

        self.failure_probability = failure_probability

    def get_failure_probability(self) -> float:
        """
        ONLY USED FOR TESTING. DO NOT USE IN PRODUCTION.
        """
        return self.failure_probability

    async def wait_for_zero_pending(self) -> int:
        """
        Blocking call: waits until there are no more pending writes. Returns the
        total number of failures.
        """
        if self.num_pending > 0:
            await self.event.wait()
            self.event.clear()
        return self.num_failures


def _write_once(
    full_file_name: Union[str, PurePath],
    contents: AnyStr,
    counter: int,
) -> bool:
    """
    Internal function: returns True if the write succeeds. Logs an error and returns
    False if something went wrong.
    """
    fp = get_failure_probability_for_testing()
    if fp > 0.0:
        r = random.random()  # in the range [0.0, 1.0)
        if r < fp:
            log_and_print(
                f"test-induced write error: {full_file_name} (attempt #{counter})"
            )
            return False

    write_mode = "w" if isinstance(contents, str) else "wb"
    try:
        with open(full_file_name, write_mode) as f:
            f.write(contents)
        return True
    except Exception as e:
        log_and_print(
            f"failed to write {full_file_name} (attempt #{counter}): {str(e)}"
        )
        return False


@ray.remote
def r_delayed_write_file_with_retries(
    full_file_name: Union[str, PurePath],
    contents: AnyStr,
    num_attempts: int,
    initial_delay: float,
    delta_delay: float,
    counter: int,
    status_actor: Optional[ActorHandle],  # pragma: no cover
) -> None:
    # actual return type: Optional[ActorHandle[WriteRetryStatusActor]], but type params not supported

    for attempt in range(counter, num_attempts):
        sleep(initial_delay)
        success = _write_once(full_file_name, contents, attempt)
        if success:
            if status_actor:
                status_actor.decrement_pending.remote(False)
            return

        initial_delay += delta_delay

    log_and_print(f"giving up writing {full_file_name}: failed {num_attempts} times")
    if status_actor:
        status_actor.decrement_pending.remote(True)


__singleton_name = "WriteRetryStatusActorSingleton"
__status_actor: Optional[ActorHandle] = None


def reset_pending_state() -> None:
    """
    If you've finished one computation and are about to start another, this resets all
    the internal counters. Also, this will remove any actors, which will then be restarted
    if necessary.

    In a testing scenario, you might call this after a call to `ray.shutdown()`.
    """
    global __status_actor
    global __failure_probability
    global __local_failed_writes

    __status_actor = None
    __failure_probability = None
    __local_failed_writes = 0


def init_status_actor() -> None:
    """
    Helper function, called by our own ray_init_* routines, exactly once, to make the singleton
    WriteRetryStatusActor. If you want a handle to the actor, call `get_status_actor`.
    """
    global __status_actor
    __status_actor = WriteRetryStatusActor.options(name=__singleton_name).remote()  # type: ignore


def get_status_actor() -> ActorHandle:
    """
    Gets the global WriteRetryStatusActor singleton.
    """

    # We're using a "named actor" to hold our singleton.
    # https://docs.ray.io/en/master/actors.html#named-actors

    # - First we check if we have a saved reference in our process.
    #   Useful side effect: keeps the actor from getting garbage collected.
    # - Next, we see if it's already running somewhere else.
    # - If not, that means that initialization was done wrong.

    global __status_actor

    if __status_actor is None:
        __status_actor = ray.get_actor(__singleton_name)

    if __status_actor is None:
        log_and_print(
            "Configuration failure: we should have a status actor, but we don't."
        )
        exit(1)

    return __status_actor


__failure_probability: Optional[float] = None


def set_failure_probability_for_testing(failure_probability: float) -> None:
    """
    ONLY USE NON-ZERO HERE FOR TESTING.
    """

    global __failure_probability
    __failure_probability = failure_probability

    if ray.is_initialized():
        get_status_actor().set_failure_probability.remote(failure_probability)


def get_failure_probability_for_testing() -> float:
    """
    ONLY USE THIS FOR TESTING.
    """
    global __failure_probability

    if __failure_probability is not None:
        return __failure_probability

    if not ray.is_initialized():
        log_and_print(
            "Ray not initialized, so we're assuming a zero failure probability for writes."
        )
        __failure_probability = 0.0
    else:
        __failure_probability = ray.get(
            get_status_actor().get_failure_probability.remote()
        )

    return __failure_probability


__local_failed_writes = 0


def wait_for_zero_pending_writes() -> int:
    """
    Blocking call: waits until all pending writes are complete, then returns
    the number of failed writes (i.e., writes where the number of failures
    exceeded the number of retry attempts). If this is zero, then every write
    succeeded, eventually. If it's non-zero, then you should warn the user
    with the number, perhaps suggesting that something really bad happened.
    """
    global __local_failed_writes
    if ray.is_initialized():
        num_failures: int = ray.get(get_status_actor().wait_for_zero_pending.remote())
        return num_failures
    else:
        return __local_failed_writes


def write_file_with_retries(
    full_file_name: Union[str, PurePath],
    contents: AnyStr,  # bytes or str
    num_attempts: int = 1,
    initial_delay: float = 1.0,
    delta_delay: float = 1.0,
) -> None:
    """
    Helper function: given a fully resolved file path, or a path-like object describing
    a file location, writes the given contents to the a file of that name, and if it
    fails, tries it again and again (based on the `num_attempts` parameter). This works
    around occasional failures that happen, for no good reason, with s3fs-fuse in big
    clouds.

    If Ray is in use, this gets a little more interesting. If it fails, it will
    launch a Ray task to delay a bit and try again, allowing the initial call to
    return immediately, and the file will *eventually* get written out.

    The delay time, in seconds, starts with the `initial_delay` (default: 1 second)
    then grows by `delta_delay` (default: 1 second) each time.
    """
    global __local_failed_writes
    prev_exception = None

    if ray.is_initialized():
        status_actor = get_status_actor()
        if _write_once(full_file_name, contents, 1):
            return
        if status_actor:
            status_actor.increment_pending.remote()
        r_delayed_write_file_with_retries.remote(
            full_file_name,
            contents,
            num_attempts,
            initial_delay,
            delta_delay,
            2,
            status_actor,
        )

    else:
        for retry_number in range(1, num_attempts + 1):
            if _write_once(full_file_name, contents, retry_number):
                return
            sleep(initial_delay)
            initial_delay += delta_delay

        if num_attempts > 1:
            log_and_print(
                f"giving up writing {full_file_name}: failed {num_attempts} times"
            )
        else:
            log_and_print(f"giving up writing {full_file_name}")

        __local_failed_writes += 1

        if prev_exception:
            raise prev_exception
