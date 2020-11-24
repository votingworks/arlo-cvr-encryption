import random
from asyncio import Event
from pathlib import PurePath
from time import sleep
from typing import Union, AnyStr, Optional

from electionguard.logs import log_warning
from ray.actor import ActorHandle

from arlo_e2e.eg_helpers import log_and_print

import ray


@ray.remote
class WriteRetryStatusActor:
    num_failures: int
    num_pending: int
    event: Event
    failure_probability: float

    def __init__(self) -> None:
        self.num_failures = 0
        self.num_pending = 0
        self.failure_probability = 0.0
        self.event = Event()

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
        log_warning(f"setting failure probability to {failure_probability:0.3f}")
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
    initial_delay: int,
    counter: int,
    status_actor: Optional[ActorHandle],  # WriteRetryStatusActor
) -> None:
    sleep(initial_delay)
    success = _write_once(full_file_name, contents, counter)
    if success:
        if status_actor:
            status_actor.decrement_pending.remote(False)
        return

    if counter >= num_attempts:
        if status_actor:
            status_actor.decrement_pending.remote(True)
        log_and_print(
            f"giving up writing {full_file_name}: failed {num_attempts} times"
        )
        return


__status_actor: Optional[ActorHandle] = None


def get_status_actor() -> ActorHandle:
    """
    Gets the global WriteRetryStatusActor singleton. If it doesn't exist, creates it.
    """

    global __status_actor
    singleton_name = "WriteRetryStatusActorSingleton"

    # We're using a "named actor" to hold our singleton.
    # https://docs.ray.io/en/master/actors.html#named-actors

    # - First we check if we have a saved reference in our process.
    #   Useful side effect: keeps the actor from getting garbage collected.
    # - Next, we see if it's already running somewhere else.
    # - Last chance, we launch it ourselves.

    if __status_actor is None:
        try:
            __status_actor = ray.get_actor(singleton_name)
        except ValueError:
            # it's not there, but that's fine
            pass
    if __status_actor is None:
        # mypy doesn't know about the "options" attribute, but it's there
        __status_actor = WriteRetryStatusActor.options(name=singleton_name).remote()  # type: ignore
    return __status_actor


__failure_probability = 0.0


def set_failure_probability_for_testing(failure_probability: float) -> None:
    """
    ONLY USE THIS FOR TESTING.
    """
    log_warning(f"setting failure probability to {failure_probability:0.3f}")
    global __failure_probability

    __failure_probability = failure_probability
    if ray.is_initialized():
        get_status_actor().set_failure_probability.remote(failure_probability)


def get_failure_probability_for_testing() -> float:
    """
    ONLY USE THIS FOR TESTING.
    """
    global __failure_probability

    if ray.is_initialized():
        __failure_probability = ray.get(
            get_status_actor().get_failure_probability.remote()
        )

    return __failure_probability


def write_file_with_retries(
    full_file_name: Union[str, PurePath],
    contents: AnyStr,  # bytes or str
    num_attempts: int = 1,
    initial_delay: int = 1,
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

    The delay time, in seconds, will be equal to the retry number, so first it waits
    one second, then two, etc. (You can use the optional `initial_delay` parameter
    to specify the first wait time, and thereafter it increments by one.)
    """
    prev_exception = None

    if ray.is_initialized():
        status_actor = get_status_actor()
        if _write_once(full_file_name, contents, 1):
            return
        if status_actor:
            status_actor.increment_pending.remote()
        r_delayed_write_file_with_retries.remote(
            full_file_name, contents, num_attempts, initial_delay, 2, status_actor
        )

    else:
        for retry_number in range(1, num_attempts + 1):
            if _write_once(full_file_name, contents, retry_number):
                return
            sleep(initial_delay)
            initial_delay += 1

        if num_attempts > 1:
            log_and_print(
                f"giving up writing {full_file_name}: failed {num_attempts} times"
            )
        if prev_exception:
            raise prev_exception
