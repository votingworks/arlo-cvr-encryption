import os
import random
from asyncio import Event
from os import stat, path
from pathlib import PurePath, Path
from stat import S_ISREG
from time import sleep
from typing import (
    Union,
    AnyStr,
    Optional,
    List,
    Type,
    TypeVar,
    Dict,
    NamedTuple,
    Iterator,
)

import ray
from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.logs import log_warning, log_error, log_info
from electionguard.serializable import Serializable
from electionguard.utils import flatmap_optional
from jsons import DecodeError, UnfulfilledArgumentError
from ray.actor import ActorHandle

from arlo_e2e.constants import BALLOT_FILENAME_PREFIX_DIGITS
from arlo_e2e.eg_helpers import log_and_print

S = TypeVar("S", bound=Serializable)


def _fail_if_running_in_production() -> None:
    # Various stackoverflow posts suggest that looking at sys.modules to detect
    # the presence of a test framework is the way to go here. Others will set
    # an environment variable from their test harness or even look at sys.argv.

    # TODO: figure out why this doesn't work

    # if not ("unittest" in sys.modules.keys() or "pytest" in sys.modules.keys()):
    #     raise RuntimeError("test-only feature used in production!")
    pass


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

        _fail_if_running_in_production()

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
    status_actor: Optional[ActorHandle],
) -> None:  # pragma: no cover
    # actual return type: Optional[ActorHandle[WriteRetryStatusActor]], but type params not supported

    for attempt in range(counter, num_attempts):
        sleep(initial_delay)
        success = _write_once(full_file_name, contents, attempt)
        if success:
            log_and_print(f"attempt #{attempt}: successfully wrote {full_file_name}")
            if status_actor:
                status_actor.decrement_pending.remote(False)
            return

        initial_delay += delta_delay

    log_and_print(f"giving up writing {full_file_name}: failed {num_attempts} times")
    if status_actor:
        status_actor.decrement_pending.remote(True)


__singleton_name = "WriteRetryStatusActorSingleton"
__status_actor: Optional[ActorHandle] = None


def reset_status_actor() -> None:
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
        raise RuntimeError("No status actor")

    return __status_actor


__failure_probability: Optional[float] = None


def set_failure_probability_for_testing(failure_probability: float) -> None:
    """
    ONLY USE NON-ZERO HERE FOR TESTING.
    """

    _fail_if_running_in_production()

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
        log_info(
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


def ray_write_json_file(
    file_name: str,
    content_obj: Serializable,
    root_dir: str,
    subdirectories: List[str] = None,
    num_retries: int = 1,
) -> None:
    """
    Given a filename, subdirectory, and an ElectionGuard "serializable" object, serializes it and
    writes the contents out to the file.

    :param subdirectories: paths to be introduced between `root_dir` and the file; empty-list means no subdirectory
    :param file_name: name of the file, including any suffix
    :param content_obj: any ElectionGuard "Serializable" object
    :param root_dir: optional root directory, below which the files are written
    :param num_retries: how many attempts to make writing the file; works around occasional network filesystem glitches
    """

    # This used to be called write_json_file, but there's a method of the same exact
    # name inside ElectionGuard, so it's sensible to give this one a different name,
    # especially since it's somewhat Ray-specific, dealing with S3 write failures, etc.

    json_txt = content_obj.to_json(strip_privates=True)
    ray_write_file(
        file_name,
        json_txt,
        root_dir=root_dir,
        subdirectories=subdirectories,
        num_retries=num_retries,
    )


def ray_write_ciphertext_ballot(
    ballot: CiphertextAcceptedBallot,
    root_dir: str,
    num_retries: int = 1,
) -> None:
    """
    Given a ciphertext ballot, writes the ballot to disk in the "ballots" subdirectory.
    :param ballot: any "accepted" ballot, ready to be written out
    :param root_dir: mandatory root directory, in which the "ballots" subdirectory will appear
    :param num_retries: how many attempts to make writing the file; works around occasional network filesystem glitches
    """
    ballot_name = ballot.object_id
    ballot_name_prefix = ballot_name[0:BALLOT_FILENAME_PREFIX_DIGITS]
    ray_write_json_file(
        ballot_name + ".json",
        ballot,
        root_dir=root_dir,
        subdirectories=["ballots", ballot_name_prefix],
        num_retries=num_retries,
    )


def ray_write_file(
    file_name: str,
    file_contents: AnyStr,
    root_dir: str,
    subdirectories: List[str] = None,
    num_retries: int = 1,
) -> None:
    """
    Given a filename, subdirectory, and desired contents of the file, writes the contents out to the file.

    :param subdirectories: paths to be introduced between `root_dir` and the file; empty-list means no subdirectory
    :param file_name: name of the file, including any suffix
    :param file_contents: string to be written to the file
    :param root_dir: optional root directory, below which the files are written
    :param num_retries: how many attempts to make writing the file; works around occasional network filesystem glitches
    """

    # This used to be called write_file, but that name is used in many other modules,
    # creating lots of opportunity for confusion.

    if subdirectories is None:
        subdirectories = []

    mkdir_list_helper(root_dir, subdirectories, num_retries=num_retries)

    if isinstance(file_contents, bytes):
        file_utf8_bytes = file_contents
    else:
        file_utf8_bytes = file_contents.encode("utf-8")

    ray_write_file_with_retries(
        file_name,
        file_utf8_bytes,
        root_dir=root_dir,
        subdirectories=subdirectories,
        num_attempts=num_retries,
        initial_delay=1,
    )


def ray_write_file_with_retries(
    file_name: str,
    contents: AnyStr,  # bytes or str
    root_dir: str,
    subdirectories: List[str] = None,
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

    if subdirectories is None:
        subdirectories = []

    full_file_name = compose_filename(root_dir, file_name, subdirectories)

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


def unlink_helper(p: Union[str, PurePath], num_retries: int = 1) -> None:
    """Wrapper around os.unlink, with retries for errors, and won't complain if the file isn't there."""

    prev_exception = None
    p = Path(p)

    for attempt in range(0, num_retries):
        try:
            p.unlink(missing_ok=True)
            return
        except Exception as e:
            prev_exception = e
            log_and_print(f"failed to remove file {p} (attempt {attempt}): {str(e)}")

            # S3 failures seem to happen at the same time; sleeping might help.
            sleep(1)

    if num_retries > 1:
        log_and_print(
            f"failed to remove file {p} after {num_retries} attempts, failing"
        )

    if prev_exception:
        raise prev_exception


def mkdir_helper(p: Union[str, PurePath], num_retries: int = 1) -> None:
    """
    Wrapper around `os.mkdir` that will work correctly even if the directory already exists.
    """
    prev_exception = None
    p = Path(p)

    for attempt in range(0, num_retries):
        try:
            p.mkdir(parents=True, exist_ok=True)
            return
        except Exception as e:
            prev_exception = e
            log_and_print(f"failed to make directory {p} (attempt {attempt}): {str(e)}")

            # S3 failures seem to happen at the same time; sleeping might help.
            sleep(1)

    if num_retries > 1:
        log_and_print(
            f"failed to make directory {p} after {num_retries} attempts, failing"
        )

    if prev_exception:
        raise prev_exception


def mkdir_list_helper(
    root_dir: str, paths: List[str] = None, num_retries: int = 1
) -> None:
    """
    Like mkdir_helper, but takes a list of strings, each of which corresponds to a directory
    to make if it doesn't exist, each within the previous. So, `mkdir_list_helper('foo', ['a', 'b', 'c'])`
    would create `foo`, then `foo/a`, `foo/a/b`, and `foo/a/b/c`.
    """

    if paths is not None:
        mkdir_helper(PurePath(root_dir, *paths), num_retries=num_retries)
    else:
        mkdir_helper(PurePath(root_dir), num_retries=num_retries)


def compose_filename(
    root_dir: Union[PurePath, str], file_name: str, subdirectories: List[str] = None
) -> PurePath:
    """
    Helper function: given a root directory, a file name, and an optional list of subdirectories
    might go in between (empty-list implies no subdirectory), returns a string corresponding
    to the full filename, properly joined, for the local operating system. On Windows,
    it will have backslashes, while on Unix system it will have forward slashes.
    """

    if not isinstance(root_dir, PurePath):
        root_dir = PurePath(root_dir)

    if subdirectories is not None:
        root_dir = root_dir.joinpath(*subdirectories)
    return root_dir / file_name


def file_exists_helper(
    root_dir: str,
    file_name: Union[str, PurePath],
    subdirectories: List[str] = None,
) -> bool:
    """
    Checks whether the desired file exists.

    Note: if the file_name is a path-like object, the root_dir and subdirectories
    are ignored and the file is directly loaded.

    :param root_dir: top-level directory where we'll be reading files
    :param file_name: name of the file, including any suffix
    :param subdirectories: path elements to be introduced between `root_dir` and the file; empty-list means no subdirectory
    :returns: True if the file is a regular file with non-zero size, otherwise False
    """
    if isinstance(file_name, PurePath):
        full_name = file_name
    else:
        full_name = compose_filename(root_dir, file_name, subdirectories)

    try:
        s = stat(full_name)
        return s.st_size > 0 and S_ISREG(s.st_mode)
    except FileNotFoundError:
        return False
    except OSError:
        return False


def ray_load_file(
    root_dir: str,
    file_name: Union[str, PurePath],
    subdirectories: List[str] = None,
) -> Optional[str]:
    """
    Reads the requested file, by name, returning its contents as a Python string.

    Note: if the file_name is a path-like object, the root_dir and subdirectories
    are ignored and the file is directly loaded.

    :param root_dir: top-level directory where we'll be reading files
    :param file_name: name of the file, including any suffix
    :param subdirectories: path elements to be introduced between `root_dir` and the file; empty-list means no subdirectory
    :returns: the contents of the file, or `None` if there was an error
    """

    if isinstance(file_name, PurePath):
        full_name = file_name
    else:
        full_name = compose_filename(root_dir, file_name, subdirectories)

    try:
        s = stat(full_name)
        if s.st_size == 0:  # pragma: no cover
            log_error(f"The file ({full_name}) is empty")
            return None

        with open(full_name, "r") as f:
            data = f.read()

            return data
    except FileNotFoundError as e:
        log_error(f"Error reading file ({full_name}): {e}")
        return None
    except OSError as e:
        log_error(f"Error reading file ({full_name}): {e}")
        return None


def _decode_json_file_contents(json_str: str, class_handle: Type[S]) -> Optional[S]:
    """
    Wrapper around JSON deserialization. Given a string of JSON text and a handle to an
    ElectionGuard `Serializable` class, tries to decode the JSON into an instance of that
    class. If anything fails, the result will be `None`. No exceptions will be raised outside
    of this method, but all such failures will be logged to the ElectionGuard log.

    :param json_str: any JSON string
    :param class_handle: the class, itself, that we're trying to deserialize to
    :returns: the contents of the file, or `None` if there was an error
    """
    try:
        result = class_handle.from_json(json_str)
    except DecodeError as err:  # pragma: no cover
        log_error(f"Failed to decode an instance of {class_handle}: {err}")
        return None
    except UnfulfilledArgumentError as err:  # pragma: no cover
        log_error(f"Decoding failure for {class_handle}: {err}")
        return None

    return result


def ray_load_json_file(
    root_dir: str,
    file_name: Union[str, PurePath],
    class_handle: Type[S],
    subdirectories: List[str] = None,
) -> Optional[S]:
    """
    Wrapper around JSON deserialization that, given a directory name and file name (including
    the ".json" suffix) as well as an optional handle to the class type, will load the contents
    of the file and return an instance of the desired class, if it fits. If anything fails, the
    result should be `None`. No exceptions will be raised outside of this method, but all such
    errors will be logged as part of the ElectionGuard log.

    Note: if the file_name is actually a path-like object, the root_dir and subdirectories are ignored,
    and the path is directly loaded.

    :param root_dir: top-level directory where we'll be reading files
    :param file_name: name of the file, including the suffix, excluding any directories leading up to the file
    :param class_handle: the class, itself, that we're trying to deserialize to
    :param subdirectories: path elements to be introduced between `root_dir` and the file; empty-list means no subdirectory
    :returns: the contents of the file, or `None` if there was an error
    """
    file_contents = ray_load_file(root_dir, file_name, subdirectories)
    return flatmap_optional(
        file_contents, lambda f: _decode_json_file_contents(f, class_handle)
    )


class DirInfo(NamedTuple):
    plain_files: Dict[str, PurePath]
    """Mapping from local file names to `PurePath` objects usable to open those files."""

    directories: Dict[str, PurePath]
    """Mapping from local directory names to `PurePath` objects usable to open those files."""


def read_directory_contents(root_dir: str, subdirectories: List[str] = None) -> DirInfo:
    """
    Given a directory name, and optional list of subdirectories, returns DirInfo structure
    with all the files and directories present as dictionary keys mapping to `PurePath` objects.
    Any file or directory name starting with a dot is ignored.
    """
    startpoint = path.join(root_dir, *subdirectories) if subdirectories else root_dir
    plain_files: Dict[str, PurePath] = {}
    directories: Dict[str, PurePath] = {}

    with os.scandir(startpoint) as it:
        # typecasting here because there isn't an annotation on os.scandir()
        typed_it: Iterator[os.DirEntry] = it
        for entry in typed_it:
            if not entry.name.startswith("."):
                if entry.is_file():
                    plain_files[entry.name] = PurePath(entry.path)
                elif entry.is_dir():
                    directories[entry.name] = PurePath(entry.path)
                else:
                    # something other than a file or directory? ignore for now.
                    pass

    return DirInfo(plain_files, directories)
