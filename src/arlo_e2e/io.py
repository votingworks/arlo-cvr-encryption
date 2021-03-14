import hashlib
import os
import random
from asyncio import Event
from base64 import b64encode
from dataclasses import dataclass
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

import boto3
import ray
from botocore.exceptions import ClientError
from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.logs import log_warning, log_error, log_info
from electionguard.serializable import Serializable
from electionguard.utils import flatmap_optional
from jsons import DecodeError, UnfulfilledArgumentError
from mypy_boto3_s3 import S3Client
from ray.actor import ActorHandle

from arlo_e2e.constants import BALLOT_FILENAME_PREFIX_DIGITS
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.manifest import sha256_hash

S = TypeVar("S", bound=Serializable)

# Mypy / type annotation note: to get all the types to check properly here,
# you have to follow some steps. Everything in the `typing` directory was
# auto-generated (via https://pypi.org/project/boto3-stubs/) and then you
# have to link it into PyCharm (instructions at that URL).

_s3_client_handle: Optional[S3Client] = None

DEFAULT_S3_STORAGE_CLASS = "STANDARD"
# Twice as expensive per month for storage, but supports immediate deletion,
# cheaper access. Suitable for testing, when we're creating and nuking these
# files fairly quickly.

# DEFAULT_S3_STORAGE_CLASS = "STANDARD_IA"
# Suitable for 'Long-lived, infrequently accessed data'. Half the cost of "STANDARD"
# for long-term storage, but you pay access charges, and objects have a minimum 30
# day lifespan. Suitable for production deployment (?). Or maybe we want to use
# "INTELLIGENT_TIERING" instead, which incurs a monthly cost, and will migrate
# objects back and forth between these two classes.

# Relevant documentation:
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html
# https://aws.amazon.com/s3/pricing/


def _s3_client() -> S3Client:
    """Gets S3 client resource. Initializes and caches as necessary."""
    global _s3_client_handle

    if _s3_client_handle is None:
        _s3_client_handle = boto3.client("s3")
    return _s3_client_handle


def _md5_hash(b: bytes) -> str:
    """Only used for error-checking on S3 put_object calls."""
    m = hashlib.md5()
    m.update(b)
    return b64encode(m.digest()).decode("utf-8")


@dataclass(eq=True, unsafe_hash=True)
class FileName:
    """
    Rather than Python Path/PurePath objects, or whatever else, we're going to use this
    FileName class to capture what we're doing, keeping separate track of the root directory,
    the list of subdirectories below that, and ultimately the file name. If the root
    directory happens to be "s3://bucket-name", then `protocol` will become `s3` and
    `root_dir` will become `bucket-name`. Otherwise, `protocol` will be `file`.

    In the S3 case, `bucket-name` could be any sort of string that `boto3`'s `put_object`
    method accepts, including an access-point-name or ARN.

    Methods on this class deal with loading and saving files from text strings, arrays
    of bytes and ElectionGuard `Serializable` objects. There's also special handling
    for `CiphertextBallot` objects, where the FileName instance should point to the
    the root, and everything below that (e.g, `ballots/b0000/b00001234.json`) is
    dealt with internally.

    When the `FileName` is actually naming a directory rather than a file, the `file_name`
    field will be an empty string.
    """

    protocol: str
    root_dir: str
    subdirectories: List[str]
    file_name: str

    def __init__(
        self, file_name: str, root_dir: str = ".", subdirectories: List[str] = None
    ):
        self.subdirectories = [] if subdirectories is None else subdirectories
        self.file_name = file_name
        if root_dir.startswith("s3://"):
            self.protocol = "s3"
            self.root_dir = root_dir[5:]
        else:
            self.protocol = "file"
            self.root_dir = root_dir

    def __str__(self) -> str:
        if self.protocol == "s3":
            return f"s3://{self.root_dir}" + self.s3_key_name()
        else:
            suffix = "/".join(self.subdirectories + [self.file_name])
            if self.root_dir.startswith("/"):
                return f"file:/{self.root_dir}/" + suffix
            else:
                return f"file:{self.root_dir}/" + suffix

    def is_local(self) -> bool:
        return self.protocol == "file"

    def local_file_path(self) -> Path:
        if self.protocol != "file":
            raise RuntimeError("can't convert an s3 filename to a local file path")
        else:
            return Path(PurePath(self.root_dir, *self.subdirectories))

    def s3_bucket(self) -> str:
        return self.root_dir

    def s3_key_name(self) -> str:
        if self.protocol != "s3":
            raise RuntimeError("can't get an s3 key-name from a non-s3 file path")
        else:
            return "/" + "/".join(self.subdirectories + [self.file_name])

    def file_exists(self) -> bool:
        if self.is_local():
            s = stat(self.local_file_path())
            return s.st_size > 0 and S_ISREG(s.st_mode)
        else:
            client = _s3_client()
            s3_bucket = self.s3_bucket()
            s3_key = self.s3_key_name()

            try:
                result = client.head_object(Bucket=s3_bucket, Key=s3_key)
                return result["ContentLength"] > 0

            except ClientError as error:
                error_dict = error.response["Error"]
                log_and_print(f"failed to stat {str(self)}: {str(error_dict)}")
                return False

            except Exception as e:
                log_and_print(f"failed to stat {str(self): {str(e)}}")
                return False

    def unlink(self) -> None:
        """
        Attempts to remove the file. If the file doesn't exist, nothing happens.
        """
        if self.file_name == "":
            # we're going to ignore directories; this only works on files
            return

        if self.is_local():
            self.local_file_path().unlink(missing_ok=True)
        else:
            client = _s3_client()
            s3_bucket = self.s3_bucket()
            s3_key = self.s3_key_name()

            try:
                client.delete_object(Bucket=s3_bucket, Key=s3_key)

            except ClientError as error:
                error_dict = error.response["Error"]
                log_and_print(f"failed to remove {str(self)}: {str(error_dict)}")

            except Exception as e:
                log_and_print(f"failed to remove {str(self): {str(e)}}")

    def write(self, contents: AnyStr, num_attempts: int = 1) -> bool:
        """
        Attempts to write the requested contents to this FileName, and will make the
        requested number of attempts, with some sleeping in between to work around
        failures. Returns True if the write succeeded. Logs an error and returns False
        if something went wrong.
        """
        attempt = 0

        status_actor = get_status_actor() if ray.is_initialized() else None

        while attempt < num_attempts:
            attempt += 1
            if self._write_once(contents, attempt):
                if attempt > 1:
                    if status_actor:
                        status_actor.decrement_pending.remote(False)
                    log_and_print(
                        f"Successful write for {str(self)} (attempt #{attempt})!"
                    )
                return True
            if status_actor:
                status_actor.increment_pending.remote()
            sleep(1)

        log_and_print(f"Failed write for {str(self)}, {attempt} attempt(s), aborting")
        if status_actor and attempt > 1:
            status_actor.decrement_pending.remote(True)
        return False

    def write_json(
        self,
        content_obj: Serializable,
        num_attempts: int = 1,
    ) -> bool:
        """
        Given an ElectionGuard "serializable" object, serializes it and writes the contents
        out to this FileName, making the requested number of attempts, with some sleeping in
        between to work around failures. Returns True if the write succeeded. Logs an error
        and returns False if something went wrong.
        """

        json_txt = content_obj.to_json(strip_privates=True)
        return self.write(json_txt, num_attempts)

    def _ballot_file_from_ballot_id(self, ballot_id: str) -> "FileName":
        """
        Internal function: constructs the appropriate FileName for a given ballot
        based on its `ballot_id` (without the ".json" suffix).
        """
        ballot_name_prefix = ballot_id[0:BALLOT_FILENAME_PREFIX_DIGITS]

        return FileName(
            ballot_id + ".json",
            self.root_dir,
            self.subdirectories + ["ballots", ballot_name_prefix],
        )

    def write_ciphertext_ballot(
        self,
        ballot: CiphertextAcceptedBallot,
        num_attempts: int = 1,
    ) -> None:
        """
        Given a ciphertext ballot, writes the ballot out, in the "ballots" subdirectory,
        of the current FileName top-level directory. Returns True if the write succeeded.
        Logs an error and returns False if something went wrong.
        """
        assert (
            self.file_name != ""
        ), "ciphertext ballots can only be written to FileNames representing directories"

        ballot_file_name = self._ballot_file_from_ballot_id(ballot.object_id)
        ballot_file_name.write_json(ballot, num_attempts)

    def _write_once(
        self,
        contents: AnyStr,
        counter: int,
    ) -> bool:
        """
        Internal function: returns True if the write succeeds. Logs an error and returns
        False if something went wrong.
        """
        if self.file_name == "":
            log_and_print(f"Trying to write a file without a filename: {str(self)}")
            return False

        fp = get_failure_probability_for_testing()
        if fp > 0.0:
            r = random.random()  # in the range [0.0, 1.0)
            if r < fp:
                log_and_print(
                    f"test-induced write error: {str(self)} (attempt #{counter})"
                )
                return False

        if self.is_local():
            write_mode = "w" if isinstance(contents, str) else "wb"
            try:
                file_path = self.local_file_path()
                file_path.mkdir(parents=True, exist_ok=True)
                with open(file_path / self.file_name, write_mode) as f:
                    f.write(contents)
                return True
            except Exception as e:
                log_and_print(
                    f"failed to write {str(self)} (attempt #{counter}): {str(e)}"
                )
                return False
        else:
            client = _s3_client()
            s3_bucket = self.s3_bucket()
            s3_key = self.s3_key_name()
            if isinstance(contents, str):
                binary_contents = contents.encode()
            else:
                binary_contents = contents

            # This is used as a fancy checksum to detect transmission errors, not for
            # anything where cryptographic strength is important.

            md5_hash = _md5_hash(binary_contents)

            try:
                if DEFAULT_S3_STORAGE_CLASS == "STANDARD_IA":
                    client.put_object(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        Body=binary_contents,
                        ContentMD5=md5_hash,
                        StorageClass="STANDARD_IA",
                    )
                else:
                    # This code is redundant because the type declaration for put_object requires a
                    # "literal" string, which means we can't just directly reference our global variable,
                    # even if it's final.

                    client.put_object(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        Body=binary_contents,
                        ContentMD5=md5_hash,
                        StorageClass="STANDARD",
                    )

            except ClientError as error:
                error_dict = error.response["Error"]
                log_and_print(
                    f"failed to write {str(self)} (attempt #{counter}): {str(error_dict)}"
                )
                return False

            except Exception as e:
                log_and_print(
                    f"failed to write {str(self)} (attempt #{counter}): {str(e)}"
                )
                return False

            return True

    def read_ciphertext_ballot(
        self, ballot_id: str, expected_sha256_hash: Optional[str] = None
    ) -> Optional[CiphertextAcceptedBallot]:
        """
        Reads the requested ballot, whether on S3 or local, returning its contents as a `CiphertextAcceptedBallot`.

        :param ballot_id: name of the ballot (without the ".json" suffix)
        :param expected_sha256_hash: If this parameter is not None, then the file bytes must match the specified hash,
          with any mismatch causing an error to be logged and None to be returned.
        """
        ballot_file_name = self._ballot_file_from_ballot_id(ballot_id)
        return ballot_file_name.read_json(
            CiphertextAcceptedBallot, expected_sha256_hash
        )

    def read_json(
        self, class_handle: Type[S], expected_sha256_hash: Optional[str] = None
    ) -> Optional[S]:
        """
        Reads the requested file, whether on S3 or local, returning its contents as a Python object for the given class handle.

        :param class_handle: The class, itself, that we're trying to deserialize to (if None, then you get back
          whatever the JSON becomes, e.g., a dict).
        :param expected_sha256_hash: If this parameter is not None, then the file bytes must match the specified hash,
          with any mismatch causing an error to be logged and None to be returned.
        """

        # this loads the file and verifies the hashes
        file_contents = self.read(expected_sha256_hash)
        if file_contents is not None:
            return _decode_json_file_contents(file_contents, class_handle)
        else:
            return None

    def read(self, expected_sha256_hash: Optional[str] = None) -> Optional[str]:
        """
        Reads the requested file, whether on S3 or local, and returns the bytes,
        if they exist or None if there's an error.

        :param expected_sha256_hash: If this parameter is not None, then the file bytes must match the specified hash,
          with any mismatch causing an error to be logged and None to be returned.
        """
        binary_contents = self._read()
        if binary_contents is None:
            return None

        if expected_sha256_hash is not None:
            actual_sha256_hash = sha256_hash(binary_contents)
            if expected_sha256_hash == actual_sha256_hash:
                return binary_contents.decode("utf-8")
            else:
                log_and_print(
                    f"mismatching hash for {str(self)}: expected {expected_sha256_hash}, found {actual_sha256_hash}"
                )
                return None
        else:
            return binary_contents.decode("utf-8")

    def _read(self) -> Optional[bytes]:
        """
        Internal function: Reads the requested file, whether on S3 or local, and returns the bytes,
        if they exist, or None if there's an error.
        """
        if self.file_name == "":
            log_and_print(f"Trying to read a file without a filename: {str(self)}")
            return None

        if self.is_local():
            try:
                with open(self.local_file_path(), "rb") as f:
                    data = f.read()
                    return data

            except FileNotFoundError as e:
                log_error(f"Error reading file ({str(self)}): {e}")
                return None

            except OSError as e:
                log_error(f"Error reading file ({str(self)}): {e}")
                return None

        else:
            client = _s3_client()
            s3_bucket = self.s3_bucket()
            s3_key = self.s3_key_name()

            try:
                result = client.get_object(Bucket=s3_bucket, Key=s3_key)
                body = result["Body"]
                binary_contents: bytes = body.read()
                body.close()
                return binary_contents

            except ClientError as error:
                error_dict = error.response["Error"]
                log_and_print(f"failed to read {str(self)}: {str(error_dict)}")
                return None

            except Exception as e:
                log_and_print(f"failed to read {str(self): {str(e)}}")
                return None


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
        """Increments the internal number of pending writes."""
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
