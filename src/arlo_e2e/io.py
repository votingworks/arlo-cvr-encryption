import hashlib
import os
import random
from abc import ABC, abstractmethod
from asyncio import Event
from base64 import b64encode
from dataclasses import dataclass
from os import stat
from pathlib import PurePath, Path
from stat import S_ISREG, S_ISDIR
from time import sleep
from typing import (
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
from jsons import DecodeError, UnfulfilledArgumentError
from mypy_boto3_s3 import S3Client, ListObjectsV2Paginator
from mypy_boto3_s3.type_defs import ListObjectsV2OutputTypeDef
from ray.actor import ActorHandle

from arlo_e2e.constants import BALLOT_FILENAME_PREFIX_DIGITS
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.utils import sha256_hash

S = TypeVar("S", bound=Serializable)

_s3_client_handle: Optional[S3Client] = None
_local_failed_writes: int = 0


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
    """Used for error-checking on S3 put_object calls."""
    m = hashlib.md5()
    m.update(b)
    return b64encode(m.digest()).decode("utf-8")


class FileRef(ABC):
    """
    Base class for LocalFileRef and S3FileRef. Never create one of these directly.
    Instead use `make_file_ref`.

    Rather than Python Path/PurePath objects, or whatever else, we're going to use this
    FileRef class to capture what we're doing, keeping separate track of the root directory,
    the list of subdirectories below that, and ultimately the file name.

    Methods on this class deal with loading and saving files from text strings, arrays
    of bytes and ElectionGuard `Serializable` objects. There's also special handling
    for `CiphertextBallot` objects, where the FileRef instance should point to the
    the root, and everything below that (e.g, `ballots/b0000/b00001234.json`) is
    dealt with internally.

    Subclasses of this will deal with local filesystems or with S3.
    """

    root_dir: str
    subdirectories: List[str]
    file_name: str

    def __init__(self, root_dir: str, subdirectories: List[str], file_name: str):
        """Don't use this constructor. Use `make_file_ref` instead."""
        self.root_dir = root_dir
        self.subdirectories = subdirectories
        self.file_name = file_name

    @abstractmethod
    def update(
        self,
        new_file_name: str = None,
        new_root_dir: str = None,
        new_subdirs: List[str] = None,
    ) -> "FileRef":
        """
        Makes a copy of the current FileRef with the opportunity to update the requested fields
        to be a new file name, root directory, and/or subdirectory list. The original doesn't
        mutate.
        """
        ...

    def is_file(self) -> bool:
        """Returns whether this is a file (True) or a directory (False)."""
        return self.file_name != ""

    def is_dir(self) -> bool:
        """Returns whether this is a directory (True) or a file (False)."""
        return not self.is_file()

    @abstractmethod
    def is_local(self) -> bool:
        """Returns whether this is a local file (True) or an S3 file (False)."""
        ...

    @abstractmethod
    def local_file_path(self) -> Path:
        """For local files or directories, returns a `Path` to the file or directory."""
        ...

    @abstractmethod
    def local_dir_path(self) -> Path:
        """
        For local files, returns a `Path` to the directory containing the file.
        For local directories, returns a `Path` to the directory itself.
        """
        ...

    @abstractmethod
    def s3_bucket(self) -> str:
        """For s3 files, returns the bucket name."""
        ...

    @abstractmethod
    def s3_key_name(self) -> str:
        """For s3 files, returns the key name."""
        ...

    @abstractmethod
    def exists(self) -> bool:
        """
        Checks whether the file exists with non-zero size (True) or not (False).
        For directories (local or remote), checks if the directory exists with
        any files in it. This leads to at least somewhat consistent behavior
        between S3 and local files, since S3 "directories" don't really exist.
        """
        ...

    @abstractmethod
    def unlink(self) -> None:
        """
        Attempts to remove the file. If the file doesn't exist, nothing happens.
        Directories are silently ignored.
        """
        ...

    def write(self, contents: AnyStr, num_attempts: int = 1) -> bool:
        """
        Attempts to write the requested contents to this FileRef, and will make the
        requested number of attempts, with some sleeping in between to work around
        failures. Returns True if the write succeeded. Logs an error and returns False
        if something went wrong.

        Notably, if this FileRef requires subdirectories that don't presently exist,
        they will be created.
        """
        attempt = 0

        status_actor = get_status_actor() if ray.is_initialized() else None

        # Careful engineering hack: we're calling ray.get() on the calls to increment or
        # decrement the number of pending writes, making them *synchronous* calls. This
        # slows us down, but it avoids a race condition where we've made it all the way
        # through the write method, but the increment and decrement operations haven't
        # even happened yet, which might cause the ultimate wait_for_zero_pending call
        # to complete prematurely.

        if status_actor:
            num_pending = ray.get(status_actor.increment_pending.remote())

        while attempt < num_attempts:
            attempt += 1
            if self._write_internal(contents, attempt):
                if status_actor:
                    num_pending = ray.get(status_actor.decrement_pending.remote(False))
                if attempt > 1:
                    log_and_print(
                        f"Successful write for {str(self)} (attempt #{attempt})!"
                    )
                return True
            sleep(1)

        log_and_print(f"Failed write for {str(self)}, {attempt} attempt(s), aborting")
        if status_actor and attempt > 1:
            num_pending = ray.get(status_actor.decrement_pending.remote(True))
        else:
            global _local_failed_writes
            _local_failed_writes += 1
        return False

    def write_json(
        self,
        content_obj: Serializable,
        num_attempts: int = 1,
    ) -> bool:
        """
        Given an ElectionGuard "serializable" object, serializes it and writes the contents
        out to this FileRef, making the requested number of attempts, with some sleeping in
        between to work around failures. Returns True if the write succeeded. Logs an error
        and returns False if something went wrong.
        """

        json_txt = content_obj.to_json(strip_privates=True)
        return self.write(json_txt, num_attempts)

    def _ballot_file_from_ballot_id(self, ballot_id: str) -> "FileRef":
        """
        Internal function: constructs the appropriate FileRef for a given ballot
        based on its `ballot_id` (without the ".json" suffix).
        """
        ballot_name_prefix = ballot_id[0:BALLOT_FILENAME_PREFIX_DIGITS]

        return self.update(
            ballot_id + ".json",
            self.root_dir,
            self.subdirectories + ["ballots", ballot_name_prefix],
        )

    def write_ciphertext_ballot(
        self,
        ballot: CiphertextAcceptedBallot,
        num_attempts: int = 1,
    ) -> bool:
        """
        Given a ciphertext ballot, writes the ballot out, in the "ballots" subdirectory,
        of the current FileRef top-level directory. Returns True if the write succeeded.
        Logs an error and returns False if something went wrong.
        """
        assert (
            self.is_dir()
        ), "ciphertext ballots can only be written to FileRefs representing directories"

        ballot_file_name = self._ballot_file_from_ballot_id(ballot.object_id)
        return ballot_file_name.write_json(ballot, num_attempts)

    @abstractmethod
    def _write_internal(self, contents: AnyStr, counter: int) -> bool:
        ...

    def _write_failure_for_testing(
        self,
        counter: int,
    ) -> bool:
        """Returns False if there should be a test-induced failure. True if everything is good."""
        fp = get_failure_probability_for_testing()
        if fp > 0.0:
            r = random.random()  # in the range [0.0, 1.0)
            if r < fp:
                log_and_print(
                    f"test-induced write error: {str(self)} (attempt #{counter})"
                )
                return False

        return True

    def read_ciphertext_ballot(
        self, ballot_id: str, expected_sha256_hash: Optional[str] = None
    ) -> Optional[CiphertextAcceptedBallot]:
        """
        Reads the requested ballot, returning its contents as a `CiphertextAcceptedBallot`.

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
        Reads the requested file, returning its contents as a Python object for the given class handle.

        :param class_handle: The class, itself, that we're trying to deserialize to (if None, then you get back
          whatever the JSON becomes, e.g., a dict).
        :param expected_sha256_hash: If this parameter is not None, then the file bytes must match the specified hash,
          with any mismatch causing an error to be logged and None to be returned.
        """

        # this loads the file and verifies the hashes
        file_contents = self.read(expected_sha256_hash)
        if file_contents is not None:
            return decode_json_file_contents(file_contents, class_handle)
        else:
            return None

    def read(self, expected_sha256_hash: Optional[str] = None) -> Optional[str]:
        """
        Reads the requested file and returns the bytes,
        if they exist or None if there's an error.

        :param expected_sha256_hash: If this parameter is not None, then the file bytes must match the specified hash,
          with any mismatch causing an error to be logged and None to be returned.
        """
        binary_contents = self._read_internal()
        if binary_contents is None:
            return None

        if expected_sha256_hash is not None:
            actual_sha256_hash = sha256_hash(binary_contents)
            if expected_sha256_hash == actual_sha256_hash:
                return binary_contents.decode("utf-8")
            else:
                log_error(
                    f"mismatching hash for {str(self)}: expected {expected_sha256_hash}, found {actual_sha256_hash}"
                )
                return None
        else:
            return binary_contents.decode("utf-8")

    @abstractmethod
    def _read_internal(self) -> Optional[bytes]:
        """
        Internal function: Reads the requested file and returns the bytes,
        if they exist, or None if there's an error.
        """
        ...

    class DirInfo(NamedTuple):
        files: Dict[str, "FileRef"]
        subdirs: Dict[str, "FileRef"]

    @abstractmethod
    def scandir(self) -> DirInfo:
        """
        Assuming that we're working with a directory, this will return a `DirInfo`,
        containing two dictionaries. The keys are the file or subdirectory names,
        and the values are `FileRef` instances.

        Any file or directory name starting with a dot is ignored.
        """
        ...

    @abstractmethod
    def size(self) -> int:
        """
        Returns the number of bytes in the file if it exists. Zero on failure.
        """
        ...


def make_file_ref_from_path(full_path: str) -> FileRef:
    """
    Given something that might show up on the Unix command-line, which might be
    a relative filename, an absolute filename, or possible a S3 URL, converts
    that input to a FileRef.

    If the given path ends with a slash, then it's assumed that we're naming a
    *directory*. If the given path doesn't end with a slash, but a local directory
    of the requested name exists, then that's also treated as a directory. Otherwise,
    it's assumed that we're talking about a *file*. This has ramifications for
    the front-end user experience. If the user is naming the output directory,
    they should be required to add a trailing slash.

    So, if you're *expecting* the user to name a directory, then just pass the
    name into this function, and call `is_dir()` on the resulting `FileRef`.
    if it's not a directory, then bomb out with a user-friendly error message.
    """

    if full_path.startswith("s3://"):
        parts = list(Path(full_path[5:]).parts)
        if len(parts) < 2:
            # we're dealing with the name a bucket, alone, so we're treating
            # it as a directory reference to the root of the bucket
            root_dir = parts[0]
            file_name = ""
            subdirs = []
        elif full_path.endswith("/"):
            # we're naming a directory rather than a file
            root_dir = parts[0]
            file_name = ""
            subdirs = parts[1:]
        else:
            root_dir = parts[0]
            file_name = parts[-1]
            subdirs = parts[1:-1]
        return make_file_ref(
            root_dir=f"s3://{root_dir}",
            file_name=file_name,
            subdirectories=list(subdirs),
        )
    else:
        path = Path(full_path)
        parts = list(path.parts)
        num_parts = len(parts)
        is_dir = path.is_dir() or full_path.endswith("/")

        if num_parts == 0:
            # this happens if the input is "."
            root_dir = "."
            file_name = ""
            subdirs = []
        elif is_dir:
            if parts[0] == "/":
                root_dir = "/"
                file_name = ""
                subdirs = parts[1:]
            else:
                root_dir = "."
                file_name = ""
                subdirs = parts
        else:
            # it's most likely a file
            if parts[0] == "/":
                root_dir = "/"
                file_name = parts[-1]
                subdirs = parts[1:-1]
            else:
                root_dir = "."
                file_name = parts[-1]
                subdirs = parts[0:-1]

        return make_file_ref(
            root_dir=root_dir, file_name=file_name, subdirectories=list(subdirs)
        )


def make_file_ref(
    file_name: str, root_dir: str = ".", subdirectories: List[str] = None
) -> FileRef:
    """
    When you want a FileRef object, you use this function to do it. If the `root_dir` starts
    with `s3://`, then you'll get an `S3FileRef`, otherwise a `LocalFileRef`, both of which
    respect the APIs in the base `FileRef` class.

    In the S3 case, `root_dir` is expected to have the `s3://` prefix, followed by the S3
    bucket name. This could be in any form that boto3 understands bucket names (ARN or whatever
    else). The subdirectories are simply concatenated together with forward slashes, with
    the `file_name` at the end to construct the S3 key.

    In the local case, `root_dir` could be an arbitrary file path. The subdirectories are
    concatenated using the proper rules for the local operating system (backslashes for
    Windows, forward slashes for Unix).
    """
    subdirectories = [] if subdirectories is None else subdirectories
    if root_dir.startswith("s3://"):
        root_dir = root_dir[5:]
        return S3FileRef(root_dir, subdirectories, file_name)
    else:
        return LocalFileRef(root_dir, subdirectories, file_name)


def validate_directory_input(
    directory_name: str,
    explanation: str,
    error_if_exists: bool = False,
    error_if_absent: bool = False,
    raise_exception_dont_exit: bool = False,
) -> str:
    """
    Given a string input, from a user, claiming to be the name of a directory,
    makes sure that it actually is a directory (e.g., requiring a trailing slash
    for S3, and making sure no such thing exists as a local file of the same name).
    If there's an error condition, a suitable message is printed to stdout, and
    `exit(1)` is called. If this function returns, the string that comes back should
    be used in place of the original `directory_name`.

    When you're about to do something heavyweight, like writing out all of the encrypted
    ballots for an election, you might want to error out if the requested directory
    already exists, as a way to make sure you don't accidentally overwrite existing ballots.
    The optional flag `error_if_exists`, if set to try, will accomplish this. Similarly,
    if you're about to read a bunch of such material, then `error_if_absent` will make
    the desired checks for you. An empty directory is considered as if it's absent.

    For testing purposes, you can set `raise_exception_dont_exit`, which will cause
    a `FileExistsError` to be raised, rather than `exit(1)` to be called.
    """

    # Input-checking logic like this was repeating in all of the apps, so it made
    # sense to pull it all together in one place, where it's testable, etc.

    dir_ref = make_file_ref_from_path(directory_name)
    if dir_ref.is_file():
        directory_name = directory_name + "/"
        dir_ref = make_file_ref_from_path(directory_name)
        if dir_ref.is_file():
            err_str = f"Unexpected error: {explanation} directory {directory_name} isn't actually a directory."
            if raise_exception_dont_exit:
                raise FileExistsError(err_str)
            print(err_str)
            exit(1)

    if error_if_absent or error_if_exists:
        dir_exists = dir_ref.exists()
        if error_if_exists and dir_exists:
            err_str = f"Error: {explanation} directory {directory_name} already exists."
            if raise_exception_dont_exit:
                raise FileExistsError(err_str)
            print(err_str)
            exit(1)
        elif error_if_absent and not dir_exists:
            err_str = (
                f"Error: {explanation} directory {directory_name} is missing or empty."
            )
            if raise_exception_dont_exit:
                raise FileExistsError(err_str)
            print(err_str)
            exit(1)

    if directory_name.endswith("/"):
        return directory_name
    else:
        return directory_name + "/"


@dataclass(eq=True, unsafe_hash=True)
class S3FileRef(FileRef):
    def __init__(self, root_dir: str, subdirectories: List[str], file_name: str):
        super().__init__(root_dir, subdirectories, file_name)

    def __str__(self) -> str:
        # Normally, a key doesn't start with a slash, but when we're dealing with
        # a reference to a "directory", which isn't really a thing in S3 anyway,
        # we want to stringify our FileRef with a trailing slash. This code needs
        # a special case to deal with a reference to the "root" of the bucket,
        # where the key_name will just be "/".
        bucket_name = f"s3://{self.s3_bucket()}/"
        key_name = self.s3_key_name()
        if key_name == "/":
            return bucket_name
        else:
            return bucket_name + self.s3_key_name()

    def update(
        self,
        new_file_name: str = None,
        new_root_dir: str = None,
        new_subdirs: List[str] = None,
    ) -> FileRef:
        return S3FileRef(
            file_name=self.file_name if new_file_name is None else new_file_name,
            root_dir=self.root_dir if new_root_dir is None else new_root_dir,
            subdirectories=self.subdirectories if new_subdirs is None else new_subdirs,
        )

    def is_local(self) -> bool:
        return False

    def local_file_path(self) -> Path:
        raise RuntimeError("can't convert an s3 filename to a local file path")

    def local_dir_path(self) -> Path:
        raise RuntimeError("can't convert an s3 filename to a local file path")

    def s3_bucket(self) -> str:
        return self.root_dir

    def s3_key_name(self) -> str:
        if not self.subdirectories and not self.file_name:
            # special case for a FileRef to the root of a bucket.
            return "/"
        else:
            # If the file_name is the empty-string, then the key-name will have a trailing
            # slash, which is exactly what we want for a directory as distinct from a file.
            return "/".join(self.subdirectories + [self.file_name])

    def exists(self) -> bool:
        if self.is_dir():
            # not the most efficient thing to do on directories with a zillion files
            # but will give us the desired semantics
            scan = self.scandir()
            return len(scan.files) > 0 or len(scan.subdirs) > 0
        else:
            client = _s3_client()
            s3_bucket = self.s3_bucket()
            s3_key = self.s3_key_name()

            try:
                result = client.head_object(Bucket=s3_bucket, Key=s3_key)
                return result["ContentLength"] > 0

            except ClientError as error:
                error_dict = error.response["Error"]
                log_error(f"failed to stat {str(self)}: {str(error_dict)}")
                return False

            except Exception as e:
                log_error(f"failed to stat {str(self)}: {str(e)}")
                return False

    def unlink(self) -> None:
        if self.is_dir():
            return

        client = _s3_client()
        s3_bucket = self.s3_bucket()
        s3_key = self.s3_key_name()

        try:
            client.delete_object(Bucket=s3_bucket, Key=s3_key)

        except ClientError as error:
            error_dict = error.response["Error"]
            log_error(f"failed to remove {str(self)}: {str(error_dict)}")

        except Exception as e:
            log_error(f"failed to remove {str(self)}: {str(e)}")

    def _write_internal(
        self,
        contents: AnyStr,
        counter: int,
    ) -> bool:
        if not self._write_failure_for_testing(counter):
            return False

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
                # even if it's final. Who thought this was a good idea?

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
            log_and_print(f"failed to write {str(self)} (attempt #{counter}): {str(e)}")
            return False

        return True

    def _read_internal(self) -> Optional[bytes]:
        if self.is_dir():
            log_error(f"Trying to read a file without a filename: {str(self)}")
            return None

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
            log_error(f"failed to read {str(self)}: {str(error_dict)}")
            return None

        except Exception as e:
            log_error(f"failed to read {str(self)}: {str(e)}")
            return None

    def scandir(self) -> FileRef.DirInfo:
        if not self.is_dir():
            raise RuntimeError("scandir only works on directories, not files")

        client = _s3_client()
        s3_bucket = self.s3_bucket()
        s3_key = self.s3_key_name()

        plain_files: Dict[str, FileRef] = {}
        directories: Dict[str, FileRef] = {}

        try:
            # When there are more 1000 results to a query, you need to issue continuation
            # requests, or you can just wrap that into one of these paginator things.
            # https://stackoverflow.com/a/59816089/4048276

            paginator: ListObjectsV2Paginator = client.get_paginator("list_objects_v2")
            pages: Iterator[ListObjectsV2OutputTypeDef] = paginator.paginate(
                Bucket=s3_bucket, Prefix=s3_key, Delimiter="/"
            )

            for page in pages:
                for obj in page["Contents"]:
                    # {
                    #     'Key': 'string',
                    #     'LastModified': datetime(2015, 1, 1),
                    #     'ETag': 'string',
                    #     'Size': 123,
                    #     'StorageClass': 'STANDARD'|'REDUCED_REDUNDANCY'|'GLACIER'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'DEEP_ARCHIVE'|'OUTPOSTS',
                    #     'Owner': {
                    #         'DisplayName': 'string',
                    #         'ID': 'string'
                    #     }
                    # },
                    k = obj["Key"]
                    if k.endswith("/"):
                        # we'll get back an entry corresponding to the directory prefix that we're
                        # actually searching for, which we just need to ignore
                        continue

                    fr = make_file_ref_from_path(f"s3://{s3_bucket}/{k}")
                    plain_files[fr.file_name] = fr
                for d in page["CommonPrefixes"]:
                    # {
                    #      'Prefix': 'string'
                    # }
                    prefix: str = d["Prefix"]
                    assert prefix.endswith(
                        "/"
                    ), "expecting an S3 `prefix` to act like a directory name"

                    path = prefix.split("/")

                    fr = make_file_ref_from_path(f"s3://{s3_bucket}/{prefix}")
                    directory_name = path[-2]
                    directories[directory_name] = fr

        except ClientError as error:
            error_dict = error.response["Error"]
            log_error(f"failed to list_objects {str(self)}: {str(error_dict)}")

        except Exception as e:
            log_error(f"failed to list_objects {str(self)}: {str(e)}")

        return FileRef.DirInfo(plain_files, directories)

    def size(self) -> int:
        if self.is_dir():
            return 0

        client = _s3_client()
        s3_bucket = self.s3_bucket()
        s3_key = self.s3_key_name()

        try:
            result = client.head_object(Bucket=s3_bucket, Key=s3_key)
            if "ContentLength" in result:
                return result["ContentLength"]

        except ClientError as error:
            error_dict = error.response["Error"]
            log_error(f"failed to head_object {str(self)}: {str(error_dict)}")

        except Exception as e:
            log_error(f"failed to head_object {str(self)}: {str(e)}")

        return 0


@dataclass(eq=True, unsafe_hash=True)
class LocalFileRef(FileRef):
    def __init__(self, root_dir: str, subdirectories: List[str], file_name: str):
        super().__init__(root_dir, subdirectories, file_name)

    def __str__(self) -> str:
        suffix = "/".join(self.subdirectories + [self.file_name])
        if self.root_dir.endswith("/"):
            return f"file:{self.root_dir}" + suffix
        else:
            return f"file:{self.root_dir}/" + suffix

    def update(
        self,
        new_file_name: str = None,
        new_root_dir: str = None,
        new_subdirs: List[str] = None,
    ) -> FileRef:
        return LocalFileRef(
            file_name=self.file_name if new_file_name is None else new_file_name,
            root_dir=self.root_dir if new_root_dir is None else new_root_dir,
            subdirectories=self.subdirectories if new_subdirs is None else new_subdirs,
        )

    def is_local(self) -> bool:
        return True

    def local_file_path(self) -> Path:
        dir_path = self.local_dir_path()
        if self.file_name:
            return dir_path / self.file_name
        else:
            return dir_path

    def local_dir_path(self) -> Path:
        return Path(PurePath(self.root_dir, *self.subdirectories))

    def s3_bucket(self) -> str:
        raise RuntimeError("s3_bucket only defined for s3 files")

    def s3_key_name(self) -> str:
        raise RuntimeError("can't get an s3 key-name from a non-s3 file path")

    def exists(self) -> bool:
        if self.is_dir():
            # not the most efficient thing to do on directories with a zillion files
            # but will give us the desired semantics
            scan = self.scandir()
            return len(scan.files) > 0 or len(scan.subdirs) > 0
        else:
            try:
                s = stat(self.local_file_path())
                return s.st_size > 0 and S_ISREG(s.st_mode)
            except FileNotFoundError:
                return False

    def unlink(self) -> None:
        if not self.is_dir():
            self.local_file_path().unlink(missing_ok=True)

    def _write_internal(
        self,
        contents: AnyStr,
        counter: int,
    ) -> bool:
        if not self._write_failure_for_testing(counter):
            return False

        write_mode = "w" if isinstance(contents, str) else "wb"
        try:
            file_path = self.local_dir_path()
            file_path.mkdir(parents=True, exist_ok=True)
            with open(self.local_file_path(), write_mode) as f:
                f.write(contents)
            return True
        except Exception as e:
            log_and_print(f"failed to write {str(self)} (attempt #{counter}): {str(e)}")
            return False

    def _read_internal(self) -> Optional[bytes]:
        if self.is_dir():
            log_error(f"Trying to read a file without a filename: {str(self)}")
            return None

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

    def scandir(self) -> FileRef.DirInfo:
        if not self.is_dir():
            raise RuntimeError("scandir only works on directories, not files")

        startpoint = self.local_file_path()
        plain_files: Dict[str, "FileRef"] = {}
        directories: Dict[str, "FileRef"] = {}

        try:
            with os.scandir(startpoint) as it:
                # typecasting here because there isn't an annotation on os.scandir()
                typed_it: Iterator[os.DirEntry] = it
                for entry in typed_it:
                    if not entry.name.startswith("."):
                        if entry.is_file():
                            plain_files[entry.name] = self.update(
                                new_file_name=entry.name,
                                new_subdirs=self.subdirectories,
                                new_root_dir=self.root_dir,
                            )
                        elif entry.is_dir():
                            directories[entry.name] = self.update(
                                new_file_name="",
                                new_subdirs=self.subdirectories + [entry.name],
                                new_root_dir=self.root_dir,
                            )
                        else:
                            # something other than a file or directory? ignore for now.
                            pass
            return FileRef.DirInfo(plain_files, directories)
        except FileNotFoundError:
            return FileRef.DirInfo({}, {})

    def size(self) -> int:
        try:
            stats = os.stat(self.local_file_path())
        except Exception:
            return 0

        is_dir = S_ISDIR(stats.st_mode)
        num_bytes = stats.st_size if not is_dir else 0
        return num_bytes


@ray.remote
class WriteRetryStatusActor:  # pragma: no cover
    # sadly, code coverage testing can't trace into Ray remote methods and actors
    num_failures: int
    num_pending: int
    event: Event
    failure_probability: float

    def __init__(self) -> None:
        log_info("WriteRetryStatusActor: starting!", verbose=True)
        self.event = Event()
        self.reset_to_zero()

    def reset_to_zero(self) -> None:
        self.num_failures = 0
        self.num_pending = 0
        self.failure_probability = 0.0

    def increment_pending(self) -> int:
        """Increments the internal number of pending writes, returns the
        total number of pending writes."""
        self.num_pending += 1
        return self.num_pending

    def decrement_pending(self, failure_happened: bool) -> int:
        """
        Decrements the internal number of pending writes and also tracks whether
        the write ultimately succeeded (pass `failure_happened=False`) or failed
        (pass `failure_happened=True`). Returns the total number of pending writes.
        """
        if failure_happened:
            self.num_failures += 1

        self.num_pending -= 1

        if self.num_pending == 0:
            self.event.set()

        return self.num_pending

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


_singleton_name = "WriteRetryStatusActorSingleton"
_status_actor: Optional[ActorHandle] = None


def reset_status_actor() -> None:
    """
    If you've finished one computation and are about to start another, this resets all
    the internal counters. Also, this will remove any actors, which will then be restarted
    if necessary.

    In a testing scenario, you might call this after a call to `ray.shutdown()`.
    """
    global _status_actor
    global __failure_probability
    global _local_failed_writes

    _status_actor = None
    __failure_probability = None
    _local_failed_writes = 0


def init_status_actor() -> None:
    """
    Helper function, called by our own ray_init_* routines, exactly once, to make the singleton
    WriteRetryStatusActor. If you want a handle to the actor, call `get_status_actor`.
    """
    global _status_actor
    _status_actor = WriteRetryStatusActor.options(name=_singleton_name).remote()  # type: ignore


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

    global _status_actor

    if _status_actor is None:
        _status_actor = ray.get_actor(_singleton_name)

    if _status_actor is None:
        log_error("Configuration failure: we should have a status actor, but we don't.")
        raise RuntimeError("No status actor")

    return _status_actor


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


def wait_for_zero_pending_writes() -> int:
    """
    Blocking call: waits until all pending writes are complete, then returns
    the number of failed writes (i.e., writes where the number of failures
    exceeded the number of retry attempts). If this is zero, then every write
    succeeded, eventually. If it's non-zero, then you should warn the user
    with the number, perhaps suggesting that something really bad happened.
    """
    global _local_failed_writes
    if ray.is_initialized():
        num_failures: int = ray.get(get_status_actor().wait_for_zero_pending.remote())
        return num_failures
    else:
        return _local_failed_writes


def decode_json_file_contents(json_str: str, class_handle: Type[S]) -> Optional[S]:
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
