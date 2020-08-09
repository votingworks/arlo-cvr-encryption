import os
from pathlib import PurePath
from hashlib import sha256
from os import path, mkdir
from typing import (
    TypeVar,
    Callable,
    Sequence,
    List,
    Iterable,
    Optional,
    Type,
    cast,
    Union,
)

import jsons
from electionguard.logs import log_error
from electionguard.serializable import Serializable
from jsons import DecodeError, UnfulfilledArgumentError

T = TypeVar("T")
U = TypeVar("U")


def flatmap(f: Callable[[T], Iterable[U]], li: Iterable[T]) -> Sequence[U]:
    """
    General-purpose flatmapping on sequences/lists/iterables. The lambda is
    expected to return a list of items for each input in the original list `li`,
    and all those lists are concatenated together.
    """
    mapped = map(f, li)

    # This function eagerly computes its result. The below link tries to do it in a
    # more lazy fashion using Python's yield keyword. Might be worth investigating.
    # https://stackoverflow.com/questions/53509826/equivalent-to-pyspark-flatmap-in-python

    result: List[U] = []
    for item in mapped:
        for subitem in item:
            result.append(subitem)
    return result


def shard_list(input: Sequence[T], num_per_group: int) -> Sequence[Sequence[T]]:
    """
    Breaks a list up into a list of lists, with `num_per_group` entries in each group,
    except for the final group which might be smaller. Useful for many things, including
    dividing up work units for parallel dispatch.
    """
    assert num_per_group >= 1, "need a positive number of list elements per group"
    length = len(input)
    return [input[i : i + num_per_group] for i in range(0, length, num_per_group)]


def mkdir_helper(p: str) -> None:
    """
    Wrapper around `os.mkdir` that will work correctly even if the directory already exists.
    """
    if not path.exists(p):
        mkdir(p)


def mkdir_list_helper(root_dir: str, paths: List[str] = None) -> None:
    """
    Like mkdir_helper, but takes a list of strings, each of which corresponds to a directory
    to make if it doesn't exist, each within the previous. So, `mkdir_list_helper('foo', ['a', 'b', 'c'])`
    would create `foo`, then `foo/a`, `foo/a/b`, and `foo/a/b/c`.
    """

    mkdir_helper(root_dir)

    if paths is None:
        return

    for i in range(len(paths)):
        subpath = path.join(root_dir, *(paths[0 : i + 1]))
        mkdir_helper(subpath)


def sha256_hash(input: str) -> int:
    """
    Given a string, returns an integer representing the 256-bit SHA2-256
    hash of that input string, encoded as UTF-8 bytes.
    """
    h = sha256()
    h.update(input.encode("utf-8"))
    return int.from_bytes(h.digest(), byteorder="big")


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


def compose_manifest_name(file_name: str, subdirectories: List[str] = None) -> str:
    """
    Helper function: given a file name, and an optional list of subdirectories that
    go in front (empty-list implies no subdirectory), returns a string corresponding
    to the full filename, properly joined, suitable for use in MANIFEST.json.

    This method is distinct from `compose_filename` because it must give the same
    answer on any platform. This is why it uses vertical bars rather than forward
    or backward slashes.
    """
    if subdirectories is None:
        dirs = [file_name]
    else:
        dirs = subdirectories + [file_name]
    return "|".join(dirs)


def manifest_name_to_filename(root_dir: str, manifest_name: str) -> PurePath:
    """
    Helper function: given the name of a file, as it would appear in a MANIFEST.json
    file, get the expected local filesystem name.
    """
    subdirs = root_dir.split("|")
    return compose_filename(root_dir, subdirs[-1], subdirs[0:-1])


def filename_to_manifest_name(root_dir: str, filename: Union[str, PurePath]) -> str:
    """
    Helper function: given the name of a file (or a Path to that file), return the name as
    it would appear in MANIFEST.json.
    """
    if not isinstance(filename, PurePath):
        filename = PurePath(filename)
    elems = list(filename.parts)  # need to convert from tuple to list
    assert elems[0] != root_dir, f"unexpected root directory in path: {filename}"
    return compose_manifest_name(elems[-1], elems[1:-1])


def load_file_helper(
    root_dir: str,
    file_name: Union[str, PurePath],
    subdirectories: List[str] = None,
    expected_sha256: Optional[int] = None,
) -> Optional[str]:
    """
    Reads the requested file, by name, returning its contents as a Python string.
    If no hash for the file is present, or if the file doesn't match its known
    hash, then `None` will be returned and an error will be logged.

    Note: if the file_name is a path-like object, the root_dir and subdirectories
    are ignored and the file is directly loaded.

    :param root_dir: top-level directory where we'll be reading files
    :param file_name: name of the file, including any suffix
    :param subdirectories: path elements to be introduced between `root_dir` and the file; empty-list means no subdirectory
    :param expected_sha256: if present, the file contents will be compared to the hash
    :returns: the contents of the file, or `None` if there was an error
    """

    if isinstance(file_name, PurePath):
        full_name = file_name
    else:
        full_name = compose_filename(root_dir, file_name, subdirectories)

    try:
        s = os.stat(full_name)
        if s.st_size == 0:  # pragma: no cover
            log_error(f"The file ({full_name}) is empty")
            return None

        with open(full_name, "r") as f:
            data = f.read()

            if expected_sha256 is not None:
                data_hash = sha256_hash(data)
                if data_hash != expected_sha256:  # pragma: no cover
                    log_error(
                        f"File {full_name} did not have the expected hash (expected: {expected_sha256}, actual: {data_hash})"
                    )
                    return None
            return data
    except OSError as e:  # pragma: no cover
        log_error(f"Error reading file ({full_name}): {e}")
        return None


def load_json_helper(
    root_dir: str,
    file_name: Union[str, PurePath],
    class_handle: Type[Serializable[T]],
    subdirectories: List[str] = None,
    expected_sha256: Optional[int] = None,
) -> Optional[T]:
    """
    Wrapper around JSON deserialization that, given a directory name and file prefix (without
    the ".json" suffix) as well as an optional handle to the class type, will load the contents
    of the file and return an instance of the desired class, if it fits. If anything fails, the
    result should be `None`. No exceptions should be raised outside of this method, but all such
    errors will be logged as part of the ElectionGuard log.

    Optionally, by passing in a SHA256 value for the file, such as might have been generated by
    `sha256_hash`, this will validate the file's hash, returning None if there's a mismatch.

    Note: if the file_name is actually a path-like object, the root_dir and subdirectories are ignored,
    and the path is directly loaded.

    :param root_dir: top-level directory where we'll be reading files
    :param file_name: name of the file, including the suffix, excluding any directories leading up to the file
    :param class_handle: the class, itself, that we're trying to deserialize to (if None, then you get back
      whatever the JSON becomes, e.g., a dict)
    :param file_suffix: ".json" or something like that
    :param subdirectories: path elements to be introduced between `root_dir` and the file; empty-list means no subdirectory
    :param expected_sha256: if present, the file contents will be compared to the hash
    :returns: the contents of the file, or `None` if there was an error
    """
    if isinstance(file_name, PurePath):
        file_contents = load_file_helper(
            root_dir, file_name, subdirectories, expected_sha256
        )
    else:
        full_filename = compose_filename(root_dir, file_name, subdirectories)
        file_contents = load_file_helper(
            root_dir, file_name, subdirectories, expected_sha256
        )
    if file_contents is None:
        return None

    try:
        result = class_handle.from_json(file_contents)
    except DecodeError as err:  # pragma: no cover
        log_error(f"Failed to decode an instance of {class_handle}: {err}")
        return None
    except UnfulfilledArgumentError as err:  # pragma: no cover
        log_error(f"Decoding failure for {class_handle}: {err}")
        return None

    return result


def all_files_in_directory(root_dir: str) -> List[PurePath]:
    """
    Given a directory name, returns a list of all the files in that directory. The
    directory name will be prepended before each filename in the result.
    """
    results: List[str] = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            results.append(path.join(root, file))
    return [PurePath(x) for x in results]
