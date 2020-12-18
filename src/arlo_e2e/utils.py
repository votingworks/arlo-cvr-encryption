from math import ceil, floor
from os import path, stat, walk
from pathlib import PurePath, Path
from stat import S_ISREG
from typing import (
    TypeVar,
    Callable,
    Sequence,
    List,
    Iterable,
    Optional,
    Type,
    Union,
)

from electionguard.logs import log_error
from electionguard.serializable import Serializable
from electionguard.utils import flatmap_optional
from jsons import DecodeError, UnfulfilledArgumentError

from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.ray_write_retry import write_file_with_retries

T = TypeVar("T")
U = TypeVar("U")
S = TypeVar("S", bound=Serializable)


BALLOT_FILENAME_PREFIX_DIGITS = 5
"""
When we're writing ballots out to disk, we'll carve out this many characters and 
use that as a directory name. This avoids directories with insane numbers of files
that take forever to list or interact with.
"""


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


def shard_list(input: Iterable[T], num_per_group: int) -> Sequence[Sequence[T]]:
    """
    Breaks a list up into a list of lists, with `num_per_group` entries in each group,
    except for the final group which might be smaller. Useful for many things, including
    dividing up work units for parallel dispatch.
    """
    assert num_per_group >= 1, "need a positive number of list elements per group"
    input_list = list(input)
    length = len(input_list)
    return [input_list[i : i + num_per_group] for i in range(0, length, num_per_group)]


def shard_list_uniform(input: Iterable[T], num_per_group: int) -> Sequence[Sequence[T]]:
    """
    Similar to `shard_list`, but using a error residual propagation technique
    to ensure that the minimum and maximum number of elements per shard differ
    by at most one. The maximum number of elements per group will be less than
    or equal to `num_per_group`.
    """
    assert num_per_group >= 1, "need a positive number of list elements per group"
    input_list = list(input)
    length = len(input_list)
    if length == 0:
        return []

    num_groups = ceil(length / num_per_group)

    # this will be a floating point number, not an integer
    num_per_group_revised = length / num_groups

    output = []

    # leftover error, akin to Floyd-Steinberg dithering
    # (https://en.wikipedia.org/wiki/Floyd%E2%80%93Steinberg_dithering)
    residual = 0.0

    while input_list:
        current_num_per_group_float = num_per_group_revised + residual
        current_num_per_group_int = floor(num_per_group_revised + residual)
        residual = current_num_per_group_float - current_num_per_group_int

        # With floating point approximations, we might get really close but not quite, so we'll compensate
        # for this by saying if we're within epsilon of adding one more, we'll treat it as good enough.
        if residual > 0.99:
            current_num_per_group_int += 1
            residual = current_num_per_group_float - current_num_per_group_int

        output.append(input_list[:current_num_per_group_int])
        input_list = input_list[current_num_per_group_int:]

    return output


def mkdir_helper(p: Union[str, Path], num_retries: int = 1) -> None:
    """
    Wrapper around `os.mkdir` that will work correctly even if the directory already exists.
    """
    prev_exception = None
    if isinstance(p, str):
        path = Path(p)
    else:
        path = p

    for attempt in range(0, num_retries):
        try:
            path.mkdir(parents=True, exist_ok=True)
            return
        except Exception as e:
            prev_exception = e
            log_and_print(f"failed to make directory {p} (attempt {attempt}): {str(e)}")

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
        mkdir_helper(Path(root_dir, *paths), num_retries=num_retries)
    else:
        mkdir_helper(Path(root_dir), num_retries=num_retries)


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


def load_file_helper(
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


def write_json_helper(
    root_dir: str,
    file_name: Union[str, PurePath],
    json_obj: Serializable,
    subdirectories: List[str] = None,
    num_retries: int = 1,
) -> int:
    """
    Wrapper around JSON serialization that, given a directory name and file name (including
    the ".json" suffix), or a path-like object, will save the serialized contents
    of any ElectionGuard `Serializable` object as a JSON file and return the number of bytes written.

    Note: if the file_name is actually a path-like object, the root_dir and subdirectories are ignored,
    and the path is directly used.

    :param root_dir: top-level directory where we'll be reading files
    :param file_name: name of the file, including the suffix, excluding any directories leading up to the file
    :param json_obj: any ElectionGuard serializable object
    :param subdirectories: path elements to be introduced between `root_dir` and the file; empty-list means no subdirectory
    :param num_retries: how many attempts to make writing the file; works around occasional network filesystem glitches
    :returns: the number of bytes written
    """

    if isinstance(file_name, PurePath):
        full_name = file_name
    else:
        full_name = compose_filename(root_dir, file_name, subdirectories)
        mkdir_list_helper(root_dir, subdirectories, num_retries=num_retries)

    json_txt = json_obj.to_json(strip_privates=True)
    json_bytes = json_txt.encode("utf-8")
    num_bytes = len(json_bytes)

    write_file_with_retries(full_name, json_bytes, num_retries)

    return num_bytes


def load_json_helper(
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
    file_contents = load_file_helper(root_dir, file_name, subdirectories)
    return flatmap_optional(
        file_contents, lambda f: decode_json_file_contents(f, class_handle)
    )


def all_files_in_directory(root_dir: str) -> List[PurePath]:
    """
    Given a directory name, returns a list of all the files in that directory. The
    directory name will be prepended before each filename in the result.
    """
    results: List[str] = []
    for root, dirs, files in walk(root_dir):
        for file in files:
            results.append(path.join(root, file))
    return [PurePath(x) for x in results]
