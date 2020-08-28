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
from electionguard.serializable import Serializable, WRITE
from electionguard.utils import flatmap_optional
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


def mkdir_helper(p: Union[str, Path]) -> None:
    """
    Wrapper around `os.mkdir` that will work correctly even if the directory already exists.
    """
    if isinstance(p, str):
        path = Path(p)
    else:
        path = p

    path.mkdir(parents=True, exist_ok=True)


def mkdir_list_helper(root_dir: str, paths: List[str] = None) -> None:
    """
    Like mkdir_helper, but takes a list of strings, each of which corresponds to a directory
    to make if it doesn't exist, each within the previous. So, `mkdir_list_helper('foo', ['a', 'b', 'c'])`
    would create `foo`, then `foo/a`, `foo/a/b`, and `foo/a/b/c`.
    """

    if paths is not None:
        mkdir_helper(Path(root_dir, *paths))
    else:
        mkdir_helper(Path(root_dir))


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


def decode_json_file_contents(
    json_str: str, class_handle: Type[Serializable[T]]
) -> Optional[T]:
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
    :returns: the number of bytes written
    """

    if isinstance(file_name, PurePath):
        full_name = file_name
    else:
        full_name = compose_filename(root_dir, file_name, subdirectories)
        mkdir_list_helper(root_dir, subdirectories)

    json_txt = json_obj.to_json(strip_privates=True)
    num_bytes = len(json_txt.encode("utf-8"))

    with open(full_name, WRITE) as f:
        f.write(json_txt)

    return num_bytes


def load_json_helper(
    root_dir: str,
    file_name: Union[str, PurePath],
    class_handle: Type[Serializable[T]],
    subdirectories: List[str] = None,
) -> Optional[T]:
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
