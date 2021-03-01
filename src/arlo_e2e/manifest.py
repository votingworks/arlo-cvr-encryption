from base64 import b64encode
from dataclasses import dataclass
from hashlib import sha256
from typing import (
    Dict,
    Optional,
    Type,
    List,
    AnyStr,
    TypeVar,
    Tuple,
    cast,
    NamedTuple,
)

import ray
from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.logs import log_error
from electionguard.serializable import Serializable
from electionguard.utils import flatmap_optional
from ray.actor import ActorHandle

from arlo_e2e.constants import (
    BALLOT_FILENAME_PREFIX_DIGITS,
    NUM_WRITE_RETRIES,
    MANIFEST_FILE,
)
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.ray_helpers import ray_wait_for_workers
from arlo_e2e.ray_io import (
    ray_load_file,
    _decode_json_file_contents,
    read_directory_contents,
    ray_write_file,
    unlink_helper,
    compose_filename,
)
from arlo_e2e.ray_progress import ProgressBar

T = TypeVar("T")
S = TypeVar("S", bound=Serializable)


class ManifestFileInfo(NamedTuple):
    """
    Internal helper class: When we write a file to disk, we need to know its SHA256 hash
    and length (in bytes).
    """

    hash: str
    """
    SHA256 hash of the file, represented as a base64 string
    """

    num_bytes: int
    """
    Length of the file in bytes
    """


@dataclass(eq=True, unsafe_hash=True)
class ManifestExternal(Serializable):
    """
    This class is the on-disk representation of the Manifest class. The only difference is that
    it doesn't have the `root_dir` or `subdirectories` fields, which wouldn't be necessary.
    """

    file_hashes: Dict[str, ManifestFileInfo]
    """Mapping from file names to hashes."""

    directory_hashes: Dict[str, ManifestFileInfo]
    """Mapping from directory names to subdirectory manifest hashes."""

    def to_manifest(
        self,
        root_dir: str,
        subdirectories: Optional[List[str]],
        manifest_hash: ManifestFileInfo,
    ) -> "Manifest":
        """
        Converts this to a Manifest class, suitable for working with in-memory.
        """
        if subdirectories is None:
            subdirectories = []

        return Manifest(
            root_dir,
            subdirectories,
            self.file_hashes,
            self.directory_hashes,
            manifest_hash,
        )


@dataclass(eq=True, unsafe_hash=True)
class Manifest:
    """
    This class is a front-end for writing files to disk that can also generate two useful things:
    a series of `index.html` pages for every subdirectory, as well as a top-level file, `MANIFEST.json`,
    which includes a JSON object mapping from filenames to their SHA256 hashes.

    Do not construct this directly. Instead, use `make_manifest_for_directory` or `load_existing_manifest`.
    """

    root_dir: str
    """Root directory of the whole manifest structure."""

    subdirectories: List[str]
    """Path from the root to get to this specific manifest."""

    file_hashes: Dict[str, ManifestFileInfo]
    """Mapping from file names to hashes."""

    directory_hashes: Dict[str, ManifestFileInfo]
    """Mapping from directory names to subdirectory manifest hashes."""

    subdirectory_manifests: Dict[str, "Manifest"]
    """Internal storage of manifests for subdirectories. Filled out on demand."""

    manifest_hash: ManifestFileInfo
    """Cached copy of the hash information on the manifest, itself."""

    def __init__(
        self,
        root_dir: str,
        subdirectories: List[str],
        file_hashes: Dict[str, ManifestFileInfo],
        directory_hashes: Dict[str, ManifestFileInfo],
        manifest_hash: ManifestFileInfo,
    ):
        self.root_dir = root_dir
        self.subdirectories = subdirectories if subdirectories else []
        self.file_hashes = file_hashes
        self.directory_hashes = directory_hashes
        self.manifest_hash = manifest_hash
        self.subdirectory_manifests = {}

    def read_json_file(
        self,
        file_name: str,
        class_handle: Type[S],
        subdirectories: List[str] = None,
    ) -> Optional[S]:
        """
        Reads the requested file, by name, returning its contents as a Python object for the given class handle.
        If no hash for the file is present, if the file doesn't match its known hash, or if the JSON deserialization
        process fails, then `None` will be returned and an error will be logged. If the `file_name`
        is actually a path-like object, the subdirectories are ignored.

        :param subdirectories: Path elements to be introduced between `root_dir` and the file; empty-list means
          no subdirectory. Ignored if the file_name is a path-like object.
        :param file_name: Name of the file, including any suffix, or a path-like object.
        :param class_handle: The class, itself, that we're trying to deserialize to (if None, then you get back
          whatever the JSON becomes, e.g., a dict).
        :returns: The contents of the file, or `None` if there was an error.
        """

        # this loads the file and verifies the hashes
        file_contents = self.read_file(file_name, subdirectories)
        return flatmap_optional(
            file_contents, lambda f: _decode_json_file_contents(f, class_handle)
        )

    def _validate_contents(self, filename: str, file_contents: str) -> bool:
        """
        Checks the manifest for the given file name. Returns True if the name is
        included in the manifest *and* the file_contents match the manifest. If anything
        is not properly validated, a suitable error will be written to the ElectionGuard log.
        """
        if filename not in self.file_hashes:
            log_error(f"File {filename} was not in the manifest")
            return False

        file_info = self.file_hashes[filename]
        file_len = len(file_contents.encode("utf-8"))

        if file_len != file_info.num_bytes:
            log_error(
                f"File {filename} did not have the expected length (expected: {file_info.num_bytes} bytes, actual: {file_len} bytes)"
            )
            return False

        data_hash = sha256_hash(file_contents)
        if data_hash != file_info.hash:
            log_error(
                f"File {filename} did not have the expected hash (expected: {file_info.hash}, actual: {data_hash})"
            )
            return False

        return True

    def _get_subdirectory_manifest(self, directory_name: str) -> Optional["Manifest"]:
        """
        Gets a manifest for the requested subdirectory. If it doesn't exist, or if
        the hash doesn't match, `None` is returned.
        """
        if directory_name in self.subdirectory_manifests:
            # this is our cache of already-validated subdirectories
            return self.subdirectory_manifests[directory_name]

        if directory_name not in self.directory_hashes:
            # this means that we're asking for a subdirectory we don't know about
            return None

        dir_manifest_hash, dir_manifest_num_bytes = self.directory_hashes[
            directory_name
        ]

        subdir_manifest = load_existing_manifest(
            self.root_dir,
            self.subdirectories + [directory_name],
            dir_manifest_hash,
            dir_manifest_num_bytes,
        )

        if not subdir_manifest:
            # no need to log errors here; they're logged by load_existing_manifest
            return None

        # cache the subdirectory manifest for future use
        self.subdirectory_manifests[directory_name] = subdir_manifest
        return subdir_manifest

    def _read_file_recursive(
        self, file_name: str, subdirectories: List[str]
    ) -> Optional[str]:
        """
        Recursively works through the subdirectories, checking that all hashes match,
        ultimately returning the contents of the file, if successful, or `None` if
        something went wrong.
        """
        if not subdirectories:
            file_contents = ray_load_file(self.root_dir, file_name, self.subdirectories)
            if file_contents is None:
                log_and_print(f"failed to load file: {file_name}", verbose=True)
                return None
            elif self._validate_contents(file_name, file_contents):
                return file_contents
            else:
                log_and_print(
                    f"failed to validate file: {file_name}, mismatching hashes",
                    verbose=True,
                )
                return None

        else:
            subdir_head = subdirectories[0]
            subdir_tail = subdirectories[1:]
            subdir_manifest = self._get_subdirectory_manifest(subdir_head)
            if subdir_manifest is None:
                log_and_print(
                    f"failed to find subdirectory manifest: {subdir_head}", verbose=True
                )
                return None
            return subdir_manifest._read_file_recursive(file_name, subdir_tail)

    def read_file(
        self, file_name: str, subdirectories: List[str] = None
    ) -> Optional[str]:
        """
        Reads the requested file, by name, returning its contents as a Python string.
        If no hash for the file is present, or if the file doesn't match its known
        hash, then `None` will be returned and an error will be logged. If the file_name
        is actually a path-like object, the subdirectories are ignored.

        :param subdirectories: Path elements to be introduced between `root_dir` and the file; empty-list means
          no subdirectory. Ignored if the file_name is a path-like object.
        :param file_name: Name of the file, including any suffix, or a path-like object.
        :returns: The contents of the file, or `None` if there was an error.
        """

        if not subdirectories:
            subdirectories = []

        return self._read_file_recursive(file_name, subdirectories)

    def load_ciphertext_ballot(
        self, ballot_id: str
    ) -> Optional[CiphertextAcceptedBallot]:
        """
        Given a manifest and a ballot identifier string, attempts to load the ballot
        from disk. Returns `None` if the ballot doesn't exist or if the hashes fail
        to verify.
        """
        ballot_name_prefix = ballot_id[0:BALLOT_FILENAME_PREFIX_DIGITS]
        return self.read_json_file(
            ballot_id + ".json",
            CiphertextAcceptedBallot,
            ["ballots", ballot_name_prefix],
        )

    def equivalent(self, other: "Manifest") -> bool:
        """
        Recursively wanders both this and the other manifest, to ensure that their
        hashes are all identical. The assumption is that the root directories are
        different, but we're testing that everything else is the same.
        """

        if self.subdirectories != other.subdirectories:
            return False

        if self.file_hashes != other.file_hashes:
            return False

        if self.directory_hashes != other.directory_hashes:
            return False

        if self.manifest_hash != other.manifest_hash:
            return False

        for d in self.directory_hashes.keys():
            my_sub = self._get_subdirectory_manifest(d)
            other_sub = other._get_subdirectory_manifest(d)
            if my_sub is None or other_sub is None or not my_sub.equivalent(other_sub):
                return False

        return True


@ray.remote
def _r_hash_file(
    root_dir: str,
    subdirectories: Optional[List[str]],
    filename: str,
    progress_actor: Optional[ActorHandle],
    logging_enabled: bool,
) -> Tuple[str, Optional[ManifestFileInfo]]:  # pragma: no cover
    """
    Hashes the file, returning the filename and sha256 hash (base64-encoded).
    If there's a failure, the result is the filename and `None`.
    """
    if progress_actor:
        progress_actor.update_num_concurrent.remote("Files", 1)

    if logging_enabled:
        log_and_print(
            f"_r_hash_file('{root_dir}', {subdirectories}, '{filename}')",
            verbose=True,
        )

    contents = ray_load_file(root_dir, filename, subdirectories)
    fileinfo = sha256_info(contents) if contents else None

    if progress_actor:
        progress_actor.update_num_concurrent.remote("Files", -1)
        progress_actor.update_completed.remote("Files", 1)

    return filename, fileinfo


@ray.remote
def _r_build_manifest_for_directory(
    root_dir: str,
    subdirectories: List[str],
    progress_actor: Optional[ActorHandle],
    num_retries: int,
    logging_enabled: bool,
) -> Tuple[str, Optional[ManifestFileInfo]]:  # pragma: no cover
    if progress_actor:
        progress_actor.update_num_concurrent.remote("Directories", 1)

    if logging_enabled:
        log_and_print(
            f"_r_build_manifest_for_directory('{root_dir}', {subdirectories})",
            verbose=True,
        )
    plain_files, directories = read_directory_contents(root_dir, subdirectories)

    if MANIFEST_FILE in plain_files:
        # we could just bomb out with an error; instead we're going to
        # log something, ignore the manifest file, and move onward
        if logging_enabled:
            log_and_print(
                f"Warning: found existing MANIFEST.json in {root_dir}/{'/'.join(subdirectories)}",
                verbose=True,
            )
        del plain_files[MANIFEST_FILE]

    if progress_actor:
        progress_actor.update_total.remote("Directories", len(directories))
        progress_actor.update_total.remote("Files", len(plain_files))

    # We're going to launch the file hashes first, such that directories,
    # which will potentially increase the number of tasks enormously, don't
    # execute until all these file hashes are done.
    file_hashes_r = [
        _r_hash_file.remote(
            root_dir, subdirectories, f, progress_actor, logging_enabled
        )
        for f in plain_files.keys()
    ]

    directory_manifests_r = [
        _r_build_manifest_for_directory.remote(
            root_dir, subdirectories + [d], progress_actor, num_retries, logging_enabled
        )
        for d in directories.keys()
    ]

    directory_hashes_l = ray.get(directory_manifests_r)
    file_hashes_l = ray.get(file_hashes_r)

    failures = any([x[1] is None for x in directory_hashes_l]) or any(
        [x[1] is None for x in file_hashes_l]
    )

    manifest_info: Optional[ManifestFileInfo] = None
    if not failures:
        directory_hashes: Dict[str, ManifestFileInfo] = {
            t[0]: t[1]
            for t in cast(List[Tuple[str, ManifestFileInfo]], directory_hashes_l)
        }
        file_hashes: Dict[str, ManifestFileInfo] = {
            t[0]: t[1] for t in cast(List[Tuple[str, ManifestFileInfo]], file_hashes_l)
        }
        result_ex = ManifestExternal(file_hashes, directory_hashes)
        manifest_info = _write_json_file_get_hash(
            file_name=MANIFEST_FILE,
            content_obj=result_ex,
            subdirectories=subdirectories,
            root_dir=root_dir,
            num_retries=num_retries,
        )

    if progress_actor:
        progress_actor.update_num_concurrent.remote("Directories", -1)
        progress_actor.update_completed.remote("Directories", 1)

    # the final element in the list of subdirectories is the name of "this" subdirectory
    if subdirectories:
        return subdirectories[-1:][0], manifest_info
    else:
        return "", manifest_info


def build_manifest_for_directory(
    root_dir: str,
    subdirectories: List[str] = None,
    show_progressbar: bool = False,
    num_write_retries: int = NUM_WRITE_RETRIES,
    logging_enabled: bool = False,
) -> Optional[str]:
    """
    Recursively walks down, starting at the requested subdirectory of the root directory,
    computing and writing out `MANIFEST.json` files. Returns the *root hash* as a string,
    if successful, otherwise `None`.

    This will launch a number of concurrent Ray tasks to accelerate the computation process.
    :param root_dir: a name for the directory about to be filled up with fresh files
    :param subdirectories: optional specification of a subdirectory in which to compute
    :param show_progressbar: if true, displays a tqdm progressbar for the computation
    :param num_write_retries: number of times to retry a failed write
    :param logging_enabled: adds additional logging and printing while the manifest is being built
    """

    if not subdirectories:
        subdirectories = []

    ray_wait_for_workers()

    pba: Optional[ActorHandle] = None
    if show_progressbar:
        pb = ProgressBar({"Files": 0, "Directories": 0})
        pba = pb.actor

    result: Tuple[str, Optional[ManifestFileInfo]] = ray.get(
        _r_build_manifest_for_directory.remote(
            root_dir, subdirectories, pba, num_write_retries, logging_enabled
        )
    )

    _, root_hash = result

    return flatmap_optional(root_hash, lambda r: r.hash)


def load_existing_manifest(
    root_dir: str,
    subdirectories: List[str] = None,
    expected_root_hash: Optional[str] = None,
    expected_num_bytes: int = -1,
) -> Optional[Manifest]:
    """
    Constructs a `Manifest` instance from a directory that contains a `MANIFEST.json` file.
    If the file is missing or something else goes wrong, you could get `None` as a result.
    :param expected_root_hash: optional string of the form produced by `sha256_hash`; validates the
      manifest against the hash, returns `None` and logs if there's a mismatch.
    :param expected_num_bytes: optional integer with the expected length, in bytes, of the
      manifest. Returns `None` and logs if there's a mismatch.
    :param root_dir: a name for the directory containing `MANIFEST.json` and other files.
    :param subdirectories: a list of subdirectories below the root directory
    """

    if subdirectories is None:
        subdirectories = []

    manifest_str = ray_load_file(
        root_dir=root_dir,
        subdirectories=subdirectories,
        file_name=MANIFEST_FILE,
    )

    if manifest_str is None:
        return None

    manifest_info = sha256_info(manifest_str)
    if expected_root_hash is not None:
        data_hash, num_bytes = manifest_info
        if data_hash != expected_root_hash:
            log_and_print(
                f"Manifest hash mismatch on {'/'.join(subdirectories)}/MANIFEST.json; expected {expected_root_hash}, got {data_hash}"
            )
            return None
        if expected_num_bytes > -1 and expected_num_bytes != num_bytes:
            log_and_print(
                f"Manifest length mismatch on {'/'.join(subdirectories)}/MANIFEST.json; expected {expected_num_bytes}, got {num_bytes}"
            )
            return None

    manifest_ex: Optional[ManifestExternal] = _decode_json_file_contents(
        manifest_str, class_handle=ManifestExternal
    )
    return flatmap_optional(
        manifest_ex, lambda m: m.to_manifest(root_dir, subdirectories, manifest_info)
    )


def sha256_hash(input: AnyStr) -> str:
    """
    Given a string or array of bytes, returns a base64-encoded representation of the
    256-bit SHA2-256 hash of that input string, first encoding the input string as UTF8.
    """
    return sha256_info(input).hash


def sha256_info(input: AnyStr) -> ManifestFileInfo:
    """
    Given a string or array of bytes, returns a base64-encoded representation of the
    256-bit SHA2-256 hash of that input string, first encoding the input string as UTF8,
    in `ManifestFileInfo` format.
    """
    h = sha256()
    if isinstance(input, str):
        encoded = input.encode("utf-8")
        num_bytes = len(encoded)
        h.update(encoded)
    else:
        num_bytes = len(input)
        h.update(input)

    hash_str = b64encode(h.digest()).decode("utf-8")
    return ManifestFileInfo(hash_str, num_bytes)


def _write_json_file_get_hash(
    file_name: str,
    content_obj: Serializable,
    subdirectories: List[str] = None,
    root_dir: str = ".",
    num_retries: int = 1,
) -> ManifestFileInfo:
    """
    A wrapper around `ray_write_json_file` that returns a `ManifestInfo` rather
    than nothing.
    """

    if subdirectories is None:
        subdirectories = []

    # If a manifest already exists, we're going to remove it; we don't want to
    # do this in general, but it's something that might happen when driving
    # manifest creation from the command-line.
    unlink_helper(compose_filename(root_dir, file_name, subdirectories))

    json_txt = content_obj.to_json(strip_privates=True)
    ray_write_file(
        file_name,
        json_txt,
        root_dir=root_dir,
        subdirectories=subdirectories,
        num_retries=num_retries,
    )
    return sha256_info(json_txt)
