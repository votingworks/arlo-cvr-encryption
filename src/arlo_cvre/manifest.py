import time
from dataclasses import dataclass
from typing import (
    Dict,
    Optional,
    Type,
    List,
    TypeVar,
    Tuple,
    cast,
    NamedTuple,
    AnyStr,
)

import ray
from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.logs import log_error
from electionguard.serializable import Serializable
from electionguard.utils import flatmap_optional
from ray.actor import ActorHandle

from arlo_cvre.constants import (
    BALLOT_FILENAME_PREFIX_DIGITS,
    NUM_WRITE_RETRIES,
    MANIFEST_FILE,
)
from arlo_cvre.eg_helpers import log_and_print
from arlo_cvre.io import decode_json_file_contents, FileRef
from arlo_cvre.ray_helpers import ray_wait_for_workers
from arlo_cvre.ray_progress import ProgressBar
from arlo_cvre.utils import sha256_hash

T = TypeVar("T")
S = TypeVar("S", bound=Serializable)

# When we're computing the manifest, we can overwhelm the progress bar with updates,
# so we'll check the time, and once this many seconds pass, then we'll send an
# update to the progress bar.
SECONDS_PER_PROGRESS_UPDATE = 5


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
        root_dir_ref: FileRef,
        manifest_hash: ManifestFileInfo,
    ) -> "Manifest":
        """
        Converts this to a Manifest class, suitable for working with in-memory.
        """
        return Manifest(
            root_dir_ref,
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

    dir_ref: FileRef
    """Reference to the directory holding this manifest file."""

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
        root_dir_ref: FileRef,
        file_hashes: Dict[str, ManifestFileInfo],
        directory_hashes: Dict[str, ManifestFileInfo],
        manifest_hash: ManifestFileInfo,
    ):
        self.dir_ref = root_dir_ref
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

        :param subdirectories: Path elements to be introduced between the `root_dir_ref` and the file; empty-list means
          no subdirectory. Ignored if the file_name is a path-like object.
        :param file_name: Name of the file, including any suffix, or a path-like object.
        :param class_handle: The class, itself, that we're trying to deserialize to (if None, then you get back
          whatever the JSON becomes, e.g., a dict).
        :returns: The contents of the file, or `None` if there was an error.
        """

        # this loads the file and verifies the hashes
        file_contents = self.read_file(file_name, subdirectories)
        return flatmap_optional(
            file_contents,
            lambda f: decode_json_file_contents(f.decode("utf-8"), class_handle),
        )

    def _validate_contents(self, filename: str, file_contents: AnyStr) -> bool:
        """
        Checks the manifest for the given file name. Returns True if the name is
        included in the manifest *and* the file_contents match the manifest. If anything
        is not properly validated, a suitable error will be written to the ElectionGuard log.
        """
        if filename not in self.file_hashes:
            log_error(f"File {filename} was not in the manifest")
            return False

        file_info = self.file_hashes[filename]
        if isinstance(file_contents, str):
            file_len = len(file_contents.encode("utf-8"))
        else:
            file_len = len(file_contents)

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
            self.dir_ref / directory_name,
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
    ) -> Optional[bytes]:
        """
        Recursively works through the subdirectories, checking that all hashes match,
        ultimately returning the contents of the file, if successful, or `None` if
        something went wrong.
        """
        if not subdirectories:
            file_contents = (self.dir_ref + file_name).read()
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
    ) -> Optional[bytes]:
        """
        Reads the requested file, by name, returning its contents as an array of raw bytes.
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

        Note: this doesn't read the files, themselves, to validate that they
        match the hashes in the manifests.
        """

        if not (
            self.dir_ref == other.dir_ref
            and self.file_hashes == other.file_hashes
            and self.directory_hashes == other.directory_hashes
            and self.manifest_hash == other.manifest_hash
        ):
            return False

        for d in self.directory_hashes.keys():
            my_sub = self._get_subdirectory_manifest(d)
            other_sub = other._get_subdirectory_manifest(d)
            if my_sub is None or other_sub is None or not my_sub.equivalent(other_sub):
                return False

        return True


def hash_file(
    root_dir_ref: FileRef,
    filename: str,
    logging_enabled: bool,
) -> Tuple[str, Optional[ManifestFileInfo]]:  # pragma: no cover
    """
    Hashes the file, returning the filename and sha256 hash (base64-encoded).
    If there's a failure, the result is the filename and `None`.
    """
    if logging_enabled:
        log_and_print(
            f"hash_file('{str(root_dir_ref)}'/'{filename}')",
            verbose=True,
        )

    contents = (root_dir_ref + filename).read()
    fileinfo = sha256_manifest_info(contents) if contents else None

    return filename, fileinfo


@ray.remote
def _r_build_manifest_for_directory(
    root_dir_ref: FileRef,
    progress_actor: Optional[ActorHandle],
    num_attempts: int,
    logging_enabled: bool,
    overwrite_existing_manifests: bool,
) -> Tuple[str, Optional[ManifestFileInfo]]:  # pragma: no cover

    # Engineering note: this design is recursive, so for every directory level, we're
    # launching another call to this function. This causes various warnings from Ray
    # about nested tasks, and we end up with many more workers than we have CPU cores.
    # Nonetheless, this seems to run well, and there's actually a benefit to having
    # a huge number of tasks, since most of them are going to be blocking on IO, rather
    # than doing compute.

    # If this ever becomes a bigger problem, then we'll have to ditch all this pretty
    # recursion, and instead scan for all the filenames and run it through our map-reduce
    # library, where the map part gives us back [filename, hash] tuples, and the reduce
    # part perhaps smashes those together into a nested dictionary structure that can
    # later be written out to the filesystem.

    if progress_actor:
        progress_actor.update_num_concurrent.remote("Directories", 1)

    if logging_enabled:
        log_and_print(
            f"_r_build_manifest_for_directory({str(root_dir_ref)})",
            verbose=True,
        )
    dir_scan: FileRef.DirInfo = root_dir_ref.scandir()
    plain_files, file_sizes, directories = dir_scan

    if MANIFEST_FILE in plain_files:
        if overwrite_existing_manifests:
            plain_files[MANIFEST_FILE].unlink()
        else:
            # we could just bomb out with an error; instead we're going to
            # log something, ignore the manifest file, and move onward
            if logging_enabled:
                log_and_print(
                    f"Warning: found existing MANIFEST.json in {str(root_dir_ref)}",
                    verbose=True,
                )
        del plain_files[MANIFEST_FILE]

    if progress_actor:
        progress_actor.update_total.remote("Directories", len(directories))
        progress_actor.update_total.remote("Files", len(plain_files))

    # We're going to launch the recursive directory hashes first, which
    # will make sure we get our concurrency rolling, but then we're going
    # to do all the file hashes locally. While it seems kinda cool to run
    # all the file hashes concurrently as well, this turns out to run into
    # a rate-limiter in S3, where you get "SlowDown" errors coming back
    # when you read files too fast.
    # https://aws.amazon.com/premiumsupport/knowledge-center/s3-resolve-503-slowdown-throttling/

    # So, turns out that this rate limiter happens when we're talking about
    # objects with the same "prefix" (i.e., files in the same directory),
    # so we're not going to have a problem increasing concurrency by working
    # on multiple directories simultaneously. (Hopefully.)bb
    directory_hashes_r = [
        _r_build_manifest_for_directory.remote(
            root_dir_ref / d,
            progress_actor,
            num_attempts,
            logging_enabled,
            overwrite_existing_manifests,
        )
        for d in directories.keys()
    ]

    file_hashes_l: List[Tuple[str, Optional[ManifestFileInfo]]] = []
    timestamp = time.perf_counter()
    hashes_complete = 0
    for f in plain_files.keys():
        file_hashes_l.append(hash_file(root_dir_ref, f, logging_enabled))
        hashes_complete += 1
        new_time = time.perf_counter()
        if new_time - timestamp >= SECONDS_PER_PROGRESS_UPDATE and progress_actor:
            progress_actor.update_completed.remote("Files", hashes_complete)
            hashes_complete = 0
            timestamp = new_time

    if progress_actor and hashes_complete > 0:
        progress_actor.update_completed.remote("Files", hashes_complete)

    directory_hashes_l: List[Tuple[str, Optional[ManifestFileInfo]]] = ray.get(
        directory_hashes_r
    )
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
            file_ref=root_dir_ref + MANIFEST_FILE,
            content_obj=result_ex,
            num_attempts=num_attempts,
        )

    if progress_actor:
        progress_actor.update_num_concurrent.remote("Directories", -1)
        progress_actor.update_completed.remote("Directories", 1)

    # the final element in the list of subdirectories is the name of "this" subdirectory
    if root_dir_ref.subdirectories:
        return root_dir_ref.subdirectories[-1:][0], manifest_info
    else:
        return "", manifest_info


def build_manifest_for_directory(
    root_dir_ref: FileRef,
    show_progressbar: bool = False,
    num_write_retries: int = NUM_WRITE_RETRIES,
    logging_enabled: bool = False,
    overwrite_existing_manifests: bool = False,
) -> Optional[str]:
    """
    Recursively walks down, starting at the requested subdirectory of the root directory,
    computing and writing out `MANIFEST.json` files. Returns the *root hash* as a string,
    if successful, otherwise `None`.

    This will launch a number of concurrent Ray tasks to accelerate the computation process.
    :param root_dir_ref: a FileRef for the directory where the manifest is to be written
    :param show_progressbar: if true, displays a tqdm progressbar for the computation
    :param num_write_retries: number of times to retry a failed write
    :param logging_enabled: adds additional logging and printing while the manifest is being built
    :param overwrite_existing_manifests: if true, existing MANIFEST.json files will be overwritten
      rather than raising an error
    """

    ray_wait_for_workers()

    pb: Optional[ProgressBar] = None
    pba: Optional[ActorHandle] = None
    if show_progressbar:
        pb = ProgressBar({"Files": 0, "Directories": 0})
        pba = pb.actor

    task_ref = _r_build_manifest_for_directory.remote(
        root_dir_ref,
        pba,
        num_write_retries,
        logging_enabled,
        overwrite_existing_manifests,
    )

    if pb is not None:
        pb.print_until_ready(task_ref)
        # pb.print_until_done()

    result: Tuple[str, Optional[ManifestFileInfo]] = ray.get(task_ref)
    _, root_hash = result

    if pb is not None:
        pb.close()

    return flatmap_optional(root_hash, lambda r: r.hash)


def load_existing_manifest(
    root_dir_ref: FileRef,
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
    :param root_dir_ref: a FileRef for the directory containing `MANIFEST.json` and other files.
    """

    manifest_str = (root_dir_ref + MANIFEST_FILE).read()

    if manifest_str is None:
        return None

    manifest_info = sha256_manifest_info(manifest_str)
    if expected_root_hash is not None:
        data_hash, num_bytes = manifest_info
        if data_hash != expected_root_hash:
            log_and_print(
                f"Manifest hash mismatch on {str(root_dir_ref)}/MANIFEST.json; expected {expected_root_hash}, got {data_hash}"
            )
            return None
        if expected_num_bytes > -1 and expected_num_bytes != num_bytes:
            log_and_print(
                f"Manifest length mismatch on {str(root_dir_ref)}/MANIFEST.json; expected {expected_num_bytes}, got {num_bytes}"
            )
            return None

    manifest_ex: Optional[ManifestExternal] = decode_json_file_contents(
        manifest_str.decode("utf-8"), class_handle=ManifestExternal
    )
    return flatmap_optional(
        manifest_ex, lambda m: m.to_manifest(root_dir_ref, manifest_info)
    )


def _write_json_file_get_hash(
    file_ref: FileRef,
    content_obj: Serializable,
    num_attempts: int = 1,
) -> ManifestFileInfo:  # pragma: no cover
    """
    A wrapper around `ray_write_json_file` that returns a `ManifestInfo` rather
    than nothing.
    """

    # If a manifest already exists, we're going to remove it; we don't want to
    # do this in general, but it's something that might happen when driving
    # manifest creation from the command-line.
    file_ref.unlink()

    json_txt = content_obj.to_json(strip_privates=True)
    file_ref.write(
        json_txt,
        num_attempts=num_attempts,
    )
    return sha256_manifest_info(json_txt)


def sha256_manifest_info(input: AnyStr) -> ManifestFileInfo:
    """
    Given a string or array of bytes, returns a base64-encoded representation of the
    256-bit SHA2-256 hash of that input string, first encoding the input string as UTF8,
    in `ManifestFileInfo` format.
    """
    if isinstance(input, str):
        encoded = input.encode("utf-8")
        num_bytes = len(encoded)
    else:
        num_bytes = len(input)
        encoded = input

    return ManifestFileInfo(sha256_hash(encoded), num_bytes)
