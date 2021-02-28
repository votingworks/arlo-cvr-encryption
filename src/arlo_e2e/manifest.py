from base64 import b64encode
from dataclasses import dataclass
from hashlib import sha256
from pathlib import PurePath
from typing import Dict, Optional, Type, List, Union, AnyStr, TypeVar

from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.logs import log_error
from electionguard.serializable import Serializable
from electionguard.utils import flatmap_optional

from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.ray_io import (
    compose_filename,
    ray_load_file,
    _decode_json_file_contents,
    BALLOT_FILENAME_PREFIX_DIGITS,
)

T = TypeVar("T")
S = TypeVar("S", bound=Serializable)


@dataclass(eq=True, unsafe_hash=True)
class FileInfo(Serializable):
    """
    Internal helper class: When we write a file to disk, we need to know its SHA256 hash
    and length (in bytes). This is returned from methods that write some things to disk.
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
    it doesn't have the `root_dir` field, which wouldn't make sense to write to disk.
    """

    hashes: Dict[str, FileInfo]
    bytes_written: int = 0

    def to_manifest(self, root_dir: str) -> "Manifest":
        """
        Converts this to a Manifest class, suitable for working with in-memory.
        """
        return Manifest(root_dir, self.hashes, self.bytes_written)


@dataclass(eq=True, unsafe_hash=True)
class Manifest:
    """
    This class is a front-end for writing files to disk that can also generate two useful things:
    a series of `index.html` pages for every subdirectory, as well as a top-level file, `MANIFEST.json`,
    which includes a JSON object mapping from filenames to their SHA256 hashes.

    Do not construct this directly. Instead, use `make_fresh_manifest` or `make_existing_manifest`.
    """

    root_dir: str
    hashes: Dict[str, FileInfo]
    bytes_written: int = 0

    def write_manifest(self, num_retries: int = 1) -> str:
        """
        Writes out `MANIFEST.json` into the existing `root_dir`, providing a mapping from filenames
        to their SHA256 hashes.
        :param num_retries: how many attempts to make writing the file; works around occasional network filesystem glitches
        :returns: the SHA256 hash of `MANIFEST.json`, itself
        """

        # Note that we don't want to have the manifest, itself, inside the manifest, so
        # we're going to use the skip_manifest flag.

        # TODO: write manifest to disk

        result = None
        #  ray_write_json_file(
        # "MANIFEST.json",
        # self.to_manifest_external(),
        # [],
        # skip_manifest=True,
        # num_retries=num_retries,
        # )
        return result

    def _get_hash_required(self, filename: str) -> Optional[FileInfo]:
        """
        Gets the hash for the requested filename (fully composed path, such as we might get from
        `utils.compose_filename`). If absent, logs an error and returns None.
        """
        hash = self.hashes[filename]
        if hash is None:
            log_error(f"No hash available for file: {filename}")
            return None
        return hash

    def read_json_file(
        self,
        file_name: Union[PurePath, str],
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

    def validate_contents(self, manifest_file_name: str, file_contents: str) -> bool:
        """
        Checks the manifest for the given file name. Returns True if the name is
        included in the manifest *and* the file_contents match the manifest. If anything
        is not properly validated, a suitable error will be written to the ElectionGuard log.
        """
        if manifest_file_name not in self.hashes:
            log_error(f"File {manifest_file_name} was not in the manifest")
            return False

        file_info: Optional[FileInfo] = self._get_hash_required(manifest_file_name)

        if file_info is None:
            return False

        file_len = len(file_contents.encode("utf-8"))

        if file_len != file_info.num_bytes:
            log_error(
                f"File {manifest_file_name} did not have the expected length (expected: {file_info.num_bytes} bytes, actual: {file_len} bytes)"
            )
            return False

        data_hash = sha256_hash(file_contents)
        if data_hash != file_info.hash:
            log_error(
                f"File {manifest_file_name} did not have the expected hash (expected: {file_info.hash}, actual: {data_hash})"
            )
            return False

        return True

    def read_file(
        self, file_name: Union[PurePath, str], subdirectories: List[str] = None
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
        if isinstance(file_name, PurePath):
            full_name = file_name
        else:
            full_name = compose_filename(self.root_dir, file_name, subdirectories)

        # TODO: recursive reads

        file_contents = ray_load_file(self.root_dir, full_name, subdirectories)
        # if file_contents is not None and self.validate_contents(
        #     manifest_name, file_contents
        # ):
        if False:  # TODO
            return file_contents
        else:
            return None

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

    def equivalent(self, manifest: "Manifest") -> bool:
        """
        Checks whether the other manifest is "equivalent" to this one, i.e., having
        all the same hashes for all the same files. If there are subdirectories,
        this will recursively traverse them on both sides.
        """
        return False


def make_manifest_for_directory(
    root_dir: str, subdirectories: Optional[List[str]] = None
) -> Manifest:
    """
    Constructs a fresh `Manifest` instance. Scans all files, computes their hashes,
    and so forth. This will launch a number of concurrent Ray tasks to accelerate
    the computation process.
    :param root_dir: a name for the directory about to be filled up with fresh files
    :param subdirectories: optional specification of a subdirectory in which to compute
    """

    # TODO: all the recursive / remote computation starts here!

    return Manifest(root_dir=root_dir, hashes={})


def make_existing_manifest(
    root_dir: str, expected_root_hash: Optional[str] = None
) -> Optional[Manifest]:
    """
    Constructs a `Manifest` instance from a directory that contains a `MANIFEST.json` file.
    If the file is missing or something else goes wrong, you could get `None` as a result.
    :param expected_root_hash: optional string of the form produced by `sha256_hash`; validates the
      manifest against the hash, returns `None` and logs if there's a mismatch
    :param root_dir: a name for the directory containing `MANIFEST.json` and other files.
    """
    manifest_str = ray_load_file(root_dir=root_dir, file_name="MANIFEST.json")

    if manifest_str is None:
        return None

    if expected_root_hash is not None:
        data_hash = sha256_hash(manifest_str)
        if data_hash != expected_root_hash:
            log_and_print(
                f"Root hash mismatch on MANIFEST.json; expected {expected_root_hash}, got {data_hash}"
            )
            return None

    manifest_ex: Optional[ManifestExternal] = _decode_json_file_contents(
        manifest_str, class_handle=ManifestExternal
    )
    return flatmap_optional(manifest_ex, lambda m: m.to_manifest(root_dir))


def sha256_hash(input: AnyStr) -> str:
    """
    Given a string or array of bytes, returns a base64-encoded representation of the
    256-bit SHA2-256 hash of that input string, first encoding the input string as UTF8.
    """
    h = sha256()
    if isinstance(input, str):
        h.update(input.encode("utf-8"))
    else:
        h.update(input)
    return b64encode(h.digest()).decode("utf-8")
