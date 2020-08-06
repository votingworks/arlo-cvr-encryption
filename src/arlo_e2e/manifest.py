import shutil
from dataclasses import dataclass
from typing import Dict, Optional, Type

import jsons
from electionguard.logs import log_error
from electionguard.serializable import WRITE
from electionguard.utils import flatmap_optional

from arlo_e2e.utils import (
    load_json_helper,
    sha256_hash,
    T,
    load_file_helper,
    compose_filename,
)


@dataclass(eq=True, unsafe_hash=True)
class Manifest:
    """
    This class is a front-end for writing files to disk that can also generate two useful things:
    a series of `index.html` pages for every subdirectory, as well as a top-level file, `MANIFEST.json`,
    which includes a JSON object mapping from filenames to their SHA256 hashes.

    Do not construct this directly. Instead, use `make_fresh_manifest` or `make_existing_manifest`.
    """

    root_dir: str
    hashes: Dict[str, int]

    def write_file(
        self, file_name: str, subdirectory: str = "", file_contents: str = ""
    ) -> int:
        """
        Given a filename, subdirectory, and contents of the file, writes the contents out to the file. As a
        side-effect, the full filename and its contents' hash are remembered in `self.hashes`, to be written
        out later with a call to `write_manifest`.

        :param subdirectory: path to be introduced between `root_dir` and the file; empty-string means no subdirectory
        :param file_name: name of the file, including any suffix
        :returns: the SHA256 hash of `file_contents`
        """
        hash = sha256_hash(file_contents)
        full_name = compose_filename(self.root_dir, file_name, subdirectory)
        self.hashes[full_name] = hash
        with open(full_name, WRITE) as f:
            f.write(file_contents)
        return hash

    def write_html_indices(self, title: str, front_page_contents: str) -> None:
        # TODO: write out index.html files that have links to every other file in the indices as well
        #   as some meaningful "front page contents", perhaps as simple as the name of the election,
        #   and some sort of navigation hyperlinks.
        pass

    def write_manifest(self) -> int:
        """
        Writes out `MANIFEST.json` into the existing `root_dir`, providing a mapping from filenames
        to their SHA256 hashes.
        :returns: the SHA256 hash of `MANIFEST.json`
        """

        # As a side-effect, this will also add a hash for the manifest itself into hashes, but that's
        # something of an oddball case that won't ever matter in practice.
        return self.write_file("MANIFEST.json", "", jsons.dumps(self.hashes))

    def _get_hash_required(self, filename: str) -> Optional[int]:
        """
        Gets the hash for the requested filename (fully composed path, such as we might get from
        utils.compose_filename). If absent, logs an error and returns None.
        """
        hash = self.hashes[filename]
        if hash is None:
            log_error(f"No hash available for file: {filename}")
            return None
        return hash

    def read_json_file(
        self,
        file_name: str,
        subdirectory: str = "",
        class_handle: Optional[Type[T]] = None,
    ) -> Optional[T]:
        """
        Reads the requested file, by name, returning its contents as a Python object for the given class handle.
        If no hash for the file is present, if the file doesn't match its known hash, or if the JSON deserialization
        process fails, then `None` will be returned and an error will be logged.

        :param subdirectory: path to be introduced between `root_dir` and the file; empty-string means no subdirectory
        :param file_name: name of the file, including any suffix
        :param class_handle: the class, itself, that we're trying to deserialize to (if None, then you get back
          whatever the JSON becomes, e.g., a dict)
        :returns: the contents of the file, or `None` if there was an error
        """
        full_name = compose_filename(self.root_dir, file_name, subdirectory)
        return flatmap_optional(
            self._get_hash_required(full_name),
            lambda hash: load_json_helper(
                root_dir=self.root_dir,
                file_prefix=file_name,
                class_handle=class_handle,
                file_suffix="",
                subdirectory=subdirectory,
                expected_sha256=hash,
            ),
        )

    def read_file(self, file_name: str, subdirectory: str = "") -> Optional[str]:
        """
        Reads the requested file, by name, returning its contents as a Python string.
        If no hash for the file is present, or if the file doesn't match its known
        hash, then `None` will be returned and an error will be logged.

        :param subdirectory: path to be introduced between `root_dir` and the file; empty-string means no subdirectory
        :param file_name: name of the file, including any suffix
        :returns: the contents of the file, or `None` if there was an error
        """
        full_name = compose_filename(self.root_dir, file_name, subdirectory)
        return flatmap_optional(
            self._get_hash_required(full_name),
            lambda hash: load_file_helper(self.root_dir, file_name, subdirectory, hash),
        )


def make_fresh_manifest(root_dir: str, delete_existing: bool = False) -> Manifest:
    """
    Constructs a fresh `Manifest` instance.
    :param root_dir: a name for the directory about to be filled up with fresh files
    :param delete_existing: if true, will delete any existing files in the given root directory (false by default)
    """
    if delete_existing:
        try:
            shutil.rmtree(root_dir)
        except FileNotFoundError:
            pass

    return Manifest(root_dir=root_dir, hashes={})


def make_existing_manifest(root_dir: str) -> Optional[Manifest]:
    """
    Constructs a `Manifest` instance from a directory that contains a `MANIFEST.json` file.
    If the file is missing or something else goes wrong, you could get `None` as a result.
    :param root_dir: a name for the directory containing `MANIFEST.json` and other files.
    """
    return flatmap_optional(
        load_json_helper(root_dir, "MANIFEST"),
        lambda hashes: Manifest(root_dir=root_dir, hashes=hashes),
    )
