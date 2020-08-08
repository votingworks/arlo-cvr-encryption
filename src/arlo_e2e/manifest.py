import shutil
from dataclasses import dataclass
from pathlib import PurePath
from typing import Dict, Optional, Type, List, Union

import jsons
from electionguard.logs import log_error
from electionguard.serializable import WRITE, Serializable
from electionguard.utils import flatmap_optional

from arlo_e2e.utils import (
    load_json_helper,
    sha256_hash,
    load_file_helper,
    compose_filename,
    T,
    compose_manifest_name,
    mkdir_list_helper,
    filename_to_manifest_name,
)


@dataclass(eq=True, unsafe_hash=True)
class ManifestExternal(Serializable):
    """
    This class is the on-disk representation of the Manifest class. The only difference is that
    it doesn't have the `root_dir` field, which wouldn't make sense to write to disk.
    """

    hashes: Dict[str, int]
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
    hashes: Dict[str, int]
    bytes_written: int = 0

    def to_manifest_external(self) -> ManifestExternal:
        """
        Converts this to a ManifestExternal class, suitable for serializing to disk.
        """
        return ManifestExternal(self.hashes, self.bytes_written)

    def write_json_file(
        self,
        file_name: str,
        content_obj: Serializable,
        subdirectories: List[str] = None,
    ) -> int:
        """
        Given a filename, subdirectory, and contents of the file, writes the contents out to the file. As a
        side-effect, the full filename and its contents' hash are remembered in `self.hashes`, to be written
        out later with a call to `write_manifest`.

        :param subdirectories: paths to be introduced between `root_dir` and the file; empty-list means no subdirectory
        :param file_name: name of the file, including any suffix
        :param content_obj: any ElectionGuard "Serializable" object
        :returns: the SHA256 hash of `file_contents`
        """

        json_txt = content_obj.to_json(strip_privates=True)
        return self.write_file(file_name, json_txt, subdirectories)

    def write_file(
        self, file_name: str, file_contents: str, subdirectories: List[str] = None
    ) -> int:
        """
        Given a filename, subdirectory, and contents of the file, writes the contents out to the file. As a
        side-effect, the full filename and its contents' hash are remembered in `self.hashes`, to be written
        out later with a call to `write_manifest`.

        :param subdirectories: paths to be introduced between `root_dir` and the file; empty-list means no subdirectory
        :param file_name: name of the file, including any suffix
        :param file_contents: string to be written to the file
        :returns: the SHA256 hash of `file_contents`
        """
        mkdir_list_helper(self.root_dir, subdirectories)
        hash = sha256_hash(file_contents)
        self.bytes_written += len(file_contents.encode("utf-8"))
        full_name = compose_filename(self.root_dir, file_name, subdirectories)
        manifest_name = compose_manifest_name(file_name, subdirectories)
        self.hashes[manifest_name] = hash
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
        return self.write_json_file("MANIFEST.json", self.to_manifest_external(), [])

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
        file_name: Union[PurePath, str],
        class_handle: Type[Serializable[T]],
        subdirectories: List[str] = None,
    ) -> Optional[T]:
        """
        Reads the requested file, by name, returning its contents as a Python object for the given class handle.
        If no hash for the file is present, if the file doesn't match its known hash, or if the JSON deserialization
        process fails, then `None` will be returned and an error will be logged.

        :param subdirectories: path elements to be introduced between `root_dir` and the file; empty-list means no subdirectory
        :param file_name: name of the file, including any suffix
        :param class_handle: the class, itself, that we're trying to deserialize to (if None, then you get back
          whatever the JSON becomes, e.g., a dict)
        :returns: the contents of the file, or `None` if there was an error
        """

        if isinstance(file_name, PurePath):
            full_name = file_name
        else:
            full_name = compose_filename(self.root_dir, file_name, subdirectories)
        manifest_name = filename_to_manifest_name(self.root_dir, file_name)

        return flatmap_optional(
            self._get_hash_required(manifest_name),
            lambda hash: load_json_helper(
                root_dir=self.root_dir,
                file_name=full_name,
                class_handle=class_handle,
                expected_sha256=hash,
            ),
        )

    def read_file(
        self, file_name: str, subdirectories: List[str] = None
    ) -> Optional[str]:
        """
        Reads the requested file, by name, returning its contents as a Python string.
        If no hash for the file is present, or if the file doesn't match its known
        hash, then `None` will be returned and an error will be logged.

        :param subdirectories: path elements to be introduced between `root_dir` and the file; empty-list means no subdirectory
        :param file_name: name of the file, including any suffix
        :returns: the contents of the file, or `None` if there was an error
        """
        manifest_name = compose_manifest_name(file_name, subdirectories)
        return flatmap_optional(
            self._get_hash_required(manifest_name),
            lambda hash: load_file_helper(
                self.root_dir, file_name, subdirectories, hash
            ),
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
    manifest_ex: Optional[ManifestExternal] = load_json_helper(
        root_dir=root_dir, file_name="MANIFEST.json", class_handle=ManifestExternal
    )
    return flatmap_optional(manifest_ex, lambda m: m.to_manifest(root_dir))
