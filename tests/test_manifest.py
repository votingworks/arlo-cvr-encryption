import shutil
import unittest
from datetime import timedelta
from os import path
from pathlib import PurePath
from typing import List

from electionguard.logs import log_warning
from hypothesis import given, assume, settings
from hypothesis.strategies import lists

from arlo_e2e.manifest import (
    make_fresh_manifest,
    make_existing_manifest,
    path_to_manifest_name,
)
from arlo_e2e.utils import mkdir_helper
from arlo_e2e_testing.manifest_hypothesis import (
    file_name_and_contents,
    FileNameAndContents,
)

MANIFEST_TESTING_DIR = "manifest_testing"


class TestManifestPublishing(unittest.TestCase):
    def _removeTree(self) -> None:
        try:
            shutil.rmtree(MANIFEST_TESTING_DIR)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def tearDown(self) -> None:
        self._removeTree()

    def setUp(self) -> None:
        log_warning("EXPECT MANY ERRORS TO BE LOGGED. THIS IS NORMAL.")

    @given(
        lists(
            min_size=2,
            max_size=10,
            elements=file_name_and_contents(),
            unique_by=lambda f: f.file_name,
        )
    )
    @settings(
        deadline=timedelta(milliseconds=50000),
    )
    def test_manifest(self, files: List[FileNameAndContents]) -> None:
        self._removeTree()
        mkdir_helper(MANIFEST_TESTING_DIR)

        for f in files:
            for f2 in files:
                if f.file_name != f2.file_name:
                    assume(f.file_contents != f2.file_contents)

        manifest = make_fresh_manifest(MANIFEST_TESTING_DIR)
        for file_name, file_path, file_contents in files:
            manifest.write_file(file_name, file_contents, file_path)
        manifest.write_manifest()

        manifest2 = make_existing_manifest(MANIFEST_TESTING_DIR)

        # we're going to remove the entry for MANIFEST.json just so we can test for equality of the result,
        # and tweak the number of bytes appropriately.
        manifest_bytes = manifest.hashes["MANIFEST.json"].num_bytes
        del manifest.hashes["MANIFEST.json"]
        manifest.bytes_written -= manifest_bytes
        self.assertEqual(manifest, manifest2)

        file_contents2 = [
            manifest2.read_file(file_name, file_path)
            for file_name, file_path, _ in files
        ]
        self.assertTrue(None not in file_contents2)
        self.assertListEqual([f.file_contents for f in files], file_contents2)

        manifest_names = [
            path_to_manifest_name(
                MANIFEST_TESTING_DIR,
                PurePath(
                    path.join(
                        path.join(MANIFEST_TESTING_DIR, *(f.file_path)), f.file_name
                    )
                ),
            )
            for f in files
        ]
        self.assertTrue(
            manifest2.validate_contents(manifest_names[0], files[0].file_contents)
        )
        self.assertTrue(
            manifest2.validate_contents(manifest_names[1], files[1].file_contents)
        )

        if files[1].file_contents != files[0].file_contents:
            self.assertFalse(
                manifest2.validate_contents(manifest_names[0], files[1].file_contents)
            )
            self.assertFalse(
                manifest2.validate_contents(manifest_names[1], files[0].file_contents)
            )

        self._removeTree()
