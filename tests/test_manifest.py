import shutil
import unittest
from datetime import timedelta
from os import path, getcwd
from pathlib import PurePath
from typing import List

from electionguard.logs import log_warning
from hypothesis import given, assume, settings
from hypothesis.strategies import lists, integers, booleans

from arlo_e2e.manifest import (
    make_fresh_manifest,
    make_existing_manifest,
    path_to_manifest_name,
)
from arlo_e2e.utils import mkdir_helper
from arlo_e2e_testing.manifest_hypothesis import (
    file_name_and_contents,
    FileNameAndContents,
    list_file_names_contents,
)

MANIFEST_TESTING_DIR = "manifest_testing"


class TestManifestPublishing(unittest.TestCase):
    def removeTree(self) -> None:
        try:
            shutil.rmtree(MANIFEST_TESTING_DIR, ignore_errors=True)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def tearDown(self) -> None:
        self.removeTree()

    def setUp(self) -> None:
        log_warning("EXPECT MANY ERRORS TO BE LOGGED. THIS IS NORMAL.")
        self.removeTree()

    @given(
        integers(2, 10).flatmap(lambda length: list_file_names_contents(length)),
        booleans(),
    )
    @settings(
        deadline=timedelta(milliseconds=50000),
    )
    def test_manifest(
        self, files: List[FileNameAndContents], use_absolute_paths: bool
    ) -> None:
        self.removeTree()

        manifest_testing_dir = (
            path.join(getcwd(), MANIFEST_TESTING_DIR)
            if use_absolute_paths
            else MANIFEST_TESTING_DIR
        )
        mkdir_helper(manifest_testing_dir)

        manifest = make_fresh_manifest(manifest_testing_dir)
        for file_name, file_path, file_contents in files:
            manifest.write_file(file_name, file_contents, file_path)
        manifest.write_manifest()
        self.assertTrue(manifest.all_hashes_unique())

        manifest2 = make_existing_manifest(manifest_testing_dir)
        self.assertTrue(manifest.equivalent(manifest2))

        file_contents2 = [
            manifest2.read_file(file_name, file_path)
            for file_name, file_path, _ in files
        ]
        self.assertTrue(None not in file_contents2)
        self.assertListEqual([f.file_contents for f in files], file_contents2)

        manifest_names = [
            path_to_manifest_name(
                manifest_testing_dir,
                PurePath(
                    path.join(
                        path.join(manifest_testing_dir, *f.file_path), f.file_name
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

        self.removeTree()

    @given(
        list_file_names_contents(5, file_name_prefix="list1_"),
        list_file_names_contents(5, file_name_prefix="list2_"),
    )
    @settings(
        deadline=timedelta(milliseconds=50000),
    )
    def test_manifest_merge(
        self, files: List[FileNameAndContents], files2: List[FileNameAndContents]
    ) -> None:
        self.removeTree()
        mkdir_helper(MANIFEST_TESTING_DIR)

        manifest1 = make_fresh_manifest(MANIFEST_TESTING_DIR)
        for file_name, file_path, file_contents in files:
            manifest1.write_file(file_name, file_contents, file_path)

        manifest2 = make_fresh_manifest(MANIFEST_TESTING_DIR)
        for file_name, file_path, file_contents in files2:
            manifest2.write_file(file_name, file_contents, file_path)

        manifest1.merge_from(manifest2)

        manifest1_keys = set(manifest1.hashes.keys())
        manifest2_keys = set(manifest1.hashes.keys())
        intersection_keys = manifest1_keys.intersection(manifest2_keys)

        self.assertEqual(manifest2_keys, intersection_keys)

        self.removeTree()

    @given(
        list_file_names_contents(5, file_name_prefix="list1_"),
        list_file_names_contents(5, file_name_prefix="list2_"),
        file_name_and_contents(),
    )
    @settings(
        deadline=timedelta(milliseconds=50000),
    )
    def test_manifest_merge_with_safe_overlap(
        self,
        files: List[FileNameAndContents],
        files2: List[FileNameAndContents],
        extra: FileNameAndContents,
    ) -> None:
        self.removeTree()
        mkdir_helper(MANIFEST_TESTING_DIR)

        manifest1 = make_fresh_manifest(MANIFEST_TESTING_DIR)
        for file_name, file_path, file_contents in files:
            manifest1.write_file(file_name, file_contents, file_path)
        manifest1.write_file(extra.file_name, extra.file_contents, extra.file_path)

        manifest2 = make_fresh_manifest(MANIFEST_TESTING_DIR)
        for file_name, file_path, file_contents in files2:
            manifest2.write_file(file_name, file_contents, file_path)
        manifest2.write_file(extra.file_name, extra.file_contents, extra.file_path)

        manifest1.merge_from(manifest2)

        manifest1_keys = set(manifest1.hashes.keys())
        manifest2_keys = set(manifest1.hashes.keys())
        intersection_keys = manifest1_keys.intersection(manifest2_keys)

        self.assertEqual(manifest2_keys, intersection_keys)

        self.removeTree()

    @given(
        list_file_names_contents(5, file_name_prefix="list1_"),
        list_file_names_contents(5, file_name_prefix="list2_"),
        file_name_and_contents(),
    )
    @settings(
        deadline=timedelta(milliseconds=50000),
    )
    def test_manifest_merge_with_unsafe_overlap(
        self,
        files: List[FileNameAndContents],
        files2: List[FileNameAndContents],
        extra: FileNameAndContents,
    ) -> None:
        self.removeTree()
        mkdir_helper(MANIFEST_TESTING_DIR)

        manifest1 = make_fresh_manifest(MANIFEST_TESTING_DIR)
        for file_name, file_path, file_contents in files:
            manifest1.write_file(file_name, file_contents, file_path)
        manifest1.write_file(extra.file_name, extra.file_contents, extra.file_path)

        manifest2 = make_fresh_manifest(MANIFEST_TESTING_DIR)
        for file_name, file_path, file_contents in files2:
            manifest2.write_file(file_name, file_contents, file_path)
        manifest2.write_file(
            extra.file_name, extra.file_contents + "something", extra.file_path
        )

        with self.assertRaises(RuntimeError):
            manifest1.merge_from(manifest2)

        self.removeTree()
