import shutil
import unittest
from datetime import timedelta
from typing import List

import ray
from electionguard.logs import log_warning
from hypothesis import given, settings, HealthCheck, Phase
from hypothesis.strategies import integers

from arlo_cvre.io import make_file_ref
from arlo_cvre.manifest import load_existing_manifest, build_manifest_for_directory
from arlo_cvre.ray_helpers import ray_init_localhost
from arlo_e2e_testing.manifest_hypothesis import (
    FileNameAndContents,
    list_file_names_contents,
)

MANIFEST_TESTING_DIR = "manifest_testing"
MANIFEST_TESTING_DIR2 = "manifest_testing2"


class TestManifestPublishing(unittest.TestCase):
    def removeTree(self) -> None:
        try:
            shutil.rmtree(MANIFEST_TESTING_DIR, ignore_errors=True)
            shutil.rmtree(MANIFEST_TESTING_DIR2, ignore_errors=True)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def tearDown(self) -> None:
        self.removeTree()
        ray.shutdown()

    def setUp(self) -> None:
        log_warning("EXPECT MANY ERRORS TO BE LOGGED. THIS IS NORMAL.")
        self.removeTree()
        ray_init_localhost()

    @given(integers(1, 20).flatmap(lambda n: list_file_names_contents(n)))
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=20,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_manifest_end_to_end(self, contents: List[FileNameAndContents]) -> None:
        self.removeTree()

        for c in contents:
            c.write(MANIFEST_TESTING_DIR)

        manifest_dir_ref = make_file_ref(
            file_name="", subdirectories=[], root_dir=MANIFEST_TESTING_DIR
        )

        # generate the manifest on disk
        root_hash = build_manifest_for_directory(
            manifest_dir_ref, num_write_retries=1, logging_enabled=False
        )
        self.assertIsNotNone(root_hash)

        # now, build an in-memory manifest
        manifest = load_existing_manifest(
            manifest_dir_ref, expected_root_hash=root_hash
        )
        self.assertIsNotNone(manifest)

        for c in contents:
            bits = manifest.read_file(c.file_name, c.file_path)
            self.assertIsNotNone(bits)
            self.assertEqual(c.file_contents, bits.decode("utf-8"))

        # now, write out a second directory, same contents, so we can
        # check that we get identical hashes
        manifest_dir_ref2 = make_file_ref(
            file_name="", subdirectories=[], root_dir=MANIFEST_TESTING_DIR2
        )

        for c in contents:
            c.write(MANIFEST_TESTING_DIR2)
        root_hash2 = build_manifest_for_directory(
            manifest_dir_ref2, num_write_retries=1, logging_enabled=False
        )
        self.assertIsNotNone(root_hash2)

        # now, build an in-memory manifest
        manifest2 = load_existing_manifest(
            manifest_dir_ref2, expected_root_hash=root_hash2
        )
        self.assertIsNotNone(manifest2)
        self.assertEqual(root_hash, root_hash2)

        self.assertTrue(manifest.equivalent(manifest2))

        # next up, inducing errors; add a file that's not already there; reading should fail
        make_file_ref(file_name="something-else", root_dir=MANIFEST_TESTING_DIR).write(
            "unexpected content"
        )
        self.assertIsNone(manifest.read_file("something-else"))
        self.assertIsNone(manifest.read_file("something-else-missing"))

        # and, lastly, try modifying the existing files; reading should fail
        for c in contents:
            c.overwrite(MANIFEST_TESTING_DIR, "something-unexpected")
            self.assertIsNone(manifest.read_file(c.file_name, c.file_path))

        self.removeTree()
