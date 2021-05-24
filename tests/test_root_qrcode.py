import re
import shutil
import unittest
from datetime import timedelta
from typing import List

from hypothesis import given, settings, HealthCheck, Phase
from hypothesis.strategies import integers

from arlo_cvre.io import make_file_ref
from arlo_cvre.manifest import build_manifest_for_directory, load_existing_manifest
from arlo_cvre.ray_helpers import ray_init_localhost
from arlo_cvre.root_qrcode import gen_root_qrcode
from arlo_cvre_testing.manifest_hypothesis import (
    list_file_names_contents,
    FileNameAndContents,
)

QRCODE_TESTING_DIR = "qrcode_test"


def remove_test_tree() -> None:
    try:
        shutil.rmtree(QRCODE_TESTING_DIR, ignore_errors=True)
    except FileNotFoundError:
        # okay if it's not there
        pass


metadata = {
    "s3_host": "amazonaws.com",
    "s3_bucket": "arlo-e2e-denver-demo",
    "s4_region": "us-east-2",
    "s3_directory": "test2020",
    "s3_directory_decrypted": "test2020_decrypted",
    "web_prefix": "http://arlo-e2e-denver-demo.s3-website.us-east-2.amazonaws.com/harris2020",
    "web_prefix_decrypted": "http://arlo-e2e-denver-demo.s3-website.us-east-2.amazonaws.com/harris2020_decrypted",
}


class TestRootQrCode(unittest.TestCase):
    def setUp(self) -> None:
        remove_test_tree()
        ray_init_localhost()

    def tearDown(self) -> None:
        remove_test_tree()

    def test_failures(self) -> None:
        # no MANIFEST written
        qr_test_dir_ref = make_file_ref(
            root_dir=QRCODE_TESTING_DIR, subdirectories=[], file_name=""
        )
        gen_root_qrcode(
            election_name="Test Election 2020",
            tally_dir_ref=qr_test_dir_ref,
            metadata=metadata,
        )

        self.assertFalse((qr_test_dir_ref + "root_hash.html").exists())
        self.assertFalse((qr_test_dir_ref + "root_hash_qrcode.png").exists())

        remove_test_tree()

    @given(integers(1, 20).flatmap(lambda n: list_file_names_contents(n)))
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=10,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_qrcode_end_to_end(self, contents: List[FileNameAndContents]) -> None:
        remove_test_tree()

        for c in contents:
            c.write(QRCODE_TESTING_DIR)

        qr_test_dir_ref = make_file_ref(
            root_dir=QRCODE_TESTING_DIR, subdirectories=[], file_name=""
        )

        # generate the manifest on disk
        root_hash = build_manifest_for_directory(
            root_dir_ref=qr_test_dir_ref, num_write_retries=1, logging_enabled=False
        )
        self.assertIsNotNone(root_hash)

        # now, build an in-memory manifest
        manifest = load_existing_manifest(
            root_dir_ref=qr_test_dir_ref, expected_root_hash=root_hash
        )
        self.assertIsNotNone(manifest)

        gen_root_qrcode(
            election_name="Test Election 2020",
            tally_dir_ref=qr_test_dir_ref,
            metadata=metadata,
        )

        # Reading the QRcode back in would require additional libraries, so we'll
        # cheat and just check that the QRcode is written to disk, at all, but we'll
        # scan for the hash written to the HTML text.

        self.assertTrue((qr_test_dir_ref + "root_hash.html").exists())
        self.assertTrue((qr_test_dir_ref + "root_hash_qrcode.png").exists())

        x = (qr_test_dir_ref + "root_hash.html").read()
        self.assertIsNotNone(x)

        if x is not None:
            lines = x.decode("utf-8").splitlines()
            for line in lines:
                if "Root Hash: <code>" in line:
                    match = re.search("<code>([A-Za-z0-9+/]+={0,2})</code></li>", line)
                    written_hash = match[1]
                    self.assertEqual(manifest.manifest_hash.hash, written_hash)

        remove_test_tree()
