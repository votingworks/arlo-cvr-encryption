import re
import shutil
import unittest
from os import path

from arlo_e2e.manifest import sha256_hash
from arlo_e2e.constants import MANIFEST_FILE
from arlo_e2e.ray_io import ray_write_file_with_retries, mkdir_helper
from arlo_e2e.root_qrcode import gen_root_qrcode


def remove_test_tree() -> None:
    try:
        shutil.rmtree("write_output", ignore_errors=True)
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

    def tearDown(self) -> None:
        remove_test_tree()

    def test_simple(self) -> None:
        mkdir_helper("write_output")
        root_hash_text = "Some test text here"
        expected_hash = sha256_hash(root_hash_text)
        ray_write_file_with_retries(
            path.join("write_output", MANIFEST_FILE), root_hash_text
        )

        gen_root_qrcode(
            election_name="Test Election 2020",
            tally_dir="write_output",
            metadata=metadata,
        )

        # Reading the QRcode back in would require additional libraries, so we'll
        # cheat and just check that the QRcode is written to disk, at all, but we'll
        # scan for the hash written to the HTML text.

        self.assertTrue(path.exists(path.join("write_output", "root_hash.html")))
        self.assertTrue(path.exists(path.join("write_output", "root_hash_qrcode.png")))

        with open(path.join("write_output", "root_hash.html")) as f:
            lines = f.read().splitlines()
            for line in lines:
                if "Root Hash: <code>" in line:
                    match = re.search("<code>([A-Za-z0-9+/]+={0,2})</code></li>", line)
                    written_hash = match[1]
                    self.assertEqual(expected_hash, written_hash)

        remove_test_tree()

    def test_failures(self) -> None:
        mkdir_helper("write_output")

        # no MANIFEST written
        gen_root_qrcode(
            election_name="Test Election 2020",
            tally_dir="write_output",
            metadata=metadata,
        )

        self.assertFalse(path.exists(path.join("write_output", "root_hash.html")))
        self.assertFalse(path.exists(path.join("write_output", "root_hash_qrcode.png")))

        remove_test_tree()
