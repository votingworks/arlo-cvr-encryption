import shutil
import unittest
from os import cpu_count

import ray

from arlo_e2e.io import (
    set_failure_probability_for_testing,
    wait_for_zero_pending_writes,
    reset_status_actor,
    make_file_ref,
    FileRef,
)
from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.utils import sha256_hash


@ray.remote
def r_write_file(fr: FileRef, contents: bytes, num_attempts: int) -> None:
    fr.write(contents, num_attempts=num_attempts)


def write_all_files(num_files: int, num_attempts: int = 10) -> None:
    for f in range(0, num_files):
        name = f"file{f:03d}"
        contents = name.encode("utf-8")
        fr = make_file_ref(file_name=name, root_dir="write_output")
        if ray.is_initialized():
            r_write_file.remote(fr, contents, num_attempts)
        else:
            fr.write(contents, num_attempts=num_attempts)


def verify_all_files(num_files: int) -> bool:
    failure = False
    for f in range(0, num_files):
        name = f"file{f:03d}"
        fr = make_file_ref(name, "write_output")
        contents = fr.read(expected_sha256_hash=sha256_hash(name))
        if contents is None:
            print(f"file {name} is missing!")
            failure = True
            continue
        if contents != name:
            print(f"file {name} has wrong contents: {contents}!")
            failure = True
            continue
    return not failure


def cleanup_between_tests() -> None:
    if ray.is_initialized():
        reset_status_actor()

    try:
        shutil.rmtree("write_output", ignore_errors=True)
    except FileNotFoundError:
        # okay if it's not there
        pass


class TestBasicReadsAndWrites(unittest.TestCase):
    def setUp(self) -> None:
        cleanup_between_tests()

    def tearDown(self) -> None:
        cleanup_between_tests()

    def test_file_sizes(self) -> None:
        fr = make_file_ref("testfile", root_dir="write_output")
        fr.write("123456789")
        self.assertEqual(9, fr.size())

        self.assertEqual(0, make_file_ref("testfile2", root_dir="write_output").size())
        self.assertEqual(0, make_file_ref("", root_dir="write_output").size())
        self.assertEqual(0, make_file_ref("", root_dir="write_output2").size())
        cleanup_between_tests()

    def test_basics(self) -> None:
        write_all_files(10)
        self.assertTrue(verify_all_files(10))

        dir_info = make_file_ref("", "write_output").scandir()
        self.assertEqual(10, len(dir_info.files))
        self.assertEqual(0, len(dir_info.subdirs))

        cleanup_between_tests()

    def test_hash_verification(self) -> None:
        fr = make_file_ref("test1", "write_output")
        fr.write("test contents")
        self.assertEqual("test contents", fr.read())
        self.assertEqual(
            "test contents", fr.read(expected_sha256_hash=sha256_hash("test contents"))
        )
        self.assertIsNone(fr.read(expected_sha256_hash=sha256_hash("wrong contents")))
        cleanup_between_tests()


class TestRayWriteRetry(unittest.TestCase):
    def setUp(self) -> None:
        cleanup_between_tests()

    def tearDown(self) -> None:
        cleanup_between_tests()
        if ray.is_initialized():
            ray.shutdown()

    def test_zero_failures(self) -> None:
        cleanup_between_tests()
        ray_init_localhost(num_cpus=cpu_count())
        set_failure_probability_for_testing(0.0)

        write_all_files(10)
        num_failures = wait_for_zero_pending_writes()
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(10))

        cleanup_between_tests()
        ray.shutdown()
        reset_status_actor()

    def test_huge_failures(self) -> None:
        cleanup_between_tests()
        ray_init_localhost(num_cpus=cpu_count())
        set_failure_probability_for_testing(0.2)

        # up to 100 retries, driving odds of total failure to zero
        write_all_files(100, 100)
        num_failures = wait_for_zero_pending_writes()

        # our retries should guarantee everything succeeds!
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(100))
        cleanup_between_tests()
        ray.shutdown()
        reset_status_actor()

    def test_without_ray(self) -> None:
        cleanup_between_tests()
        if ray.is_initialized():
            ray.shutdown()

        set_failure_probability_for_testing(0.2)

        # up to 100 retries, driving odds of total failure to zero
        write_all_files(20, 100)
        num_failures = wait_for_zero_pending_writes()

        # our retries should guarantee everything succeeds!
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(20))
        cleanup_between_tests()
        reset_status_actor()
