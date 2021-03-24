import shutil
import unittest
from os import cpu_count

import ray

from arlo_e2e.io import (
    set_failure_probability_for_testing,
    wait_for_zero_pending_writes,
    reset_status_actor,
    make_file_name,
)
from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.utils import sha256_hash


def write_all_files(num_files: int, num_retries: int = 10) -> None:
    for f in range(0, num_files):
        name = f"file{f:03d}"
        fn = make_file_name(name, "write_output")
        fn.write(name, num_attempts=num_retries)


def verify_all_files(num_files: int) -> bool:
    failure = False
    for f in range(0, num_files):
        name = f"file{f:03d}"
        fn = make_file_name(name, "write_output")
        contents = fn.read(expected_sha256_hash=sha256_hash(name))
        if contents is None:
            print(f"file {name} is missing!")
            failure = True
            continue
        if contents != name:
            print(f"file {name} has wrong contents: {contents}!")
            failure = True
            continue
    return not failure


def remove_test_tree() -> None:
    try:
        shutil.rmtree("write_output", ignore_errors=True)
    except FileNotFoundError:
        # okay if it's not there
        pass


class TestBasicReadsAndWrites(unittest.TestCase):
    def setUp(self) -> None:
        remove_test_tree()

    def tearDown(self) -> None:
        remove_test_tree()

    def test_basics(self) -> None:
        write_all_files(10)
        self.assertTrue(verify_all_files(10))

        dir_info = make_file_name("", "write_output").scandir()
        self.assertEqual(10, len(dir_info.files))
        self.assertEqual(0, len(dir_info.subdirs))

        remove_test_tree()

    def test_hash_verification(self) -> None:
        fn = make_file_name("test1", "write_output")
        fn.write("test contents")
        self.assertEqual("test contents", fn.read())
        self.assertEqual(
            "test contents", fn.read(expected_sha256_hash=sha256_hash("test contents"))
        )
        self.assertIsNone(fn.read(expected_sha256_hash=sha256_hash("wrong contents")))
        remove_test_tree()


class TestRayWriteRetry(unittest.TestCase):
    def setUp(self) -> None:
        remove_test_tree()

    def tearDown(self) -> None:
        remove_test_tree()
        if ray.is_initialized():
            ray.shutdown()

    def test_zero_failures(self) -> None:
        ray_init_localhost(num_cpus=cpu_count())
        set_failure_probability_for_testing(0.0)

        write_all_files(10)
        num_failures = wait_for_zero_pending_writes()
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(10))

        remove_test_tree()
        ray.shutdown()
        reset_status_actor()

    def test_huge_failures(self) -> None:
        ray_init_localhost(num_cpus=cpu_count())
        set_failure_probability_for_testing(0.5)

        # up to 100 retries, driving odds of total failure to zero
        write_all_files(100, 100)
        num_failures = wait_for_zero_pending_writes()

        # our retries should guarantee everything succeeds!
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(100))
        remove_test_tree()
        ray.shutdown()
        reset_status_actor()

    def test_without_ray(self) -> None:
        if ray.is_initialized():
            ray.shutdown()

        set_failure_probability_for_testing(0.2)

        # up to 100 retries, driving odds of total failure to zero
        write_all_files(20, 100)
        num_failures = wait_for_zero_pending_writes()

        # our retries should guarantee everything succeeds!
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(20))
        remove_test_tree()
        reset_status_actor()
