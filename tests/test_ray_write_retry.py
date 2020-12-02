import shutil
import unittest
from os import cpu_count
from pathlib import PurePath
from typing import List

import coverage
import ray

from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.ray_write_retry import (
    set_failure_probability_for_testing,
    write_file_with_retries,
    wait_for_zero_pending_writes,
    reset_pending_state,
)
from arlo_e2e.utils import mkdir_helper, all_files_in_directory


def write_all_files(num_files: int, num_retries: int = 10) -> None:
    for f in range(0, num_files):
        name = f"file{f:03d}"
        write_file_with_retries(f"write_output/{name}", name, num_retries, 1.0, 0.1)


def verify_all_files(num_files: int) -> bool:
    for f in range(0, num_files):
        name = f"file{f:03d}"
        with open(f"write_output/{name}", "r") as file:
            if file.read() != name:
                return False
    return True


def remove_test_tree() -> None:
    try:
        shutil.rmtree("write_output", ignore_errors=True)
    except FileNotFoundError:
        # okay if it's not there
        pass


class TestRayWriteRetry(unittest.TestCase):
    def setUp(self) -> None:
        remove_test_tree()

    def tearDown(self) -> None:
        remove_test_tree()

    def test_zero_failures(self) -> None:
        ray_init_localhost(num_cpus=cpu_count())
        coverage.process_startup()  # necessary for coverage testing to work in parallel
        set_failure_probability_for_testing(0.0)

        mkdir_helper("write_output")
        write_all_files(10)
        num_failures = wait_for_zero_pending_writes()
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(10))

        # while we're here, we'll throw in a test of a helper function in utils
        file_paths: List[PurePath] = all_files_in_directory("write_output")
        self.assertEqual(10, len(file_paths))

        remove_test_tree()
        ray.shutdown()
        reset_pending_state()

    def test_huge_failures(self) -> None:
        ray_init_localhost(num_cpus=cpu_count())
        coverage.process_startup()  # necessary for coverage testing to work in parallel
        set_failure_probability_for_testing(0.5)

        mkdir_helper("write_output")

        # up to 100 retries, driving odds of total failure to zero
        write_all_files(100, 100)
        num_failures = wait_for_zero_pending_writes()

        # our retries should guarantee everything succeeds!
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(100))
        remove_test_tree()
        ray.shutdown()
        reset_pending_state()

    def test_without_ray(self) -> None:
        if ray.is_initialized():
            ray.shutdown()

        set_failure_probability_for_testing(0.2)

        mkdir_helper("write_output")

        # up to 100 retries, driving odds of total failure to zero
        write_all_files(20, 100)
        num_failures = wait_for_zero_pending_writes()

        # our retries should guarantee everything succeeds!
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(20))
        remove_test_tree()
        reset_pending_state()
