import shutil
import unittest
from os import cpu_count

import coverage
import ray

from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.ray_write_retry import (
    set_failure_probability_for_testing,
    write_file_with_retries,
    get_status_actor,
)
from arlo_e2e.utils import mkdir_helper


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


class TestRayWriteRetry(unittest.TestCase):
    def removeTree(self) -> None:
        try:
            shutil.rmtree("write_output", ignore_errors=True)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def setUp(self) -> None:
        ray_init_localhost(num_cpus=cpu_count())

        self.removeTree()
        coverage.process_startup()  # necessary for coverage testing to work in parallel

    def tearDown(self) -> None:
        self.removeTree()

    def testWithZeroFailureRate(self) -> None:
        set_failure_probability_for_testing(0.0)

        mkdir_helper("write_output")
        write_all_files(10)
        num_failures = ray.get(get_status_actor().wait_for_zero_pending.remote())
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(10))
        self.removeTree()

    def testWithHugeFailureRate(self) -> None:
        set_failure_probability_for_testing(0.5)

        mkdir_helper("write_output")
        write_all_files(
            100, 100
        )  # up to 100 retries, driving odds of total failure to zero
        num_failures = ray.get(get_status_actor().wait_for_zero_pending.remote())
        self.assertEqual(
            num_failures, 0
        )  # our retries should guarantee everything succeeds!
        self.assertTrue(verify_all_files(100))
        self.removeTree()
