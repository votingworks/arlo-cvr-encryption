import unittest
from time import sleep

import ray
from ray.actor import ActorHandle

from arlo_cvre.ray_helpers import ray_init_localhost
from arlo_cvre.ray_progress import ProgressBar


@ray.remote
def sleep_then_increment(i: int, pba: ActorHandle) -> int:
    sleep(i / 2.0)
    pba.update_completed.remote("A", 1)
    pba.update_completed.remote("B", 1)
    return i


class TestRayProgressBar(unittest.TestCase):
    def setUp(self) -> None:
        ray_init_localhost(num_cpus=2)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_ray_progressbar(self) -> None:
        num_ticks = 6
        pb = ProgressBar({"A": num_ticks, "B": num_ticks})
        actor = pb.actor
        tasks_pre_launch = [
            sleep_then_increment.remote(i, actor) for i in range(0, num_ticks)
        ]

        pb.print_until_done()
        tasks = ray.get(tasks_pre_launch)

        self.assertEqual(tasks, list(range(0, num_ticks)))
