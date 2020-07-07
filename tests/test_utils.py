import unittest
from time import sleep

from utils import flatmap, parallel_map_with_progress


class FlatmapTest(unittest.TestCase):
    def test_flatmap_basics(self):
        self.assertEqual([1, 2, 3, 4, 5, 6], flatmap(lambda x: [x, x + 1], [1, 3, 5]))
        self.assertEqual([], flatmap(lambda x: [x, x + 1], []))
        self.assertEqual([], flatmap(lambda x: [], [1, 3, 5]))


def sleepy_square(x: int) -> int:
    sleep(1)
    return x * x


class ParallelMap(unittest.TestCase):
    def test_parallel_map(self):
        input = range(1, 30)
        output = [x * x for x in input]
        self.assertEqual(output, parallel_map_with_progress(sleepy_square, input))
