import unittest

from hypothesis import given
from hypothesis.strategies import integers

from arlo_e2e.utils import flatmap, shard_list_uniform, shard_list


class FlatmapTest(unittest.TestCase):
    def test_flatmap_basics(self) -> None:
        self.assertEqual([1, 2, 3, 4, 5, 6], flatmap(lambda x: [x, x + 1], [1, 3, 5]))
        self.assertEqual([], flatmap(lambda x: [x, x + 1], []))
        self.assertEqual([], flatmap(lambda x: [], [1, 3, 5]))


class ShardingTest(unittest.TestCase):
    @given(integers(min_value=1, max_value=20), integers(min_value=1, max_value=1000))
    def test_shard_list(self, num_per_group: int, total_inputs: int) -> None:
        inputs = list(range(1, total_inputs + 1))
        shards = shard_list(inputs, num_per_group)
        self.assertTrue(len(shards) > 0)
        min_length = min([len(x) for x in shards])
        max_length = max([len(x) for x in shards])
        self.assertTrue(min_length <= max_length)
        if total_inputs >= num_per_group:
            self.assertEqual(num_per_group, max_length)
        self.assertEqual(inputs, flatmap(lambda x: x, shards))

    @given(integers(min_value=1, max_value=20), integers(min_value=1, max_value=1000))
    def test_shard_list_uniform(self, num_per_group: int, total_inputs: int) -> None:
        inputs = list(range(1, total_inputs + 1))
        shards = shard_list_uniform(inputs, num_per_group)
        self.assertTrue(len(shards) > 0)
        min_length = min([len(x) for x in shards])
        max_length = max([len(x) for x in shards])
        self.assertTrue(min_length == max_length or min_length + 1 == max_length)
        self.assertTrue(max_length <= num_per_group)
        self.assertEqual(inputs, flatmap(lambda x: x, shards))

    # special case handling for zero-length inputs
    def test_shard_list_zero_input(self) -> None:
        self.assertEqual([], shard_list([], 3))
        self.assertEqual([], shard_list_uniform([], 3))
