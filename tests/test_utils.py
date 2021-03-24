import unittest

from hypothesis import given
from hypothesis.strategies import integers

from arlo_e2e.utils import flatmap, shard_iterable_uniform, shard_iterable, first_n


class FlatmapTest(unittest.TestCase):
    def test_flatmap_basics(self) -> None:
        self.assertEqual(
            [1, 2, 3, 4, 5, 6], list(flatmap(lambda x: [x, x + 1], [1, 3, 5]))
        )
        self.assertEqual([], list(flatmap(lambda x: [x, x + 1], [])))
        self.assertEqual([], list(flatmap(lambda x: [], [1, 3, 5])))


class ShardingTest(unittest.TestCase):
    @given(integers(min_value=1, max_value=20), integers(min_value=1, max_value=1000))
    def test_shard_list(self, num_per_group: int, total_inputs: int) -> None:
        inputs = range(1, total_inputs + 1)
        shards = shard_iterable(inputs, num_per_group)

        # convert back to a list, because the iterable is consume-once
        shards = list(shards)
        shard_lengths = [len(x) for x in shards]
        min_length = min(shard_lengths)
        max_length = max(shard_lengths)

        self.assertTrue(min_length <= max_length)
        if total_inputs >= num_per_group:
            self.assertEqual(num_per_group, max_length)
        self.assertEqual(list(inputs), list(flatmap(lambda x: x, shards)))

    @given(integers(min_value=1, max_value=20), integers(min_value=1, max_value=1000))
    def test_shard_list_uniform(self, num_per_group: int, total_inputs: int) -> None:
        inputs = range(1, total_inputs + 1)
        shards = list(
            shard_iterable_uniform(inputs, num_per_group, num_inputs=total_inputs)
        )
        self.assertTrue(len(shards) > 0)
        lengths = [len(list(x)) for x in shards]
        min_length = min(lengths)
        max_length = max(lengths)
        self.assertTrue(min_length == max_length or min_length + 1 == max_length)
        self.assertTrue(max_length <= num_per_group)
        if num_per_group > 2 and total_inputs > 1:
            self.assertTrue(min_length >= 2)
        self.assertEqual(list(inputs), list(flatmap(lambda x: x, shards)))

    # special case handling for zero-length inputs
    def test_shard_list_zero_input(self) -> None:
        self.assertEqual([], list(shard_iterable([], 3)))
        self.assertEqual(
            [], list(shard_iterable_uniform([], num_per_group=4, num_inputs=0))
        )

    def test_first_n(self) -> None:
        self.assertEqual([1, 2, 3], first_n(range(1, 20), 3))
        self.assertEqual([1], first_n([1], 3))
        self.assertEqual([], first_n([], 3))

        # and now, with an internally mutating generator
        r = (n for n in range(1, 100))
        self.assertEqual([1, 2, 3, 4, 5], first_n(r, 5))
        self.assertEqual([6, 7, 8, 9, 10], first_n(r, 5))
