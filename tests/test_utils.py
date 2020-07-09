import unittest

from utils import flatmap


class FlatmapTest(unittest.TestCase):
    def test_flatmap_basics(self):
        self.assertEqual([1, 2, 3, 4, 5, 6], flatmap(lambda x: [x, x + 1], [1, 3, 5]))
        self.assertEqual([], flatmap(lambda x: [x, x + 1], []))
        self.assertEqual([], flatmap(lambda x: [], [1, 3, 5]))
