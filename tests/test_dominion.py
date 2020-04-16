import unittest

from dominion import fix_strings


class TestDominion(unittest.TestCase):
    def test_fix_strings(self):
        self.assertEqual(0, fix_strings(""))
        self.assertEqual(0, fix_strings("0"))
        self.assertEqual(0, fix_strings(float("nan")))
        self.assertEqual(0, fix_strings('"0"'))
        self.assertEqual(0, fix_strings(0))
        self.assertEqual(0, fix_strings(0.0001))
        self.assertEqual(0.2, fix_strings(0.2))
        self.assertEqual("Hello", fix_strings("Hello"))
