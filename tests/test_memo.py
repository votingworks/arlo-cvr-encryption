import unittest

from arlo_cvre.memo import make_memo_lambda, make_memo_value


class TestMemo(unittest.TestCase):
    def test_basics(self) -> None:
        m5 = make_memo_value(5)
        m0 = make_memo_value("zero")

        self.assertEqual(5, m5.contents)
        self.assertEqual("zero", m0.contents)

        self.assertNotEqual(m5, 5)
        self.assertNotEqual(m0, "zero")

        mm5 = make_memo_lambda(lambda: 5)
        mm0 = make_memo_lambda(lambda: "zero")

        self.assertEqual(str(mm5), "Memo(?)")
        self.assertEqual(str(mm0), "Memo(?)")

        self.assertEqual(5, mm5.contents)
        self.assertEqual("zero", mm0.contents)

        self.assertEqual(str(mm5), "Memo(5)")
        self.assertEqual(str(mm0), "Memo(zero)")

        self.assertEqual(m5, mm5)
        self.assertNotEqual(m0, mm5)

        self.assertEqual({m0, m5}, {mm0, mm5})

    def test_lambda_closure(self) -> None:
        # See the docstring for make_memo_lambda for details on why this is so weird.
        memos = [(lambda i: make_memo_lambda(lambda: i))(i) for i in range(0, 10)]
        values = [memo.contents for memo in memos]
        self.assertEqual(list(range(0, 10)), values)
