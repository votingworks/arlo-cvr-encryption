import unittest

from hypothesis import given
from hypothesis.strategies import integers

from electionguard.elgamal import (
    elgamal_encrypt,
    ElGamalKeyPair,
)
from electionguard.group import ElementModQ
from electionguard.utils import get_optional
from electionguardtest.elgamal import elgamal_keypairs
from electionguardtest.group import elements_mod_q_no_zero
from hello import fib, elgamal_reencrypt


class FibTest(unittest.TestCase):
    def test_fib_basics(self) -> None:
        self.assertEqual(fib(0), 0)
        self.assertEqual(fib(1), 1)
        self.assertEqual(fib(2), 1)
        self.assertEqual(fib(3), 2)
        self.assertEqual(fib(4), 3)
        self.assertEqual(fib(5), 5)
        self.assertEqual(fib(6), 8)
        self.assertEqual(fib(7), 13)

    @given(integers(0, 10))
    def test_fib_is_positive(self, n: int) -> None:
        self.assertTrue(fib(n) >= 0)


class ElGamalTest(unittest.TestCase):
    @given(
        integers(0, 20),
        elements_mod_q_no_zero(),
        elements_mod_q_no_zero(),
        elgamal_keypairs(),
    )
    def test_reencryption(
        self,
        plaintext: int,
        nonce1: ElementModQ,
        nonce2: ElementModQ,
        keypair: ElGamalKeyPair,
    ) -> None:
        c1 = get_optional(elgamal_encrypt(plaintext, nonce1, keypair.public_key))
        c2 = get_optional(elgamal_reencrypt(keypair.public_key, nonce2, c1))
        self.assertEqual(plaintext, c1.decrypt(keypair.secret_key))
        self.assertNotEqual(c1, c2)
        self.assertEqual(plaintext, c2.decrypt(keypair.secret_key))
