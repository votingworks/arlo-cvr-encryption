import unittest

from hypothesis import given
from hypothesis.strategies import integers

from electionguard.elgamal import (
    elgamal_encrypt,
    ElGamalKeyPair,
)
from electionguard.group import unwrap_optional, ElementModQ
from electionguardtest.elgamal import arb_elgamal_keypair
from electionguardtest.group import arb_element_mod_q_no_zero
from hello import fib, elgamal_reencrypt


class FibTest(unittest.TestCase):
    def test_fib_basics(self):
        self.assertEqual(fib(0), 0)
        self.assertEqual(fib(1), 1)
        self.assertEqual(fib(2), 1)
        self.assertEqual(fib(3), 2)
        self.assertEqual(fib(4), 3)
        self.assertEqual(fib(5), 5)
        self.assertEqual(fib(6), 8)
        self.assertEqual(fib(7), 13)

    @given(integers(0, 10))
    def test_fib_is_positive(self, n: int):
        self.assertTrue(fib(n) >= 0)


class ElGamalTest(unittest.TestCase):
    @given(
        integers(0, 20),
        arb_element_mod_q_no_zero(),
        arb_element_mod_q_no_zero(),
        arb_elgamal_keypair(),
    )
    def test_reencryption(
        self,
        plaintext: int,
        nonce1: ElementModQ,
        nonce2: ElementModQ,
        keypair: ElGamalKeyPair,
    ):
        c1 = unwrap_optional(elgamal_encrypt(plaintext, nonce1, keypair.public_key))
        c2 = unwrap_optional(elgamal_reencrypt(keypair.public_key, nonce2, c1))
        self.assertEqual(plaintext, c1.decrypt(keypair.secret_key))
        self.assertNotEqual(c1, c2)
        self.assertEqual(plaintext, c2.decrypt(keypair.secret_key))
