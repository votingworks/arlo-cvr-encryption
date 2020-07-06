import unittest
from typing import Optional

from hypothesis import given
from hypothesis.strategies import integers

from electionguard.elgamal import (
    elgamal_encrypt,
    ElGamalKeyPair,
    ElGamalCiphertext,
    elgamal_add,
)
from electionguard.group import ElementModQ, ElementModP
from electionguard.utils import get_optional, flatmap_optional
from electionguardtest.elgamal import elgamal_keypairs
from electionguardtest.group import elements_mod_q_no_zero

# The tests here are really just a sanity test to make sure that we can integrate with ElectionGuard.
# If this test doesn't work, then the most likely culprit is something wrong with pipenv.


def elgamal_reencrypt(
    public_key: ElementModP, nonce: ElementModQ, ciphertext: ElGamalCiphertext
) -> Optional[ElGamalCiphertext]:
    return flatmap_optional(
        elgamal_encrypt(0, nonce, public_key),
        lambda zero: elgamal_add(zero, ciphertext),
    )


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
