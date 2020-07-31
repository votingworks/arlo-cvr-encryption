import unittest

import ray
from electionguard.elgamal import (
    ElGamalCiphertext,
    elgamal_encrypt,
    elgamal_keypair_random,
)
from electionguard.group import ElementModP, ElementModQ, int_to_q
from electionguard.nonces import Nonces

from arlo_e2e.ray_helpers import ray_init_localhost


@ray.remote
def r_encrypt(
    plaintext: int, nonce: ElementModQ, public_key: ray.ObjectID
) -> ElGamalCiphertext:
    l_public_key: ElementModP = ray.get(public_key)
    return elgamal_encrypt(plaintext, nonce, l_public_key)


@ray.remote
def r_square(i: int) -> int:
    return i * i


class TestRayBasics(unittest.TestCase):
    def test_hello_world(self):
        ray_init_localhost()
        inputs = range(0, 1000)
        serial_outputs = [i * i for i in inputs]
        parallel_outputs = ray.get([r_square.remote(i) for i in inputs])
        self.assertEqual(serial_outputs, parallel_outputs)

    def test_electionguard_basics(self):
        ray_init_localhost()
        plaintexts = range(0, 1000)
        nonces = Nonces(int_to_q(3))
        keypair = elgamal_keypair_random()
        r_public_key = ray.put(keypair.public_key)
        serial_ciphertexts = [
            elgamal_encrypt(p, n, keypair.public_key)
            for p, n in zip(plaintexts, nonces)
        ]
        parallel_ciphertexts = ray.get(
            [r_encrypt.remote(p, n, r_public_key) for p, n in zip(plaintexts, nonces)]
        )
        self.assertEqual(serial_ciphertexts, parallel_ciphertexts)
