import unittest
from timeit import default_timer as timer
from typing import List

import ray
from electionguard.elgamal import (
    ElGamalCiphertext,
    elgamal_encrypt,
    elgamal_keypair_random,
)
from electionguard.group import ElementModP, ElementModQ, int_to_q
from electionguard.nonces import Nonces
from ray import ObjectRef

from arlo_e2e.ray_helpers import ray_init_localhost


@ray.remote
def r_encrypt(
    plaintext: int, nonce: ElementModQ, public_key: ElementModP
) -> ElGamalCiphertext:
    if not isinstance(public_key, ElementModP):
        # paranoid type checking, while still getting used to working with Ray
        print(f"expected ElementModP, got {str(type(public_key))}")

    return elgamal_encrypt(plaintext, nonce, public_key)


@ray.remote
def r_square(i: int) -> int:
    return i * i


class TestRayBasics(unittest.TestCase):
    def setUp(self) -> None:
        ray_init_localhost()

    def test_hello_world(self) -> None:
        inputs = range(0, 1000)
        serial_outputs = [i * i for i in inputs]
        parallel_outputs = ray.get([r_square.remote(i) for i in inputs])
        self.assertEqual(serial_outputs, parallel_outputs)

    def test_electionguard_basics(self) -> None:
        plaintexts = range(0, 1000)
        nonces = Nonces(int_to_q(3))
        keypair = elgamal_keypair_random()
        r_public_key = ray.put(keypair.public_key)

        start = timer()
        serial_ciphertexts: List[ElGamalCiphertext] = [
            elgamal_encrypt(p, n, keypair.public_key)
            for p, n in zip(plaintexts, nonces)
        ]
        serial_time = timer()

        # List[ObjectRef[ElGamalCiphertext]
        parallel_ciphertext_objects: List[ObjectRef] = [
            r_encrypt.remote(p, n, r_public_key) for p, n in zip(plaintexts, nonces)
        ]
        parallel_ciphertexts: List[ElGamalCiphertext] = ray.get(
            parallel_ciphertext_objects
        )

        parallel_time = timer()

        self.assertEqual(serial_ciphertexts, parallel_ciphertexts)
        print(
            f"Parallel speedup: {(serial_time - start) / (parallel_time - serial_time):.3f}x"
        )
