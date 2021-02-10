import unittest
from datetime import timedelta
from typing import List, NamedTuple, Tuple
from timeit import default_timer as timer

from electionguard.elgamal import (
    ElGamalKeyPair,
    ElGamalCiphertext,
    elgamal_encrypt,
    elgamal_add,
    elgamal_keypair_from_secret,
)
from electionguard.group import int_to_q, rand_q, ElementModQ
from electionguard.nonces import Nonces
from electionguard.utils import get_optional
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import settings, HealthCheck, given
from hypothesis.strategies import lists, integers

from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.ray_map_reduce import MapReduceContext, RayMapReducer


class ElGamalEncryptor(MapReduceContext[Tuple[ElementModQ, int], ElGamalCiphertext]):
    keypair: ElGamalKeyPair

    def map(self, input: Tuple[ElementModQ, int]) -> ElGamalCiphertext:
        nonce, plaintext = input
        # print(f"map({plaintext})")
        return get_optional(elgamal_encrypt(plaintext, nonce, self.keypair.public_key))

    def reduce(self, *input: ElGamalCiphertext) -> ElGamalCiphertext:
        # print(f"reduce({len(input)}) inputs")
        return elgamal_add(*input)

    def zero(self) -> ElGamalCiphertext:
        # print("zero")
        nonce = rand_q()  # we'll get a different zero each time
        return self.map((nonce, 0))

    def __init__(self, keypair: ElGamalKeyPair):
        self.keypair = keypair


class TestRayMapReduce(unittest.TestCase):
    def setUp(self) -> None:
        ray_init_localhost()

    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=10,
    )
    @given(
        lists(integers(min_value=0, max_value=1), min_size=20, max_size=2000),
        elgamal_keypairs(),
        integers(min_value=1, max_value=10),
        integers(min_value=1, max_value=10),
        integers(min_value=2, max_value=10),
    )
    def test_map_reduce(
        self,
        counters: List[int],
        keypair: ElGamalKeyPair,
        max_tasks: int,
        map_shard_size: int,
        reduce_shard_size: int,
    ) -> None:
        self.map_reduce_helper(
            counters, keypair, max_tasks, map_shard_size, reduce_shard_size
        )

    def test_map_reduce_simplified(self) -> None:
        counters = [1, 0, 1]
        keypair = get_optional(elgamal_keypair_from_secret(int_to_q(3)))
        max_tasks = 2
        map_shard_size = 2
        reduce_shard_size = 2
        self.map_reduce_helper(
            counters, keypair, max_tasks, map_shard_size, reduce_shard_size
        )

    def map_reduce_helper(
        self,
        counters: List[int],
        keypair: ElGamalKeyPair,
        max_tasks: int,
        map_shard_size: int,
        reduce_shard_size: int,
    ) -> None:
        nonces = Nonces(int_to_q(3), "test-nonce-sequence")[0 : len(counters)]
        context = ElGamalEncryptor(keypair)

        # Apparently important: converting from a "zip object" to a "list" once, because
        # if you do it twice, the second time you get an empty-list out. Zip objects
        # aren't immutable!
        inputs = list(zip(nonces, counters))

        # reference solution: computed conventionally
        reference_start = timer()
        expected_sum = context.reduce(*[context.map(i) for i in inputs])
        reference_end = timer()

        log_and_print(
            f"Single-core reference: {len(counters) / (reference_end - reference_start):0.3f} encrypts/sec"
        )

        # now, with the map-reduce framework
        rmr = RayMapReducer(
            context,
            True,
            "Encrypts",
            "Adds",
            max_tasks=max_tasks,
            map_shard_size=map_shard_size,
            reduce_shard_size=reduce_shard_size,
        )

        actual_sum = rmr.map_reduce(*inputs)

        expected_plaintext = expected_sum.decrypt(keypair.secret_key)
        actual_plaintext = actual_sum.decrypt(keypair.secret_key)

        self.assertEqual(expected_plaintext, actual_plaintext)
        self.assertEqual(expected_sum, actual_sum)
