import unittest
from datetime import timedelta
from typing import List, Optional

import ray
from electionguard.elgamal import (
    ElGamalCiphertext,
    elgamal_encrypt,
    ElGamalKeyPair,
    elgamal_add,
)
from electionguard.group import ElementModP, ElementModQ, int_to_q
from electionguard.nonces import Nonces
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import lists, integers
from ray import ObjectRef
from ray.actor import ActorHandle

from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.ray_progress import ProgressBar
from arlo_e2e.ray_reduce import ray_reduce_with_ray_wait, ray_reduce_with_rounds


@ray.remote
def r_encrypt(
    progressbar_actor: Optional[ActorHandle],
    plaintext: int,
    nonce: ElementModQ,
    public_key: ElementModP,
) -> ElGamalCiphertext:
    if not isinstance(public_key, ElementModP):
        # paranoid type checking, while still getting used to working with Ray
        print(f"expected ElementModP, got {str(type(public_key))}")

    if progressbar_actor:
        progressbar_actor.update_completed.remote("Ballots", 1)
    return elgamal_encrypt(plaintext, nonce, public_key)


@ray.remote
def r_elgamal_add(
    progressbar_actor: Optional[ActorHandle], *counters: ElGamalCiphertext
) -> ElGamalCiphertext:
    num_counters = len(counters)
    result = elgamal_add(*counters)
    if progressbar_actor:
        progressbar_actor.update_completed.remote("Tallies", num_counters)
    return result


class TestRayReduce(unittest.TestCase):
    def setUp(self) -> None:
        ray_init_localhost()

    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=10,
    )
    @given(
        lists(integers(min_value=0, max_value=1), min_size=20, max_size=200),
        elgamal_keypairs(),
    )
    def test_reduce_with_ray_wait_no_progress(
        self, counters: List[int], keypair: ElGamalKeyPair
    ) -> None:
        nonces = Nonces(int_to_q(3))[0 : len(counters)]

        ciphertexts: List[ObjectRef] = [
            r_encrypt.remote(None, p, n, keypair.public_key)
            for p, n in zip(counters, nonces)
        ]

        # compute in parallel
        ptotal = ray.get(
            ray_reduce_with_ray_wait(
                inputs=ciphertexts,
                shard_size=3,
                reducer_first_arg=None,
                reducer=r_elgamal_add.remote,
                progressbar=None,
                progressbar_key="Tallies",
                timeout=None,
                verbose=True,
            )
        )

        # recompute serially
        stotal = elgamal_add(*ray.get(ciphertexts))

        self.assertEqual(stotal, ptotal)

    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=10,
    )
    @given(
        lists(integers(min_value=0, max_value=1), min_size=20, max_size=200),
        elgamal_keypairs(),
    )
    def test_reduce_with_ray_wait_with_progress(
        self, counters: List[int], keypair: ElGamalKeyPair
    ) -> None:
        nonces = Nonces(int_to_q(3))[0 : len(counters)]
        pbar = ProgressBar(
            {"Ballots": len(counters), "Tallies": len(counters), "Iterations": 0}
        )

        ciphertexts: List[ObjectRef] = [
            r_encrypt.remote(pbar.actor, p, n, keypair.public_key)
            for p, n in zip(counters, nonces)
        ]

        # compute in parallel
        ptotal = ray.get(
            ray_reduce_with_ray_wait(
                inputs=ciphertexts,
                shard_size=3,
                reducer_first_arg=pbar.actor,
                reducer=r_elgamal_add.remote,
                progressbar=pbar,
                progressbar_key="Tallies",
                timeout=None,
                verbose=False,
            )
        )

        # recompute serially
        stotal = elgamal_add(*ray.get(ciphertexts))

        self.assertEqual(stotal, ptotal)

    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=10,
    )
    @given(
        lists(integers(min_value=0, max_value=1), min_size=20, max_size=200),
        elgamal_keypairs(),
    )
    def test_reduce_with_rounds_without_progress(
        self, counters: List[int], keypair: ElGamalKeyPair
    ) -> None:
        nonces = Nonces(int_to_q(3))[0 : len(counters)]

        ciphertexts: List[ObjectRef] = [
            r_encrypt.remote(None, p, n, keypair.public_key)
            for p, n in zip(counters, nonces)
        ]

        # compute in parallel
        ptotal = ray.get(
            ray_reduce_with_rounds(
                inputs=ciphertexts,
                shard_size=3,
                reducer_first_arg=None,
                reducer=r_elgamal_add.remote,
                progressbar=None,
                verbose=True,
            )
        )

        # recompute serially
        stotal = elgamal_add(*ray.get(ciphertexts))

        self.assertEqual(stotal, ptotal)

    @unittest.skip("doesn't complete here, but works fine elsewhere; weird")
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=10,
    )
    @given(
        lists(integers(min_value=0, max_value=1), min_size=20, max_size=200),
        elgamal_keypairs(),
    )
    def test_reduce_with_rounds_with_progress(
        self, counters: List[int], keypair: ElGamalKeyPair
    ) -> None:
        nonces = Nonces(int_to_q(3))[0 : len(counters)]
        pbar = ProgressBar(
            {"Ballots": len(counters), "Tallies": len(counters), "Iterations": 0}
        )

        ciphertexts: List[ObjectRef] = [
            r_encrypt.remote(pbar.actor, p, n, keypair.public_key)
            for p, n in zip(counters, nonces)
        ]

        # compute in parallel
        ptotal = ray.get(
            ray_reduce_with_rounds(
                inputs=ciphertexts,
                shard_size=3,
                reducer_first_arg=pbar.actor,
                reducer=r_elgamal_add.remote,
                progressbar=pbar,
                progressbar_key="Tallies",
                verbose=False,
            )
        )

        # recompute serially
        stotal = elgamal_add(*ray.get(ciphertexts))

        self.assertEqual(stotal, ptotal)
