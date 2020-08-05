import unittest
from datetime import timedelta
from io import StringIO

import coverage
from electionguard.elgamal import ElGamalKeyPair
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import settings, given, HealthCheck, Phase
from hypothesis.strategies import booleans

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.ray_helpers import ray_init_localhost, ray_shutdown_localhost
from arlo_e2e.ray_tally import ray_tally_everything
from arlo_e2e_testing.dominion_hypothesis import dominion_cvrs


class TestRayTallies(unittest.TestCase):
    def setUp(self) -> None:
        ray_init_localhost()

    def tearDown(self) -> None:
        ray_shutdown_localhost()

    @given(dominion_cvrs(max_rows=50), elgamal_keypairs(), booleans())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_ray_end_to_end(
        self, input: str, keypair: ElGamalKeyPair, use_keypair: bool
    ) -> None:
        coverage.process_startup()  # necessary for coverage testing to work in parallel

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        if use_keypair:
            tally = ray_tally_everything(
                cvrs, verbose=True, secret_key=keypair.secret_key
            )
        else:
            tally = ray_tally_everything(cvrs, verbose=True)
        self.assertTrue(tally.all_proofs_valid(verbose=False))
