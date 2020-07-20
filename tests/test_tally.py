import unittest
from datetime import timedelta
from io import StringIO
from multiprocessing import Pool, cpu_count

import coverage
from electionguard.elgamal import ElGamalKeyPair
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import settings, given, HealthCheck, Phase
from hypothesis.strategies import booleans

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.tally import fast_tally_everything
from arlo_e2e_testing.dominion_hypothesis import dominion_cvrs


class TestFastTallies(unittest.TestCase):
    @given(dominion_cvrs(), elgamal_keypairs(), booleans())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_end_to_end(
        self, input: str, keypair: ElGamalKeyPair, use_keypair: bool
    ) -> None:
        coverage.process_startup()  # necessary for coverage testing to work in parallel

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        pool = Pool(cpu_count())
        if use_keypair:
            tally = fast_tally_everything(
                cvrs, pool, verbose=True, secret_key=keypair.secret_key
            )
        else:
            tally = fast_tally_everything(cvrs, pool, verbose=True)
        self.assertTrue(tally.all_proofs_valid(verbose=True))
        pool.close()
