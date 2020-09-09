import shutil
import unittest
from datetime import timedelta, datetime
from io import StringIO
from multiprocessing import Pool
from os import cpu_count

import coverage
import ray
from electionguard.elgamal import ElGamalKeyPair
from electionguard.group import rand_q
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import settings, given, HealthCheck, Phase
from hypothesis.strategies import booleans

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.publish import write_fast_tally, write_ray_tally
from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.ray_tally import ray_tally_everything
from arlo_e2e.tally import fast_tally_everything
from arlo_e2e_testing.dominion_hypothesis import dominion_cvrs


class TestRayTallies(unittest.TestCase):
    def removeTree(self) -> None:
        try:
            shutil.rmtree("ftally_output", ignore_errors=True)
            shutil.rmtree("rtally_output", ignore_errors=True)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def setUp(self) -> None:
        cpus = cpu_count()
        ray_init_localhost(num_cpus=cpus)
        self.pool = Pool(cpus)
        self.removeTree()
        coverage.process_startup()  # necessary for coverage testing to work in parallel

    def tearDown(self) -> None:
        ray.shutdown()
        self.pool.close()
        self.removeTree()

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
        self.removeTree()

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        if use_keypair:
            rtally = ray_tally_everything(
                cvrs,
                verbose=True,
                secret_key=keypair.secret_key,
                root_dir="rtally_output",
            )
        else:
            rtally = ray_tally_everything(cvrs, verbose=True, root_dir="rtally_output")

        ftally = rtally.to_fast_tally()
        self.assertTrue(ftally.all_proofs_valid(verbose=False))

        # now, we'll write everything to the filesystem and make sure we get the
        # same stuff

        fmanifest = write_fast_tally(ftally, "ftally_output")
        rmanifest = write_ray_tally(rtally, "rtally_output")

        # we can't just assert equality of the manifests, because the root_dirs are different
        self.assertEqual(fmanifest.hashes, rmanifest.hashes)
        self.removeTree()

    @given(dominion_cvrs(max_rows=5), elgamal_keypairs())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_ray_and_multiprocessing_agree(
        self, input: str, keypair: ElGamalKeyPair
    ) -> None:
        self.removeTree()
        # Normally these are generated internally, but by making them be the same, we take all
        # the non-determinism out of the tally_everything methods and get identical results.
        seed_hash = rand_q()
        master_nonce = rand_q()
        date = datetime.now()

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        tally = fast_tally_everything(
            cvrs,
            verbose=False,
            date=date,
            secret_key=keypair.secret_key,
            pool=self.pool,
            seed_hash=seed_hash,
            master_nonce=master_nonce,
        )
        rtally = ray_tally_everything(
            cvrs,
            verbose=False,
            date=date,
            secret_key=keypair.secret_key,
            seed_hash=seed_hash,
            master_nonce=master_nonce,
            root_dir="rtally_output",
        )

        self.assertEqual(tally, rtally.to_fast_tally())
