import shutil
import unittest
from datetime import timedelta, datetime
from io import StringIO
from multiprocessing import Pool
from os import cpu_count
from typing import List

import coverage
import ray
from electionguard.elgamal import ElGamalKeyPair
from electionguard.group import rand_q, int_to_q_unchecked, ElementModP
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import settings, given, HealthCheck, Phase
from hypothesis.strategies import booleans, lists, integers

from arlo_cvre.dominion import read_dominion_csv
from arlo_cvre.io import make_file_ref
from arlo_cvre.manifest import load_existing_manifest
from arlo_cvre.ray_helpers import ray_init_localhost
from arlo_cvre.ray_tally import ray_tally_everything
from arlo_cvre.tally import fast_tally_everything
from arlo_cvre_testing.dominion_hypothesis import dominion_cvrs

RTALLY_OUTPUT = "rtally_output"
FTALLY_OUTPUT = "ftally_output"


class TestRayTallies(unittest.TestCase):
    def removeTree(self) -> None:
        try:
            shutil.rmtree(RTALLY_OUTPUT, ignore_errors=True)
            shutil.rmtree(FTALLY_OUTPUT, ignore_errors=True)
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
        self.pool.close()
        ray.shutdown()
        self.removeTree()

    @given(dominion_cvrs(max_rows=120), elgamal_keypairs(), booleans())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=10,
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

        print(f"End-to-end Ray test with {len(ballots)} ballot(s).")
        if use_keypair:
            rtally = ray_tally_everything(
                cvrs,
                verbose=True,
                secret_key=keypair.secret_key,
                root_dir=RTALLY_OUTPUT,
                use_progressbar=False,
            )
        else:
            rtally = ray_tally_everything(
                cvrs, verbose=True, root_dir=RTALLY_OUTPUT, use_progressbar=False
            )

        self.assertTrue(rtally.all_proofs_valid(verbose=False))

        # While we're here, we'll make sure this works with the fast tally version
        # (which does funky stuff with memos for lazy loading of ballots, and is
        # sufficiently different that it's worth exercising).
        ftally = rtally.to_fast_tally()
        self.assertTrue(ftally.all_proofs_valid(verbose=False))

        # lastly, we'll compare the manifests to make sure everything went out the same
        manifest2 = load_existing_manifest(
            make_file_ref(root_dir=RTALLY_OUTPUT, subdirectories=[], file_name="")
        )
        equiv = rtally.manifest.equivalent(manifest2)
        self.assertTrue(equiv)
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

        print(f"Comparing tallies with {len(ballots)} ballot(s).")

        tally = fast_tally_everything(
            cvrs,
            verbose=False,
            date=date,
            secret_key=keypair.secret_key,
            pool=self.pool,
            seed_hash=seed_hash,
            master_nonce=master_nonce,
            root_dir=FTALLY_OUTPUT,
            use_progressbar=False,
        )
        rtally = ray_tally_everything(
            cvrs,
            verbose=False,
            date=date,
            secret_key=keypair.secret_key,
            seed_hash=seed_hash,
            master_nonce=master_nonce,
            root_dir=RTALLY_OUTPUT,
            use_progressbar=False,
        )

        self.assertEqual(tally, rtally.to_fast_tally())

        # While we're here, we'll check some important cryptographic properties.
        # We want to know that all of the pads (g^R terms) are unique, across every
        # ballot. Otherwise, something's wrong with how we're handling all those
        # nonces / base_hashes / ballot_ids / etc.

        all_pads: List[ElementModP] = []
        for b in tally.encrypted_ballots:
            for c in b.contests:
                for s in c.ballot_selections:
                    all_pads.append(s.ciphertext.pad)

        self.assertEqual(len(all_pads), len(set(all_pads)))

        all_rpads: List[ElementModP] = []
        for b in rtally.encrypted_ballots:
            for c in b.contests:
                for s in c.ballot_selections:
                    all_rpads.append(s.ciphertext.pad)

        self.assertEqual(len(all_rpads), len(set(all_rpads)))

        # And, we're also going to check that the set of nonces is exactly the
        # same for both methods, which indicates that we've got some determinism,
        # as opposed to undesired calls to the random number generator.

        self.assertEqual(set(all_rpads), set(all_pads))

    @given(lists(integers(0, 100), min_size=2, max_size=50))
    def test_elements_work_in_sets_as_expected(self, input: List[int]) -> None:
        # In the above test, we're using len(set(all_pads)) as a way to check
        # that we have no duplicates. This simple test makes sure that this
        # actually works.

        elems = [int_to_q_unchecked(i) for i in input]
        self.assertEqual(len(set(input)), len(set(elems)))
