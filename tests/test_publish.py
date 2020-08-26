import shutil
import unittest
from datetime import timedelta
from io import StringIO
from multiprocessing import Pool, cpu_count

import coverage
from electionguard.ballot import _list_eq
from electionguard.election import InternalElectionDescription
from electionguard.elgamal import ElGamalKeyPair
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import settings, given, HealthCheck, Phase
from hypothesis.strategies import booleans

from arlo_e2e.decrypt import (
    decrypt_ballots,
    verify_proven_ballot_proofs,
    exists_proven_ballot,
    write_proven_ballot,
    load_proven_ballot,
)
from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.publish import write_fast_tally, load_fast_tally
from arlo_e2e.tally import fast_tally_everything
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e_testing.dominion_hypothesis import dominion_cvrs

TALLY_TESTING_DIR = "tally_test"
DECRYPTED_DIR = "decrypted_test"


class TestTallyPublishing(unittest.TestCase):
    def removeTree(self) -> None:
        try:
            shutil.rmtree(TALLY_TESTING_DIR, ignore_errors=True)
            shutil.rmtree(DECRYPTED_DIR, ignore_errors=True)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def setUp(self) -> None:
        self.removeTree()
        self.pool = Pool(cpu_count())

    def tearDown(self) -> None:
        self.removeTree()
        self.pool.close()

    @given(dominion_cvrs(max_rows=50), booleans(), elgamal_keypairs())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_end_to_end_publications(
        self, input: str, check_proofs: bool, keypair: ElGamalKeyPair
    ) -> None:
        coverage.process_startup()  # necessary for coverage testing to work in parallel
        self.removeTree()  # if there's anything leftover from a prior run, get rid of it

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        results = fast_tally_everything(
            cvrs, self.pool, secret_key=keypair.secret_key, verbose=True
        )

        self.assertTrue(results.all_proofs_valid(self.pool))

        # dump files out to disk
        write_fast_tally(results, TALLY_TESTING_DIR)
        log_and_print("tally_testing written, proceeding to read it back in again")

        # now, read it back again!
        results2 = load_fast_tally(
            TALLY_TESTING_DIR,
            check_proofs=check_proofs,
            pool=self.pool,
            verbose=True,
            recheck_ballots_and_tallies=True,
        )
        self.assertIsNotNone(results2)

        log_and_print("tally_testing got non-null result!")

        self.assertTrue(_list_eq(results.encrypted_ballots, results2.encrypted_ballots))
        self.assertTrue(results.equivalent(results2, keypair))

        # And lastly, while we're here, we'll use all this machinery to exercise the ballot decryption
        # read/write facilities.

        ied = InternalElectionDescription(results.election_description)

        log_and_print("decrypting one more time")
        pballots = decrypt_ballots(
            ied,
            results.context.crypto_extended_base_hash,
            keypair,
            self.pool,
            results.encrypted_ballots,
        )
        self.assertEqual(len(pballots), len(results.encrypted_ballots))
        self.assertNotIn(None, pballots)

        # for speed, we're only going to do this for the first ballot, not all of them
        pballot = pballots[0]
        eballot = results.encrypted_ballots[0]
        bid = pballot.ballot.object_id
        self.assertTrue(
            verify_proven_ballot_proofs(
                results.context.crypto_extended_base_hash,
                keypair.public_key,
                eballot,
                pballot,
            )
        )
        write_proven_ballot(pballot, DECRYPTED_DIR)
        self.assertTrue(exists_proven_ballot(bid, DECRYPTED_DIR))
        self.assertFalse(exists_proven_ballot(bid + "0", DECRYPTED_DIR))
        self.assertEqual(pballot, load_proven_ballot(bid, DECRYPTED_DIR))

        self.removeTree()  # clean up our mess
