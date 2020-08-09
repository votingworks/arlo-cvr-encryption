import shutil
import unittest
from datetime import timedelta
from io import StringIO
from multiprocessing import Pool, cpu_count

import coverage
from hypothesis import settings, given, HealthCheck, Phase
from hypothesis.strategies import booleans

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.publish import write_fast_tally, load_fast_tally
from arlo_e2e.tally import fast_tally_everything, _log_and_print
from arlo_e2e_testing.dominion_hypothesis import dominion_cvrs

TALLY_TESTING_DIR = "tally_testing"

class TestTallyPublishing(unittest.TestCase):
    def _removeTree(self) -> None:
        try:
            shutil.rmtree(TALLY_TESTING_DIR)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def setUp(self) -> None:
        self.pool = Pool(cpu_count())

    def tearDown(self) -> None:
        self._removeTree()
        self.pool.close()

    @given(dominion_cvrs(max_rows=50), booleans())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_end_to_end_publications(self, input: str, check_proofs: bool) -> None:
        # nuke any pre-existing tally_testing tree, since we only want to see current output

        self._removeTree()

        coverage.process_startup()  # necessary for coverage testing to work in parallel

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        results = fast_tally_everything(cvrs, self.pool, verbose=True)

        self.assertTrue(results.all_proofs_valid(self.pool))

        # dump files out to disk
        write_fast_tally(results, TALLY_TESTING_DIR)
        _log_and_print("tally_testing written, proceeding to read it back in again")

        # now, read it back again!
        results2 = load_fast_tally(
            TALLY_TESTING_DIR, check_proofs=check_proofs, pool=self.pool, verbose=True
        )
        self.assertIsNotNone(results2)

        _log_and_print("tally_testing got non-null result!")

        self.assertEqual(
            len(results.encrypted_ballots), len(results2.encrypted_ballots)
        )
        self.assertEqual(set(results.tally.map.keys()), set(results2.tally.map.keys()))

        self._removeTree()

