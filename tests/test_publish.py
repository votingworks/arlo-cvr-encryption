import shutil
import unittest
from datetime import timedelta
from io import StringIO
from multiprocessing import Pool, cpu_count

import coverage
from hypothesis import settings, given, HealthCheck, Phase

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.tally import fast_tally_everything
from arlo_e2e.publish import write_fast_tally, load_fast_tally
from arlo_e2e_testing.dominion_hypothesis import dominion_cvrs


class TestTallyPublishing(unittest.TestCase):
    @given(dominion_cvrs())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_end_to_end_publications(self, input: str) -> None:
        coverage.process_startup()  # necessary for coverage testing to work in parallel

        cvrs = read_dominion_csv(StringIO(input))
        self.assertIsNotNone(cvrs)

        _, ballots, _ = cvrs.to_election_description()
        assert len(ballots) > 0, "can't have zero ballots!"

        pool = Pool(cpu_count())
        tally = fast_tally_everything(cvrs, pool, verbose=True)

        # dump files out to disk
        write_fast_tally(tally, "tally_testing")
        print("tally_testing written, proceeding to read it back in again")

        # now, read it back again!
        tally2 = load_fast_tally("tally_testing", check_proofs=True, pool=pool)
        self.assertIsNotNone(tally2)

        self.assertEqual(len(tally.encrypted_ballots), len(tally2.encrypted_ballots))
        self.assertEqual(set(tally.tally.t.keys()), set(tally2.tally.t.keys()))

        pool.close()

        # finally, nuke all the test files we just wrote out
        shutil.rmtree("tally_testing")
