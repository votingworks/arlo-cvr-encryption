import unittest
from datetime import timedelta
from io import StringIO
from multiprocessing import Pool, cpu_count

import coverage
from electionguard.ballot import (
    PlaintextBallotContest,
    PlaintextBallotSelection,
    PlaintextBallot,
)
from electionguard.elgamal import ElGamalKeyPair
from electionguardtest.election import (
    elections_and_ballots,
    ELECTIONS_AND_BALLOTS_TUPLE_TYPE,
)
from electionguardtest.elgamal import elgamal_keypairs
from hypothesis import settings, given, HealthCheck, Phase
from hypothesis.strategies import booleans

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.tally import (
    fast_tally_everything,
    _interpret_ballot,
    _is_overvoted_ballot,
)
from arlo_e2e_testing.dominion_hypothesis import dominion_cvrs


def _overvote_contest(c: PlaintextBallotContest) -> PlaintextBallotContest:
    # We'll jam every selection to True!
    return PlaintextBallotContest(
        object_id=c.object_id,
        ballot_selections=[
            PlaintextBallotSelection(
                object_id=s.object_id,
                vote="True",
                is_placeholder_selection=s.is_placeholder_selection,
                extended_data=s.extended_data,
            )
            for s in c.ballot_selections
        ],
    )


def _overvote_ballot(b: PlaintextBallot) -> PlaintextBallot:
    return PlaintextBallot(
        object_id=b.object_id,
        ballot_style=b.ballot_style,
        contests=[_overvote_contest(c) for c in b.contests],
    )


class TestBallotInterpretation(unittest.TestCase):
    @given(elections_and_ballots(num_ballots=1))
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_interpretation(self, e_and_b: ELECTIONS_AND_BALLOTS_TUPLE_TYPE) -> None:
        ed, ied, ballots, secret_key, cec = e_and_b

        good_ballot = ballots[0]
        igood_ballot = _interpret_ballot(ied, good_ballot)
        self.assertEqual(good_ballot, igood_ballot)

        bad_ballot = _overvote_ballot(good_ballot)
        if not _is_overvoted_ballot(ied, bad_ballot):
            # degenerate case, nothing changed
            return

        ibad_ballot = _interpret_ballot(ied, bad_ballot)
        self.assertNotEqual(bad_ballot, ibad_ballot)


class TestFastTallies(unittest.TestCase):
    def setUp(self) -> None:
        self.pool = Pool(cpu_count())

    def tearDown(self) -> None:
        self.pool.close()

    @given(dominion_cvrs(max_rows=50), elgamal_keypairs(), booleans())
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

        if use_keypair:
            tally = fast_tally_everything(
                cvrs, self.pool, verbose=True, secret_key=keypair.secret_key
            )
        else:
            tally = fast_tally_everything(cvrs, self.pool, verbose=True)

        self.assertTrue(tally.all_proofs_valid(verbose=True))

        # Now, while we've got a tally and a set of cvrs, we'll test some of the other utility
        # methods that we have. This is going to be much faster than regenerating cvrs and tallies.

        # TODO: tests for get_contest_titles_matching and get_ballot_styles_for_contest_titles

        for ballot_style in cvrs.metadata.style_map.keys():
            ballots_query = tally.get_ballots_matching_ballot_styles([ballot_style])
            ballots_pandas = cvrs.data[cvrs.data.BallotType == ballot_style]

            self.assertEqual(len(ballots_pandas), len(ballots_query))
