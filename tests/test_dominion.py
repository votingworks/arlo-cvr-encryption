import unittest
from datetime import timedelta
from io import StringIO
from typing import Optional

import pandas as pd
from electionguard.ballot_box import BallotBox
from electionguard.ballot_store import BallotStore
from electionguard.decrypt_with_secrets import decrypt_ballot_with_secret
from electionguard.election import InternalElectionDescription
from electionguard.encrypt import encrypt_ballot, EncryptionDevice
from electionguard.group import ElementModQ
from electionguard.nonces import Nonces
from electionguard.tally import tally_ballots
from electionguardtest.group import elements_mod_q
from hypothesis import given, settings, HealthCheck, Phase

from dominion import _fix_strings, _row_to_uid, read_dominion_csv, DominionCSV
from eg_helpers import decrypt_with_secret
from tests.dominion_hypothesis import (
    dominion_cvrs,
    ballots_and_context,
    DominionBallotsAndContext,
)

_good_dominion_cvrs = """
"2018 Test Election","5.2.16.1","","","","","","","","","",""
"","","","","","","","","Representative - District X (Vote For=1)","Representative - District X (Vote For=1)","Referendum","Referendum"
"","","","","","","","","Alice","Bob","For","Against"
"CvrNumber","TabulatorNum","BatchId","RecordId","ImprintedId","CountingGroup","PrecinctPortion","BallotType","DEM","REP","",""
="1",="1",="1",="1",="1-1-1","Mail","12345 - STR5 (12345 - STR5)","STR5","1","0","0","0"
="2",="1",="1",="3",="1-1-3","Mail","12345 - STSF (12345 - STSF)","STSF","0","1","0","0"
        """


class TestDominionBasics(unittest.TestCase):
    def test_fix_strings(self) -> None:
        self.assertEqual(None, _fix_strings(""))
        self.assertEqual(None, _fix_strings('""'))
        self.assertEqual(0, _fix_strings("0"))
        self.assertEqual(None, _fix_strings(float("nan")))
        self.assertEqual(0, _fix_strings('"0"'))
        self.assertEqual(1, _fix_strings('="1"'))
        self.assertEqual(0, _fix_strings(0))
        self.assertEqual(0, _fix_strings(0.0001))
        self.assertEqual(0.2, _fix_strings(0.2))
        self.assertEqual("Hello", _fix_strings("Hello"))

    def test_row_to_uid(self) -> None:
        row_dict = {
            "CvrNumber": 1,
            "TabulatorNum": 1,
            "BatchId": 1,
            "RecordId": 1,
            "ImprintedId": "1-1-1",
            "CountingGroup": "Mail",
            "PrecinctPortion": "4016532007 - STR5",
            "BallotType": "STR5",
            "Race 1 | DEM": 1,
            "Race 1 | REP": 0,
            "Race 2 | DEM": 0,
            "Race 2 | REP": 1,
            "Race 3 | DEM": 1,
            "Race 3 | REP": 1,
        }
        metadata_fields = [
            "CvrNumber",
            "TabulatorNum",
            "BatchId",
            "RecordId",
            "ImprintedId",
            "CountingGroup",
            "PrecinctPortion",
            "BallotType",
        ]
        series = pd.Series(row_dict)
        self.assertEqual(
            "Testing | 1 | 1 | 1 | 1 | 1-1-1 | Mail | 4016532007 - STR5 | STR5",
            _row_to_uid(series, "Testing", metadata_fields),
        )

    def test_read_dominion_csv(self) -> None:
        result: Optional[DominionCSV] = read_dominion_csv(StringIO(_good_dominion_cvrs))
        if result is None:
            self.fail("Expected not none")
        else:
            self.assertEqual("2018 Test Election", result.election_name)
            self.assertEqual(2, len(result.contest_map.keys()))
            self.assertIn(
                "Representative - District X (Vote For=1)", result.contest_map
            )
            self.assertIn("Referendum", result.contest_map)
            rep_map = result.contest_map["Representative - District X (Vote For=1)"]
            self.assertIsNotNone(rep_map)
            self.assertIn(("Alice", "DEM"), rep_map)
            self.assertIn(("Bob", "REP"), rep_map)
            self.assertEqual(
                "Representative - District X (Vote For=1) | Alice | DEM",
                rep_map[("Alice", "DEM")],
            )
            self.assertIn(
                "Representative - District X (Vote For=1) | Alice | DEM", result.data
            )
            self.assertIn(
                "Representative - District X (Vote For=1) | Bob | REP", result.data
            )
            referendum_map = result.contest_map["Referendum"]
            self.assertIsNotNone(referendum_map)
            self.assertIn(("For", ""), referendum_map)
            self.assertIn(("Against", ""), referendum_map)
            self.assertEqual("Referendum | For", referendum_map[("For", "")])
            self.assertEqual("Referendum | Against", referendum_map[("Against", "")])

            self.assertEqual({"REP", "DEM"}, result.all_parties)

            rows = list(result.data.iterrows())
            self.assertEqual(2, len(rows))

            x = rows[0][1]  # each row is a tuple, the second part is the Series
            self.assertTrue(isinstance(x, pd.Series))
            self.assertEqual(1, x["CvrNumber"])
            self.assertEqual("1-1-1", x["ImprintedId"])
            self.assertEqual(
                "2018 Test Election | 1 | 1 | 1 | 1 | 1-1-1 | Mail | 12345 - STR5 (12345 - STR5) | STR5",
                x["UID"],
            )

    def test_electionguard_extraction(self) -> None:
        result: Optional[DominionCSV] = read_dominion_csv(StringIO(_good_dominion_cvrs))
        if result is None:
            self.fail("Expected not none")
        else:
            election_description, ballots, id_map = result.to_election_description()
            self.assertEqual(2, len(election_description.ballot_styles))
            self.assertEqual(2, len(election_description.contests))
            self.assertEqual(4, len(election_description.candidates))
            self.assertEqual(2, len(ballots))

    def test_read_dominion_csv_failures(self) -> None:
        self.assertIsNone(read_dominion_csv("no-such-file.csv"))
        # TODO: more tests of "malformed" CSV data

    def test_read_with_holes(self) -> None:
        input_str = """
"2018 Test Election","5.2.16.1","","","","","","","","","",""
"","","","","","","","","Representative - District X (Vote For=1)","Representative - District X (Vote For=1)","Referendum","Referendum"
"","","","","","","","","Alice","Bob","For","Against"
"CvrNumber","TabulatorNum","BatchId","RecordId","ImprintedId","CountingGroup","PrecinctPortion","BallotType","DEM","REP","",""
="1",="1",="1",="1",="1-1-1","Mail","Thing1","T1","1","0",,
="2",="1",="1",="3",="1-1-3","Mail","Thing2","T2",,,"0","0"
        """
        result: Optional[DominionCSV] = read_dominion_csv(StringIO(input_str))
        if result is None:
            self.fail("Expected not none")
        else:
            self.assertNotEqual(result, None)

            rows = list(result.data.iterrows())
            self.assertEqual(2, len(rows))

            self.assertSetEqual(
                {"Representative - District X (Vote For=1)"}, result.style_map["T1"],
            )

            self.assertSetEqual({"Referendum"}, result.style_map["T2"])


class TestDominionHypotheses(unittest.TestCase):
    @given(dominion_cvrs())
    @settings(
        deadline=timedelta(milliseconds=10000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_sanity(self, cvrs: str):
        parsed = read_dominion_csv(StringIO(cvrs))
        self.assertIsNotNone(parsed)

    @given(ballots_and_context(), elements_mod_q())
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_eg_conversion(self, state: DominionBallotsAndContext, seed: ElementModQ):
        ied = InternalElectionDescription(state.ed)
        ballot_box = BallotBox(ied, state.cec)

        store = BallotStore()
        seed_hash = EncryptionDevice("Location").get_hash()
        nonces = Nonces(seed)[0 : len(state.ballots)]

        for b, n in zip(state.ballots, nonces):
            eb = encrypt_ballot(b, ied, state.cec, seed_hash, n)
            self.assertIsNotNone(eb)

            pb = decrypt_ballot_with_secret(
                eb,
                ied,
                state.cec.crypto_extended_base_hash,
                state.cec.elgamal_public_key,
                state.secret_key,
            )
            self.assertEqual(b, pb)

            self.assertGreater(len(eb.contests), 0)
            cast_result = ballot_box.cast(eb)
            self.assertIsNotNone(cast_result)

        tally = tally_ballots(ballot_box._store, ied, state.cec)
        self.assertIsNotNone(tally)
        results = decrypt_with_secret(tally, state.secret_key)

        self.assertEqual(len(results.keys()), len(state.id_map.keys()))
        for obj_id in results.keys():
            self.assertIn(obj_id, state.id_map)
            cvr_sum = int(state.dominion_cvrs.data[state.id_map[obj_id]].sum())
            decryption = results[obj_id]
            self.assertEqual(cvr_sum, decryption)
