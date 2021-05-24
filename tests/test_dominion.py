import csv
import unittest
from datetime import timedelta
from io import StringIO
from typing import Optional

import pandas as pd
from electionguard.ballot_box import BallotBox
from electionguard.decrypt_with_secrets import decrypt_ballot_with_secret
from electionguard.election import InternalElectionDescription
from electionguard.encrypt import EncryptionDevice
from electionguard.group import ElementModQ
from electionguard.nonces import Nonces
from electionguard.tally import tally_ballots
from electionguardtest.group import elements_mod_q
from hypothesis import given, settings, HealthCheck, Phase
from hypothesis.strategies import integers

from arlo_cvre.dominion import (
    fix_strings,
    dominion_row_to_uid,
    read_dominion_csv,
    DominionCSV,
)
from arlo_cvre.eg_helpers import decrypt_tally_with_secret, UidMaker
from arlo_cvre.metadata import SelectionMetadata
from arlo_cvre.tally import interpret_and_encrypt_ballot
from arlo_cvre_testing.dominion_hypothesis import (
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

_sel_uid_iter = UidMaker("s")  # see read_dominion_csv; we're need the same uid sequence

_uid_int, _uid_str = _sel_uid_iter.next_int()
_expected_alice_metadata = SelectionMetadata(
    _uid_str, _uid_int, "Representative - District X", "Alice", "DEM"
)

_uid_int, _uid_str = _sel_uid_iter.next_int()
_expected_bob_metadata = SelectionMetadata(
    _uid_str, _uid_int, "Representative - District X", "Bob", "REP"
)

_uid_int, _uid_str = _sel_uid_iter.next_int()
_expected_ref_for_metadata = SelectionMetadata(
    _uid_str, _uid_int, "Referendum", "For", ""
)

_uid_int, _uid_str = _sel_uid_iter.next_int()
_expected_ref_against_metadata = SelectionMetadata(
    _uid_str, _uid_int, "Referendum", "Against", ""
)


class TestDominionBasics(unittest.TestCase):
    def test_fix_strings(self) -> None:
        self.assertEqual(None, fix_strings(""))
        self.assertEqual(None, fix_strings('""'))
        self.assertEqual(0, fix_strings("0"))
        self.assertEqual(None, fix_strings(float("nan")))
        self.assertEqual(0, fix_strings('"0"'))
        self.assertEqual(1, fix_strings('="1"'))
        self.assertEqual(0, fix_strings(0))
        self.assertEqual(0, fix_strings(0.0001))
        self.assertEqual(0.2, fix_strings(0.2))
        self.assertEqual("Hello", fix_strings("Hello"))

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
            dominion_row_to_uid(series, "Testing", metadata_fields),
        )

    def test_read_dominion_csv(self) -> None:
        result: Optional[DominionCSV] = read_dominion_csv(StringIO(_good_dominion_cvrs))
        if result is None:
            self.fail("Expected not none")
        else:
            self.assertEqual("2018 Test Election", result.metadata.election_name)
            self.assertEqual(2, len(result.metadata.contest_map.keys()))
            self.assertIn("Representative - District X", result.metadata.contest_map)
            self.assertIn("Referendum", result.metadata.contest_map)
            rep_list = result.metadata.contest_map["Representative - District X"]
            self.assertIsNotNone(rep_list)
            self.assertIn(_expected_alice_metadata, rep_list)
            self.assertIn(_expected_bob_metadata, rep_list)
            self.assertEqual(
                "Representative - District X | Alice | DEM",
                _expected_alice_metadata.to_string(),
            )
            self.assertIn("Representative - District X | Alice | DEM", result.data)
            self.assertIn("Representative - District X | Bob | REP", result.data)
            referendum_list = result.metadata.contest_map["Referendum"]
            self.assertIsNotNone(referendum_list)
            self.assertIn(_expected_ref_for_metadata, referendum_list)
            self.assertIn(_expected_ref_against_metadata, referendum_list)
            self.assertEqual("Referendum | For", _expected_ref_for_metadata.to_string())
            self.assertEqual(
                "Referendum | Against", _expected_ref_against_metadata.to_string()
            )

            self.assertEqual({"REP", "DEM"}, result.metadata.all_parties)

            rows = list(result.data.iterrows())
            self.assertEqual(2, len(rows))

            x = rows[0][1]  # each row is a tuple, the second part is the Series
            self.assertTrue(isinstance(x, pd.Series))
            self.assertEqual(1, x["CvrNumber"])
            self.assertEqual("1-1-1", x["ImprintedId"])
            self.assertEqual(
                "2018 Test Election | 1 | 1 | 1 | 1 | 1-1-1 | Mail | 12345 - STR5 (12345 - STR5) | STR5",
                x["Guid"],
            )

    def test_electionguard_extraction(self) -> None:
        result: Optional[DominionCSV] = read_dominion_csv(StringIO(_good_dominion_cvrs))
        if result is None:
            self.fail("Expected not none")
        else:
            election_description, ballots, _ = result.to_election_description()
            self.assertEqual(2, len(election_description.ballot_styles))
            self.assertEqual(2, len(election_description.contests))
            self.assertEqual(4, len(election_description.candidates))
            self.assertEqual(2, len(ballots))

    def test_read_dominion_csv_weird_data(self) -> None:
        # generated by a hypothesis test case
        weird_data = '"Random Test Election",="5.2.16.1","","","","","","","","","","","","","","","","","","","","","","","","","",""\n"","","","","","","","","Contest1 (Vote For=1)","Contest1 (Vote For=1)","Contest2 (Vote For=1)","Contest2 (Vote For=1)","Contest2 (Vote For=1)","Contest2 (Vote For=1)","Contest2 (Vote For=1)","Contest3 (Vote For=1)","Contest3 (Vote For=1)","Contest4 (Vote For=1)","Contest4 (Vote For=1)","Contest4 (Vote For=1)","Contest4 (Vote For=1)","Contest4 (Vote For=1)","Contest5 (Vote For=1)","Contest5 (Vote For=1)","Referendum1","Referendum1","Referendum2","Referendum2"\n"","","","","","","","","Richard WHITE","Richard WALKER","Camille YOUNG","Patricia GUPTA","Anthony GUPTA","Barbara WALKER","Karen GARCIA","David MILLER","Sigurður TAYLOR","William WILSON","Patricia SMITH","Matthew JONES","Matthew WILLIAMS","Betty KING","Betty MILLER","Dorothy MARTIN","FOR","AGAINST","FOR","AGAINST"\n"CvrNumber","TabulatorNum","BatchId","RecordId","ImprintedId","CountingGroup","PrecinctPortion","BallotType","PARTY5","PARTY1","PARTY2","PARTY3","PARTY1","PARTY4","PARTY2","PARTY1","PARTY2","PARTY5","PARTY3","PARTY2","PARTY5","PARTY2","PARTY3","PARTY1","","","",""\n="1",="1",="2",="0",="1-1-1","Regular",="0","BALLOTSTYLE2",="0",="0",="1",="0",="0",="0",="0",="0",="0",="0",="0",="0",="1",="0","","","","",="1",="0"\n="2",="2",="0",="2",="1-1-2","In Person",="1","BALLOTSTYLE1",="1",="0","","","","","",="0",="1","","","","","","","","","",="1",="0"\n="3",="1",="2",="0",="1-1-3","Mail",="3","BALLOTSTYLE4","","",="0",="0",="1",="0",="0","","","","","","","",="0",="1",="0",="0",="0",="1"\n="4",="2",="0",="2",="1-1-4","Provisional",="3","BALLOTSTYLE0",="0",="0","","","","","",="1",="0",="0",="1",="0",="0",="0","","",="0",="0","",""\n="5",="0",="3",="0",="1-1-5","Mail",="3","BALLOTSTYLE1",="0",="0","","","","","",="1",="0","","","","","","","","","",="0",="1"\n="6",="3",="3",="0",="1-1-6","Election Day",="3","BALLOTSTYLE0",="0",="1","","","","","",="0",="1",="0",="0",="0",="1",="0","","",="0",="0","",""\n="7",="2",="1",="0",="1-1-7","Provisional",="3","BALLOTSTYLE2",="0",="0",="0",="1",="0",="0",="0",="0",="1",="0",="0",="0",="0",="0","","","","",="0",="0"\n="8",="2",="3",="3",="1-1-8","Mail",="2","BALLOTSTYLE3",="0",="0",="0",="0",="1",="0",="0",="0",="0","","","","","",="0",="1",="0",="1","",""\n="9",="0",="0",="0",="1-1-9","Regular",="0","BALLOTSTYLE2",="0",="1",="0",="1",="0",="0",="0",="0",="1",="0",="0",="1",="0",="0","","","","",="0",="1"\n="10",="0",="0",="1",="1-1-10","In Person",="2","BALLOTSTYLE2",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0","","","","",="0",="0"\n="11",="2",="3",="1",="1-1-11","Provisional",="2","BALLOTSTYLE3",="0",="1",="0",="0",="0",="0",="0",="0",="0","","","","","",="1",="0",="0",="1","",""\n="12",="1",="2",="3",="1-1-12","Election Day",="2","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="0",="1",="0",="0",="0",="1"\n="13",="3",="1",="1",="1-1-13","In Person",="1","BALLOTSTYLE3",="0",="1",="0",="0",="0",="0",="0",="0",="1","","","","","",="0",="1",="0",="0","",""\n="14",="2",="1",="1",="1-1-14","In Person",="3","BALLOTSTYLE4","","",="0",="0",="0",="1",="0","","","","","","","",="0",="1",="0",="1",="0",="1"\n="15",="3",="3",="2",="1-1-15","Mail",="3","BALLOTSTYLE2",="0",="0",="0",="0",="0",="0",="0",="0",="1",="0",="0",="0",="0",="0","","","","",="0",="0"\n="16",="2",="3",="3",="1-1-16","Election Day",="3","BALLOTSTYLE4","","",="0",="0",="0",="1",="0","","","","","","","",="0",="0",="1",="0",="1",="0"\n="17",="0",="2",="3",="1-1-17","Election Day",="2","BALLOTSTYLE2",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0","","","","",="0",="0"\n="18",="2",="3",="0",="1-1-18","Election Day",="0","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="0",="1",="0",="0",="1",="0"\n="19",="3",="0",="2",="1-1-19","In Person",="2","BALLOTSTYLE3",="0",="1",="0",="0",="0",="0",="0",="0",="0","","","","","",="1",="0",="1",="0","",""\n="20",="2",="3",="1",="1-1-20","Regular",="3","BALLOTSTYLE2",="0",="1",="0",="0",="0",="0",="1",="1",="0",="0",="0",="1",="0",="0","","","","",="1",="0"\n="21",="0",="3",="1",="1-1-21","Mail",="0","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="0",="1",="0",="0",="0",="1"\n="22",="2",="0",="1",="1-1-22","Regular",="0","BALLOTSTYLE0",="0",="1","","","","","",="1",="0",="0",="0",="0",="0",="0","","",="0",="1","",""\n="23",="1",="3",="0",="1-1-23","In Person",="2","BALLOTSTYLE1",="0",="1","","","","","",="1",="0","","","","","","","","","",="0",="1"\n="24",="2",="1",="1",="1-1-24","Regular",="1","BALLOTSTYLE2",="1",="0",="0",="0",="1",="0",="0",="0",="1",="0",="0",="0",="0",="0","","","","",="0",="0"\n="25",="1",="0",="0",="1-1-25","Provisional",="3","BALLOTSTYLE3",="1",="0",="1",="0",="0",="0",="0",="0",="0","","","","","",="0",="0",="0",="0","",""\n="26",="2",="0",="1",="1-1-26","Provisional",="1","BALLOTSTYLE0",="0",="0","","","","","",="0",="0",="0",="0",="1",="0",="0","","",="1",="0","",""\n="27",="3",="2",="2",="1-1-27","Mail",="0","BALLOTSTYLE4","","",="0",="0",="0",="0",="1","","","","","","","",="1",="0",="0",="1",="0",="0"\n="28",="2",="3",="2",="1-1-28","Regular",="1","BALLOTSTYLE0",="0",="1","","","","","",="0",="1",="0",="0",="0",="0",="0","","",="0",="0","",""\n="29",="0",="1",="2",="1-1-29","Mail",="0","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="0",="0",="0",="1",="0",="0"\n="30",="3",="3",="2",="1-1-30","Provisional",="1","BALLOTSTYLE2",="0",="0",="1",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0","","","","",="0",="0"\n="31",="2",="2",="2",="1-1-31","In Person",="2","BALLOTSTYLE1",="0",="0","","","","","",="0",="0","","","","","","","","","",="0",="0"\n="32",="0",="2",="2",="1-1-32","In Person",="2","BALLOTSTYLE0",="1",="0","","","","","",="1",="0",="0",="1",="0",="0",="0","","",="0",="0","",""\n="33",="2",="1",="0",="1-1-33","Mail",="2","BALLOTSTYLE4","","",="1",="0",="0",="0",="0","","","","","","","",="1",="0",="0",="1",="0",="0"\n="34",="2",="2",="2",="1-1-34","Election Day",="1","BALLOTSTYLE2",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="0",="1","","","","",="0",="1"\n="35",="0",="2",="0",="1-1-35","Provisional",="0","BALLOTSTYLE0",="1",="0","","","","","",="1",="0",="0",="0",="0",="0",="0","","",="0",="0","",""\n="36",="2",="3",="0",="1-1-36","Provisional",="1","BALLOTSTYLE3",="0",="1",="0",="0",="0",="1",="0",="0",="0","","","","","",="0",="1",="0",="0","",""\n="37",="2",="0",="2",="1-1-37","Provisional",="2","BALLOTSTYLE4","","",="0",="0",="0",="0",="0","","","","","","","",="1",="0",="1",="0",="0",="1"\n="38",="0",="2",="3",="1-1-38","Election Day",="3","BALLOTSTYLE2",="1",="0",="0",="1",="0",="0",="0",="1",="0",="0",="0",="0",="0",="0","","","","",="0",="1"\n="39",="0",="3",="2",="1-1-39","In Person",="0","BALLOTSTYLE0",="0",="1","","","","","",="0",="1",="0",="0",="0",="0",="0","","",="1",="0","",""'
        result: Optional[DominionCSV] = read_dominion_csv(StringIO(weird_data))
        self.assertIsNotNone(result)

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
                {"Representative - District X"},
                result.metadata.style_map["T1"],
            )

            self.assertSetEqual({"Referendum"}, result.metadata.style_map["T2"])

    def test_repeating_candidate_names(self) -> None:
        input_str = """
"2018 Test Election","5.2.16.1","","","","","","","","","","","","","",""
"","","","","","","","","District X (Vote For=3)","District X (Vote For=3)","District X (Vote For=3)","District X (Vote For=3)","Referendum 1","Referendum 1","Referendum 2","Referendum 2"
"","","","","","","","","Alice","Write-in","Write-in","Write-in","For","Against","For","Against"
"CvrNumber","TabulatorNum","BatchId","RecordId","ImprintedId","CountingGroup","PrecinctPortion","BallotType","DEM","","","","","","",""
="1",="1",="1",="1",="1-1-1","Mail","Thing1","T1","1","0","0","0","1","0","1","0"
                """
        result: Optional[DominionCSV] = read_dominion_csv(StringIO(input_str))
        if result is None:
            self.fail("Expected not none")
        else:
            self.assertNotEqual(result, None)
            self.assertIsNotNone(result.metadata.contest_map["District X"])
            self.assertIsNotNone(result.metadata.contest_map["Referendum 1"])
            self.assertIsNotNone(result.metadata.contest_map["Referendum 2"])

            # make sure there are no "(2)" things going on in the referenda
            ref1_choice_names = {
                x.choice_name for x in result.metadata.contest_map["Referendum 1"]
            }
            ref2_choice_names = {
                x.choice_name for x in result.metadata.contest_map["Referendum 2"]
            }
            self.assertEqual({"For", "Against"}, ref1_choice_names)
            self.assertEqual({"For", "Against"}, ref2_choice_names)

            # and now make sure we have what we expect for our write-in race
            choice_names = {
                x.choice_name for x in result.metadata.contest_map["District X"]
            }
            self.assertEqual(
                choice_names, {"Alice", "Write-in", "Write-in (2)", "Write-in (3)"}
            )


class TestDominionHypotheses(unittest.TestCase):
    @given(
        integers(1, 3).flatmap(
            lambda i: dominion_cvrs(max_rows=50, max_votes_per_race=i)
        )
    )
    @settings(
        deadline=timedelta(milliseconds=10000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_max_votes_per_race_sanity(self, cvrs: str) -> None:
        parsed = read_dominion_csv(StringIO(cvrs))
        self.assertIsNotNone(parsed)

    @given(ballots_and_context(), elements_mod_q())
    @settings(
        deadline=timedelta(milliseconds=100000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_eg_conversion(
        self, state: DominionBallotsAndContext, seed: ElementModQ
    ) -> None:
        ied = InternalElectionDescription(state.ed)
        ballot_box = BallotBox(ied, state.cec)

        seed_hash = EncryptionDevice("Location").get_hash()
        nonces = Nonces(seed)[0 : len(state.ballots)]

        for b, n in zip(state.ballots, nonces):
            eb = interpret_and_encrypt_ballot(b, ied, state.cec, seed_hash, n)
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
        results = decrypt_tally_with_secret(tally, state.secret_key)

        self.assertEqual(len(results.keys()), len(state.id_map.keys()))
        for obj_id in results.keys():
            self.assertIn(obj_id, state.id_map)
            cvr_sum = int(state.dominion_cvrs.data[state.id_map[obj_id]].sum())
            decryption = results[obj_id]
            self.assertEqual(cvr_sum, decryption)

    @given(dominion_cvrs(max_rows=10))
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=5,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_csv_metadata_roundtrip(self, cvrs: str) -> None:
        parsed = read_dominion_csv(StringIO(cvrs))
        self.assertIsNotNone(parsed)

        original_metadata = parsed.dataframe_without_selections()
        csv_data = original_metadata.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC)
        reloaded_metadata = pd.read_csv(StringIO(csv_data))

        self.assertTrue(original_metadata.equals(reloaded_metadata))
