import unittest
from io import StringIO
from typing import Optional

import pandas as pd
from hypothesis import given

from dominion import _fix_strings, _row_to_uid, read_dominion_csv, DominionCSV
from tests.dominion_hypothesis import dominion_cvrs

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
            election_description, ballots = result.to_election_description()
            self.assertEqual(2, len(election_description.ballot_styles))
            self.assertEqual(2, len(election_description.contests))
            self.assertEqual(4, len(election_description.candidates))
            self.assertEqual(2, len(ballots))

    def test_read_dominion_csv_failures(self) -> None:
        self.assertIsNone(read_dominion_csv("no-such-file.csv"))
        self.assertIsNone(read_dominion_csv(StringIO('{ "json": 0 }')))

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
                {
                    "Representative - District X (Vote For=1) | Alice | DEM",
                    "Representative - District X (Vote For=1) | Bob | REP",
                },
                result.style_map["T1"],
            )

            self.assertSetEqual(
                {"Referendum | Against", "Referendum | For"}, result.style_map["T2"]
            )


class TestDominionHypotheses(unittest.TestCase):
    @given(dominion_cvrs())
    def test_sanity(self, cvrs):
        parsed = read_dominion_csv(StringIO(cvrs))
        if parsed is None:
            print("Input that failed: \n" + cvrs + "\n")
            again = read_dominion_csv(StringIO(cvrs))
        self.assertIsNotNone(parsed)
