import unittest

import pandas as pd
from dominion import fix_strings, row_to_uid, read_dominion_csv
from io import StringIO


class TestDominion(unittest.TestCase):
    def test_fix_strings(self):
        self.assertEqual(None, fix_strings(""))
        self.assertEqual(None, fix_strings('""'))
        self.assertEqual(0, fix_strings("0"))
        self.assertEqual(0, fix_strings(float("nan")))
        self.assertEqual(0, fix_strings('"0"'))
        self.assertEqual(1, fix_strings('="1"'))
        self.assertEqual(0, fix_strings(0))
        self.assertEqual(0, fix_strings(0.0001))
        self.assertEqual(0.2, fix_strings(0.2))
        self.assertEqual("Hello", fix_strings("Hello"))

    def test_row_to_uid(self):
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
            row_to_uid(series, "Testing", metadata_fields),
        )

    def test_read_dominion_csv(self):
        input_str = """
"2018 Test Election","5.2.16.1","","","","","","","","","",""
"","","","","","","","","Representative - District X (Vote For=1)","Representative - District X (Vote For=1)","Referendum","Referendum"
"","","","","","","","","Alice","Bob","For","Against"
"CvrNumber","TabulatorNum","BatchId","RecordId","ImprintedId","CountingGroup","PrecinctPortion","BallotType","DEM","REP","",""
="1",="1",="1",="1",="1-1-1","Mail","12345 - STR5 (12345 - STR5)","STR5","1","0","0","0"
="2",="1",="1",="3",="1-1-3","Mail","12345 - STSF (12345 - STSF)","STSF","0","1","0","0"
        """
        result = read_dominion_csv(StringIO(input_str))
        self.assertNotEqual(result, None)

        election_name, ballot_types, style_map, contest_map, df = result
        self.assertEqual("2018 Test Election", election_name)
        self.assertEqual(2, len(contest_map.keys()))
        self.assertIn("Representative - District X (Vote For=1)", contest_map)
        self.assertIn("Referendum", contest_map)
        rep_map = contest_map["Representative - District X (Vote For=1)"]
        self.assertIn("Alice | DEM", rep_map)
        self.assertIn("Bob | REP", rep_map)
        self.assertEqual(
            "Representative - District X (Vote For=1) | Alice | DEM",
            rep_map["Alice | DEM"],
        )
        self.assertIn("Representative - District X (Vote For=1) | Alice | DEM", df)
        self.assertIn("Representative - District X (Vote For=1) | Bob | REP", df)

        rows = list(df.iterrows())
        self.assertEqual(2, len(rows))

        x = rows[0][1]  # each row is a tuple, the second part is the Series
        self.assertTrue(isinstance(x, pd.Series))
        self.assertEqual(1, x["CvrNumber"])
        self.assertEqual("1-1-1", x["ImprintedId"])
        self.assertEqual(
            "2018 Test Election | 1 | 1 | 1 | 1 | 1-1-1 | Mail | 12345 - STR5 (12345 - STR5) | STR5",
            x["UID"],
        )

    def test_read_dominion_csv_failures(self):
        self.assertIsNone(read_dominion_csv("no-such-file.csv"))
        self.assertIsNone(read_dominion_csv(StringIO('{ "json": 0 }')))

    def test_read_with_holes(self):
        input_str = """
2018 Test County General Election,5.2.16.1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,,,,,Race 1,Race 1,Race 2,Race 2,Race 2,Race 3,Race 3
,,,,,,,Alice,Bob,Charlie,Dorothy,Eve,Frank,Gary
CvrNumber,TabulatorNum,BatchId,RecordId,ImprintedId,PrecinctPortion,BallotType,PURPLE,GREEN,PURPLE,GREEN,ORANGE,PURPLE,GREEN
1,1,1,1,1-1-1,3065904004 - CNTY (3065904004 - CNTY),CNTY,0,0,0,0,0,,
2,1,1,2,1-1-2,3065904003 (3065904003),50JT,1,0,0,0,1,,
3,1,1,3,1-1-3,3065904002 (3065904002),50JT,0,1,0,1,0,,
4,1,1,4,1-1-4,3065904001 (3065904001),50JT,1,0,1,0,0,,
5,1,1,5,1-1-6,XYZZY,FPD,,,,,,1,0
6,1,2,1,1-2-1,XYZZY,FPD,,,,,,0,1
        """