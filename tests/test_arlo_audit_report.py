import unittest
from io import StringIO, open

from arlo_e2e.arlo_audit_report import (
    arlo_audit_report_to_sampled_ballots,
    fix_excel_thinks_its_a_date,
)


class TestArloAuditReport(unittest.TestCase):
    inyo_filename = "sample-data/audit-report-Inyo-County-2020.csv"
    inyo_data = ""  # initialized by setUp

    def setUp(self) -> None:
        # Note: IntelliJ likes to start the tests running with the current directory equal
        # to the "tests" directory, while CircleCI and others run the tests from the root
        # directory of the project. This code tries to run correctly in either circumstance.
        try:
            with open(self.inyo_filename, "r") as f:
                self.inyo_data = f.read()
        except FileNotFoundError:
            self.inyo_filename = f"../{self.inyo_filename}"
            with open(self.inyo_filename, "r") as f:
                self.inyo_data = f.read()

    def test_parser_with_bogus_input(self) -> None:
        self.assertIsNone(arlo_audit_report_to_sampled_ballots("no-such-file.txt"))
        self.assertIsNone(
            arlo_audit_report_to_sampled_ballots(StringIO("Hello, World"))
        )

        # now, we'll use our Inyo dataset, but corrupt it a bit
        self.assertGreater(len(self.inyo_data), 0)

        inyo_lines = self.inyo_data.splitlines()

        header_line_no = -1
        for i in range(0, len(inyo_lines)):
            if inyo_lines[i].startswith("######## SAMPLED BALLOTS ########"):
                header_line_no = i
                break

        data_without_sample_ballots = self.inyo_data.splitlines()[:header_line_no]
        self.assertIsNone(
            arlo_audit_report_to_sampled_ballots(
                StringIO("\n".join(data_without_sample_ballots) + "\n")
            )
        )

        # just enough to get the SAMPLED BALLOTS header but no actual data
        data_without_sample_ballots = self.inyo_data.splitlines()[: header_line_no + 1]
        self.assertEqual(
            [],
            arlo_audit_report_to_sampled_ballots(
                StringIO("\n".join(data_without_sample_ballots) + "\n")
            ),
        )

    def test_parser_with_real_input(self) -> None:
        result = arlo_audit_report_to_sampled_ballots(StringIO(self.inyo_data))
        self.assertIsNotNone(result)

        # do it again with the filename, should have identical result
        self.assertEqual(
            result, arlo_audit_report_to_sampled_ballots(self.inyo_filename)
        )

        self.assertEqual(366, len(result))
        imprint_ids = {r.imprintedId for r in result}
        self.assertEqual(366, len(imprint_ids))  # checks for uniqueness

        # Since there are no discrepancies in this sample data, we're going to assert
        # that all the audit and CVR results are equal. That's not how the files are
        # actually written, but it's a property of our parser's normalization of the
        # way CONTEST_NOT_ON_BALOT and BLANK are handled.

        for r in result:
            for k in r.audit_result.keys():
                self.assertEqual(
                    r.audit_result[k],
                    r.cvr_result[k],
                    f"equality of audit and CVR for {k} in {r}",
                )

    def test_excel_thinks_its_a_date(self) -> None:
        self.assertEqual("5-3-3", fix_excel_thinks_its_a_date("5-3-2003"))
        self.assertEqual("5-3-115", fix_excel_thinks_its_a_date("5-3-0115"))
        self.assertEqual("2-24-111", fix_excel_thinks_its_a_date("2-24-0111"))
        self.assertEqual("2-24-46", fix_excel_thinks_its_a_date("2-24-46"))
        self.assertEqual("2-24-46", fix_excel_thinks_its_a_date("2-24-1946"))
        self.assertEqual("cheeseburger", fix_excel_thinks_its_a_date("cheeseburger"))
