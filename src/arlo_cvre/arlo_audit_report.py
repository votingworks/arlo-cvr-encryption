import csv
import re
from dataclasses import dataclass
from io import StringIO
from typing import Union, Optional, List, Dict

import pandas as pd

from arlo_cvre.dominion import fix_strings
from arlo_cvre.eg_helpers import log_and_print

# The data format of an Arlo "audit report" is multiple CSV files, concatenated together,
# with these separators delineating the sections:
#
# ######## ELECTION INFO ########
# ######## CONTESTS ########
# ######## AUDIT SETTINGS ########
# ######## AUDIT BOARDS ########
# ######## ROUNDS ########
# ######## SAMPLED BALLOTS ########

# For arlo-e2e, we're only interested in the SAMPLED BALLOTS section. It then had the following
# headers (here: examples from Inyo County, CA in 2020):
# https://docs.google.com/spreadsheets/d/13y8ZKzq1ZtYdZifqZ2RdHbU-8k8n2qFJsaeC4IW5NxQ/edit#gid=121212966

# Jurisdiction Name
# Container
# Tabulator
# Batch Name
# Ballot Position
# Imprinted ID
# Ticket Numbers: Governing Board Member Big Pine Unified School
# Ticket Numbers: Governing Board Member Lone Pine Unified School
# Ticket Numbers: County Supervisor, 4th District
# Ticket Numbers: Bishop City Treasurer
# Ticket Numbers: Big Pine Fire Protection District
# Ticket Numbers: Director, Zone 1 Northern Inyo Healthcare District
# Ticket Numbers: Measure P - The Bishop Community Safety And Essential Services Measure
# Audited?
# Audit Result: Governing Board Member Big Pine Unified School
# CVR Result: Governing Board Member Big Pine Unified School
# Discrepancy: Governing Board Member Big Pine Unified School
# Audit Result: Governing Board Member Lone Pine Unified School
# CVR Result: Governing Board Member Lone Pine Unified School
# Discrepancy: Governing Board Member Lone Pine Unified School
# Audit Result: County Supervisor, 4th District
# CVR Result: County Supervisor, 4th District
# Discrepancy: County Supervisor, 4th District
# Audit Result: Bishop City Treasurer
# CVR Result: Bishop City Treasurer
# Discrepancy: Bishop City Treasurer
# Audit Result: Big Pine Fire Protection District
# CVR Result: Big Pine Fire Protection District
# Discrepancy: Big Pine Fire Protection District
# Audit Result: Director, Zone 1 Northern Inyo Healthcare District
# CVR Result: Director, Zone 1 Northern Inyo Healthcare District
# Discrepancy: Director, Zone 1 Northern Inyo Healthcare District
# Audit Result: Measure P - The Bishop Community Safety And Essential Services Measure
# CVR Result: Measure P - The Bishop Community Safety And Essential Services Measure
# Discrepancy: Measure P - The Bishop Community Safety And Essential Services Measure

# The "Imprinted ID" field is how we're going to connect this data to everything else
# in arlo-e2e. That field is our ballot UID.

# The Audited? field should be "AUDITED" otherwise we'll ignore it and move on.

# The "Audit Result" and "CVR Result" will have the name of the candidate who won the contest,
# and it's at least plausible that they could be different. Other values that might occur:

# - CONTEST_NOT_ON_BALLOT (in the "Audit" field, blank elsewhere)
# - BLANK (in the "Audit" field, blank elsewhere)
# - YES or NO (for ballot measures)
# - comma-separated candidate names for K-of-N contests (in no apparent ordering)

# The "Discrepancy" fields are blank unless there was an actual discrepancy. So, if there
# is *not* a discrepancy, we would expect the CVR and Audit results to be identical and we
# can just use the CVR results. And if there is a discrepancy, then what kinda really matters
# here is that the CVR matches up with the decrypted ciphertext, so that's what we'll focus
# on. (In other words, we only care about the "CVR Result" fields and ignore the rest.)

# Looks like quotation marks appear sometimes, like the k-of-n case.

# Alternative code that can also parse this file:
# https://github.com/umbernhard/arlo-verifier/blob/master/verify_report.py

_cvr_result = "CVR Result: "
_audit_result = "Audit Result: "
_discrepancy = "Discrepancy: "

# Dominion CSV files and Arlo CSV files use different spellings of the same thing
_audit_iid_str = "Imprinted ID"
_dominion_iid_str = "ImprintedId"


def fix_contest_name(input: str) -> str:
    """
    Removes any of the common prefix strings (e.g., "CVR Result: ") as well as
    removes any "Vote for" suffix on the name of the contest; we have the necessary
    metadata from the arlo-e2e metadata on disk to know about k-of-n contests, and
    we'd prefer for the contest names to match up.
    """

    if input.startswith(_cvr_result):
        input = input[len(_cvr_result) :]
    elif input.startswith(_audit_result):
        input = input[len(_audit_result) :]
    elif input.startswith(_discrepancy):
        input = input[len(_discrepancy) :]

    return re.sub(" Vote for.*$", "", input)


def fix_excel_thinks_its_a_date(input: str) -> str:
    """
    Sometimes raw imprint-ids like "5-3-3" get munged by Excel into "5-3-2003".
    There are memes about this, and it happens here as well. This function tries
    to convert these monstrosities back to their proper imprint-ids.
    """
    m = re.match("(\d+-\d+)-(\d+)", input)
    if not m:
        return input

    preamble, year = m[1], m[2]
    if len(year) != 4:
        return input

    if year.startswith("0"):
        return f"{preamble}-{int(year)}"
    elif year.startswith("19"):
        return f"{preamble}-{int(year) - 1900}"
    elif year.startswith("2"):
        return f"{preamble}-{int(year) - 2000}"
    else:
        return input


@dataclass(eq=True, unsafe_hash=True)
class ArloSampledBallot:
    imprintedId: str
    """String with the unique ballot id, suitable for identifying the same ballot's data elsewhere."""

    metadata: Dict[str, Optional[Union[str, int]]]
    """All ballot metadata fields (all the columns, like jurisdiction name), and also including ticket numbers."""

    audit_result: Dict[str, Optional[str]]
    """Mapping from contest name to the result from the audit."""

    cvr_result: Dict[str, Optional[str]]
    """Mapping from contest name to the result from the CVR."""

    discrepancy: Dict[str, Optional[str]]
    """Mapping from contest name to any discrepancy found."""

    def __init__(self, row: Dict[str, str]):
        global _cvr_result
        global _audit_result
        global _discrepancy

        # We're borrowing fix_strings() from our Dominion parser, since it does all the
        # same things for us that we need, such as converting Pandas's empty cells from
        # floating-point NaN to a more happy Pythonic None.

        self.imprintedId = fix_excel_thinks_its_a_date(fix_strings(row[_audit_iid_str]))
        metadata_keys = [
            k
            for k in row.keys()
            if (not k.startswith(_cvr_result))
            and (not k.startswith(_discrepancy))
            and (not k.startswith(_audit_result))
        ]
        self.metadata = {
            fix_contest_name(k): fix_strings(row[k]) for k in metadata_keys
        }

        # We're removing the prefix from the fields for CVR Result, Audit Result
        # and Discrepancy, when we're putting them into the dictionaries. Makes contest
        # strings line up more neatly with other Dominion CVR data.

        audit_keys = [k for k in row.keys() if k.startswith(_audit_result)]
        self.audit_result = {
            fix_contest_name(k): fix_strings(row[k]) for k in audit_keys
        }

        cvr_keys = [k for k in row.keys() if k.startswith(_cvr_result)]
        self.cvr_result = {fix_contest_name(k): fix_strings(row[k]) for k in cvr_keys}

        discrepancy_keys = [k for k in row.keys() if k.startswith(_discrepancy)]
        self.discrepancy = {
            fix_contest_name(k): fix_strings(row[k]) for k in discrepancy_keys
        }

        # Now we're going to deal with CONTEST_NOT_ON_BALLOT and BLANK, which
        # appear in the Audit columns, but the CVR column will just be an empty
        # string (which will be None by the time we've gotten here). We're going
        # to report out BLANK as-is, for both columns, and we're going to report
        # out None for CONTEST_NOT_ON_BALLOT for both columns.

        for k in self.audit_result.keys():
            if self.audit_result[k] == "BLANK" and self.cvr_result[k] is None:
                self.cvr_result[k] = "BLANK"
            elif (
                self.audit_result[k] == "CONTEST_NOT_ON_BALLOT"
                and self.cvr_result[k] is None
            ):
                self.audit_result[k] = None


def arlo_audit_report_to_sampled_ballots(
    file: Union[str, StringIO]
) -> Optional[List[ArloSampledBallot]]:
    """
    Given a filename of an Arlo Audit CSV (or a StringIO buffer with the same data), tries
    to read it. If successful, you get back a list of `ArloSampledBallot`. Other
    parts of the audit data are ignored.
    """
    if isinstance(file, str):
        try:
            f = open(file, "r")
            lines = f.read().splitlines()
            f.close()
        except FileNotFoundError:
            return None
    elif isinstance(file, StringIO):
        lines = file.read().splitlines()
    else:
        raise ValueError(f"unexpected type for file: {type(file)}")

    header_line_no = -1
    for i in range(0, len(lines)):
        if lines[i].startswith("######## SAMPLED BALLOTS ########"):
            header_line_no = i
            break
    if header_line_no == len(lines) - 1:
        # we found the magic header as the very last line, so there's nothing to
        # parse, but we ostensibly have an empty list of successfully parsed ballots.
        return []
    if header_line_no == -1:
        # we hit EOF and didn't find the line at all
        return None

    data = StringIO("\n".join(lines[header_line_no + 1 :]) + "\n")
    try:
        df = pd.read_csv(
            data, header=[0], quoting=csv.QUOTE_MINIMAL, sep=",", engine="python"
        )
    except FileNotFoundError:
        return None
    except pd.errors.ParserError:
        return None

    if "Imprinted ID" not in df:
        log_and_print(
            f"No `Imprinted ID` field in Arlo audit report. Fields = [{','.join(df.keys())}"
        )
        return None

    rows: List[Dict[str, str]] = df.to_dict(orient="records")
    return [ArloSampledBallot(row) for row in rows]
