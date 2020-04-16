import csv
from typing import Optional, Tuple, Any
from math import floor, isnan

import pandas as pd
from pandas.errors import ParserError

# Arlo-e2e support for CVR files from Dominion ballot scanners.

# Each data file looks something like this:
# Line 1: Name of election,unknown field (probably a version number),lots of blank fields
# Line 2: Seven commas,contest title,contest title,... [titles repeat; columns represent a specific candidate or choice]
# Line 3: Seven commas,candidate-or-choice text,... [for yes/no votes, the text here will be "yes" or "no",
#         or sometimes "yes/for" and "no/against"]
# Line 4: Column identifiers (for the first seven columns), then optional party identifiers
#     CvrNumber,TabulatorNum,BatchId,RecordId,ImprintedId,PrecinctPortion,BallotType,...
# Lines 5+: First seven columns correspond to the identifiers, then a sequence of 1/0's

# Notes:
# - Sometimes, there are quotation marks around the entries, other times, not.
# - Sometimes there are equals signs before the quotation marks, so sometimes <<<="3">>> and other times <<<3>>>
# - The number of column identifiers above isn't fixed. Sometimes it's seven, sometimes eight.
#   - Eight entry: CvrNumber,TabulatorNum,BatchId,RecordId,ImprintedId,CountingGroup,PrecinctPortion,BallotType
#   - Seven entry: CvrNumber,TabulatorNum,BatchId,RecordId,ImprintedId,PrecinctPortion,BallotType
#   - The CvrNumber appears to be sequential and always starts with 1.
#   - We can probably treat "CountingGroup" as an empty-string when we don't have it, otherwise make a ballot UID
#     from an 8-tuple of all these strings.
#   - The "CountingGroup" can be "Regular", "Provisional", "Mail", "Election Day", "In Person", or other strings.
#     - We might want special handling for Provisional ballots.
# - A write-in slot is still just voted as 1 or 0. This means tons of extra work for an election official
#   if a write-in actually wins.
# - Undervotes are sometimes indicated as an empty-string as distinct from a 0. We'll just map these to zeros.


def fix_strings(s: Any) -> Any:
    """
    In the case where a string is really a quoted number, this returns the number.
    In the case where it's an empty-string, this returns a zero.
    Otherwise, it's an identity function.
    """
    if isinstance(s, str):
        if s == "":
            return 0
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1]  # strip off the quotation marks
        try:
            return int(s)
        except ValueError:
            return s
    elif isinstance(s, float):
        if isnan(s):
            return 0
        if abs(floor(s) - s) < 0.01:
            return int(floor(s))
    return s


def read_dominion_csv(filename: str) -> Optional[Tuple[str, pd.DataFrame]]:
    """
    Given a filename of a Dominion CSV, tries to read it. If successful, you get
    back a tuple with the name of the election and a Pandas DataFrame with the
    processed contents of the file.
    """
    try:
        df = pd.read_csv(
            filename, header=[0, 1, 2, 3], escapechar="=", quoting=csv.QUOTE_NONE
        )
    except FileNotFoundError:
        return None
    except ParserError:
        return None
    filtered_columns = [
        [e for e in c if (not e.startswith("Unnamed:") and not e == '""')]
        for c in df.columns
    ]
    df = df.applymap(fix_strings)
    election_name = filtered_columns[0][0]
    final_columns = [
        filtered_columns[0][1:],
        filtered_columns[1][1:],
    ] + filtered_columns[2:]
    df.columns = [" | ".join([fix_strings(y) for y in x]) for x in final_columns]

    return fix_strings(election_name), df
