import csv
from io import StringIO
from math import floor, isnan
from typing import Optional, Tuple, Any, Dict, List, Union

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
        if s.startswith("="):
            s = s[1:]  # strip off leading = character
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1]  # strip off the quotation marks
        if s == "":
            return 0
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


CONTEST_MAP = Dict[str, Dict[str, str]]


def row_to_uid(row: pd.Series, election_title: str, fields: List[str]) -> str:
    """
    We want to derive a UID for each row of data. We're doing this with all
    the metadata fields, concatenated together with a vertical bar and spaces
    separating them. We also include the `election_title`, which should hopefully
    make these UIDs *globally* unique.
    """
    sep = " | "
    row_entries = [str(row[x]) for x in fields]
    result = f"{election_title}{sep}{sep.join(row_entries)}"
    return result


def read_dominion_csv(
    file: Union[str, StringIO]
) -> Optional[Tuple[str, CONTEST_MAP, pd.DataFrame]]:
    """
    Given a filename of a Dominion CSV (or a StringIO buffer with the same data), tries
    to read it. If successful, you get back a tuple with the name of the election, a
    "contest map", and a Pandas DataFrame with the processed contents of the file.

    The contest map is a dictionary. The keys are the titles of the contests, and the
    values are a second level of dictionary, mapping from the name of each choice to
    the ultimate string that's used as a column identifier in the Pandas dataframe.

    The Pandas dataframe will have all the columns in the original data plus one new
    one, "UID", which is a concatenation of the various ballot metadata fields along
    with the name of the election. No two ballots should ever have the same UID.
    """
    try:
        df = pd.read_csv(file, header=[0, 1, 2, 3], quoting=csv.QUOTE_NONE)
    except FileNotFoundError:
        return None
    except ParserError:
        return None
    filtered_columns = [
        [fix_strings(e) for e in c if (not e.startswith("Unnamed:") and not e == '""')]
        for c in df.columns
    ]
    election_name = filtered_columns[0][0]

    contests = [x for x in filtered_columns[2:] if len(x) > 1]

    contest_map: CONTEST_MAP = {}
    for contest in contests:
        title = contest[0]
        choice = " | ".join(contest[1:])
        key = " | ".join(contest)

        if title not in contest_map:
            contest_map[title] = {}

        contest_map[title][choice] = key

    # The first two columns have the election name and a version number in them, so we have to treat those specially,
    # otherwise, we're looking for columns with only one thing in them, which says that they're not a contest (with
    # choices) but instead they're one of the metadata columns.
    ballot_id_fields = (
        filtered_columns[0][1:]
        + filtered_columns[1][1:]
        + [x[0] for x in filtered_columns[2:] if len(x) == 1]
    )

    df = df.applymap(fix_strings)
    final_columns = [
        filtered_columns[0][1:],
        filtered_columns[1][1:],
    ] + filtered_columns[2:]

    df.columns = [" | ".join([y for y in x]) for x in final_columns]
    df["UID"] = df.apply(
        lambda row: row_to_uid(row, election_name, ballot_id_fields), axis=1
    )

    return fix_strings(election_name), contest_map, df
