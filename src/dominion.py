import csv
import datetime
from io import StringIO
from math import floor, isnan
from typing import (
    Optional,
    Any,
    Dict,
    List,
    Union,
    NamedTuple,
    Set,
    Tuple,
    Iterable,
)

import pandas as pd

from electionguard.ballot import PlaintextBallot
from electionguard.election import (
    ElectionDescription,
    ElectionType,
    Party,
    InternationalizedText,
    Language,
    Candidate,
    GeopoliticalUnit,
    ReportingUnitType,
    BallotStyle,
    ContestDescription,
    VoteVariationType,
    SelectionDescription,
)

from utils import flatmap, UidMaker


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
# - BallotType appears to be an identifier of a ballot style, i.e., every row in this sheet having the
#   same BallotType represents a voter who was given the same choices on their ballot.
# Also, just to be annoying, the way that Pandas indicates that it has no value in a numeric cell? NaN.
# Even when we explicitly set it to None, what we get back is NaN.


def _fix_strings(s: Any) -> Any:
    """
    In the case where a string is really a quoted number, this returns the number.
    In the case where it's an empty-string, this returns `None`.
    Otherwise, it's an identity function.
    """
    if isinstance(s, int):
        return s
    elif isinstance(s, float):
        if isnan(s):
            return None
        if abs(floor(s) - s) < 0.01:
            return int(floor(s))
    elif isinstance(s, str):
        if s.startswith("="):
            s = s[1:]  # strip off leading = character
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1]  # strip off the quotation marks
        if s == "":
            return None
        try:
            return int(s)
        except ValueError:
            return s
    return s


def _fix_party_string(s: Any) -> str:
    """
    Specifically converting something from the 4th line of input to a "party" is a little bit
    trickier, because not all races have parties. If we have one, we'll return that as a string,
    otherwise empty-string.
    """
    if s is None or (isinstance(s, float) and isnan(s)):
        return ""
    else:
        return str(s)


def _row_to_uid(row: pd.Series, election_title: str, fields: List[str]) -> str:
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


def _nonempty_elem(row: pd.Series, key: str) -> bool:
    """
    Decides whether the particular key in this row is present or absent.
    """
    val = getattr(row, key)

    # seems that we'll sometimes get None and other times get NaN, so we
    # have to be extra paranoid.
    if isinstance(val, float):
        return not isnan(val)
    elif isinstance(val, str):
        return str != ""
    else:
        return val is not None


def _str_to_internationalized_text_en(input: str) -> InternationalizedText:
    return InternationalizedText([Language(input, language="en")])


CONTEST_MAP = Dict[str, Dict[Tuple[str, str], str]]
STYLE_MAP = Dict[str, Set[str]]


class DominionCSV(NamedTuple):
    """
    This data structure represents everything we parse out of a Dominion CSV file. It's
    produced by `read_dominion_csv`.
    """

    election_name: str
    """
    Name of the election (as provided by the election administrator).
    """

    contests: Set[str]
    """
    List of every contest name (i.e., list of every column name in the data). Not every ballot
    will have every contest.
    """

    ballot_types: Set[str]
    """
    Set of the different "ballot type" names (as provided by the election administrator).
    """

    all_parties: Set[str]
    """
    Set of political parties (typically three-letter codes).
    """

    style_map: STYLE_MAP
    """
    A dictionary mapping ballot-type names to the set of associated contest titles.
    """

    contest_map: CONTEST_MAP
    """
    A nested dictionary. The first key has every contest title (e.g., "Governor"). The
    second key is a tuple with the candidates associated with that contest (e.g., "Alice"
    or "Bob") and the party (e.g., "REP" or "DEM"; empty-string if there is no party).
    The resulting value is the name of the dataframe column having all of the votes for this specific race.
    """

    data: pd.DataFrame
    """
    A Pandas DataFrame, having all the columns in the original data plus one new
    one, "UID", which is a concatenation of the various ballot metadata fields along
    with the name of the election. No two ballots should ever have the same UID.
    """

    def _all_parties_for_contests(self, contests: Iterable[str]) -> Set[str]:
        # The tuple we get back from the contest_map has a contest name (t[0]) and maybe a party
        # (t[1]), which will be "" if there's no party at all, so we want to filter those out.

        parties = [
            t[1] for t in flatmap(lambda c: self.contest_map[c], contests) if t[1] != ""
        ]
        return set(parties)

    def _ballot_style_from_id(
        self,
        gp: GeopoliticalUnit,
        dominion_ballot_style_id: str,
        bs_uids: UidMaker,
        party_map: Dict[str, Party],
    ) -> BallotStyle:

        contest_titles = self.style_map[dominion_ballot_style_id]
        bs_id = bs_uids.next()

        party_ids = [
            party_map[p].object_id
            for p in self._all_parties_for_contests(contest_titles)
        ]

        return BallotStyle(
            object_id=bs_id,
            geopolitical_unit_ids=[gp.object_id],
            party_ids=party_ids if party_ids else None,
        )

    def _contest_name_to_description(
        self,
        name: str,
        candidate_uid_maker: UidMaker,
        contest_uid_maker: UidMaker,
        selection_uid_maker: UidMaker,
        gp: GeopoliticalUnit,
    ) -> Tuple[ContestDescription, List[Candidate]]:

        selections: List[SelectionDescription] = []
        candidates: List[Candidate] = []

        # We're collecting the candidates at the same time that we're collecting the selections
        # to make sure that the object_id's are properly synchronized. Consider the case where
        # we ended up with two candidates with identical names. In the Dominion CSV, we'd have
        # no way to tell if they were actually the same person running in two separate contests,
        # or whether they were distinct. This solution ensures that we create a fresh Candidate
        # one-to-one with every SelectionDescription.

        # This method will be called separately for every contest name, and the resulting lists
        # of candidates should be merged to make the complete list that goes into an
        # ElectionDescription.

        for c in self.contest_map[name]:
            id_number, id_str = selection_uid_maker.next_int()

            candidate = Candidate(
                object_id=candidate_uid_maker.next(),
                ballot_name=_str_to_internationalized_text_en(c[0]),
                party_id=c[1] if c[1] != "" else None,
                image_uri=None,
            )
            candidates.append(candidate)
            selections.append(
                SelectionDescription(
                    object_id=id_str,
                    candidate_id=candidate.object_id,
                    sequence_order=id_number,
                )
            )

        id_number, id_str = contest_uid_maker.next_int()
        return (
            ContestDescription(
                object_id=id_str,
                electoral_district_id=gp.object_id,
                sequence_order=id_number,
                vote_variation=VoteVariationType.one_of_m,  # for now
                number_elected=1,
                votes_allowed=None,
                name=name,
                ballot_selections=selections,
                ballot_title=_str_to_internationalized_text_en(name),
            ),
            candidates,
        )

    def to_election_description(
        self,
    ) -> Tuple[ElectionDescription, List[PlaintextBallot]]:
        """
        Converts this data to a ElectionGuard `ElectionDescription` (having all of the metadata
        describing the election) and a list of `PlaintextBallot` (corresponding to each of the
        rows in the Dominion CVR).
        """

        ballots: List[PlaintextBallot] = list()
        date = datetime.datetime.now()

        party_uids = UidMaker("party")
        party_map = {
            p: Party(
                object_id=party_uids.next(),
                ballot_name=_str_to_internationalized_text_en(p),
            )
            for p in self.all_parties
        }

        # A ballot style is a subset of all of the contests on the ballot. Luckily, we have a column
        # in the data ("ballot type"), which is exactly what we need.

        # "Geopolitical units" are meant to be their own thing (cities, counties, precincts, etc.),
        # but we don't have any data at all about them in the Dominion CVR file. Our current hack
        # is that we have a singular geopolitical unit that we use everywhere. This is wrong, but
        # it's tolerable.

        gp = GeopoliticalUnit(
            "gpunit-singleton",
            "Global Geopolitical Unit",
            type=ReportingUnitType.unknown,
        )
        ballotstyle_uids = UidMaker("ballotstyle")

        ballotstyle_map = {
            bt: self._ballot_style_from_id(gp, bt, ballotstyle_uids, party_map)
            for bt in self.ballot_types
        }

        candidate_uids = UidMaker("candidate")
        contest_uids = UidMaker("contest")
        selection_uids = UidMaker("selection")

        contest_map: Dict[str, ContestDescription] = {}
        all_candidates: List[Candidate] = []

        for name in self.contest_map.keys():
            contest_description, candidates = self._contest_name_to_description(
                name=name,
                candidate_uid_maker=candidate_uids,
                contest_uid_maker=contest_uids,
                selection_uid_maker=selection_uids,
                gp=gp,
            )
            contest_map[name] = contest_description
            all_candidates = all_candidates + candidates

        return (
            ElectionDescription(
                name=_str_to_internationalized_text_en(self.election_name),
                election_scope_id=self.election_name,
                type=ElectionType.unknown,
                start_date=date,
                end_date=date,
                geopolitical_units=[gp],
                parties=list(party_map.values()),
                candidates=all_candidates,
                contests=list(contest_map.values()),
                ballot_styles=list(ballotstyle_map.values()),
            ),
            ballots,
        )
        pass


def read_dominion_csv(file: Union[str, StringIO]) -> Optional[DominionCSV]:
    """
    Given a filename of a Dominion CSV (or a StringIO buffer with the same data), tries
    to read it. If successful, you get back a named-tuple which describes the election.

    The contest map is a dictionary. The keys are the titles of the contests, and the
    values are a second level of dictionary, mapping from the name of each choice to
    the ultimate string that's used as a column identifier in the Pandas dataframe.

    """
    try:
        df = pd.read_csv(file, header=[0, 1, 2, 3], quoting=csv.QUOTE_NONE)
    except FileNotFoundError:
        return None
    except pd.errors.ParserError:
        return None

    # TODO: At this point, we know the file is a valid CSV and we're *assuming* it's a valid Dominion file.
    #   We shouldn't make that assumption, but checking for it would be really tricky.

    filtered_columns = [
        [_fix_strings(e) for e in c if (not e.startswith("Unnamed:") and not e == '""')]
        for c in df.columns
    ]
    election_name = filtered_columns[0][0]

    # The first two columns have the election name and a version number in them, so we have to treat those specially,
    # otherwise, we're looking for columns with only one thing in them, which says that they're not a contest (with
    # choices) but instead they're one of the metadata columns.
    ballot_metadata_fields = (
        filtered_columns[0][1:]
        + filtered_columns[1][1:]
        + [x[0] for x in filtered_columns[2:] if len(x) == 1]
    )

    df = df.applymap(_fix_strings)
    column_names = [
        filtered_columns[0][1:],
        filtered_columns[1][1:],
    ] + filtered_columns[2:]

    df.columns = [" | ".join(x) for x in column_names]
    df["UID"] = df.apply(
        lambda row: _row_to_uid(row, election_name, ballot_metadata_fields), axis=1
    )

    if "BallotType" not in df:
        return None

    # Now we're going to extract a mapping from contest titles to all the choices.
    contests = [x for x in filtered_columns[2:] if len(x) > 1]
    contest_keys = set()

    all_parties: Set[str] = set()
    contest_map: CONTEST_MAP = {}
    contest_key_to_title: Dict[str, str] = {}
    for contest in contests:
        title = contest[0]
        candidate = contest[1]
        party = _fix_party_string(contest[2]) if len(contest) > 2 else ""

        if party not in all_parties and party != "":
            all_parties.add(party)

        choice = (candidate, party)
        key = " | ".join(contest)
        contest_keys.add(key)

        if title not in contest_map:
            contest_map[title] = {}

        # goes from ["Representative - District 1"][("Alice", "DEM")] to "Representative - District 1 | Alice | DEM"
        contest_map[title][choice] = key

        # goes from "Representative - District 1 | Alice | DEM" to "Representative - District 1"
        contest_key_to_title[key] = title

    style_map: STYLE_MAP = {}

    # We're computing a set-union of all the non-empty contest fields we find, in any ballot
    # sharing a given BallotType setting, i.e., we're inferring which contests are actually
    # a part of each BallotType.

    # Potential degenerate result: in a race with very few ballots cast, it's conceivable that
    # every single ballot will undervote in at least one contest. In this specific circumstance,
    # the style map will be "wrong", which would mean that that specific candidate would be
    # completely missing from subsequent e2e crypto results. Hopefully, actual Dominion CVRs
    # will have zeros rather than blank cells to represent these undervotes, and then this case
    # will never occur.

    for index, row in df.iterrows():
        ballot_type = row["BallotType"]
        present_contests = {
            contest_key_to_title[k] for k in contest_keys if _nonempty_elem(row, k)
        }

        if ballot_type not in style_map:
            style_map[ballot_type] = present_contests
        else:
            style_map[ballot_type] = style_map[ballot_type].union(present_contests)

    return DominionCSV(
        _fix_strings(election_name),
        contest_keys,
        set(df["BallotType"]),
        all_parties,
        style_map,
        contest_map,
        df,
    )
