import csv
import re
from datetime import datetime
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
from electionguard.ballot import PlaintextBallot, PlaintextBallotContest
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
from electionguard.encrypt import selection_from

from arlo_e2e.eg_helpers import UidMaker
from arlo_e2e.metadata import (
    ElectionMetadata,
    CONTEST_MAP,
    STYLE_MAP,
    SelectionMetadata,
)
from arlo_e2e.utils import flatmap


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


def fix_strings(s: Any) -> Any:
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


def fix_party_string(s: Any) -> str:
    """
    Specifically converting something from the 4th line of input to a "party" is a little bit
    trickier, because not all races have parties. If we have one, we'll return that as a string,
    otherwise empty-string.
    """
    if s is None or (isinstance(s, float) and isnan(s)):
        return ""
    else:
        return str(s)


def dominion_row_to_uid(row: pd.Series, election_title: str, fields: List[str]) -> str:
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
    try:
        val = getattr(row, key)
    except AttributeError:
        assert False, f"key should always be present; key: ({key}), row: ({row})"

    # seems that we'll sometimes get None and other times get NaN, so we
    # have to be extra paranoid.
    if isinstance(val, float):
        return not isnan(val)
    elif isinstance(val, str):
        return str != ""
    else:
        return val is not None


def _str_to_internationalized_text_en(s: str) -> InternationalizedText:
    return InternationalizedText([Language(s, language="en")])


class DominionCSV(NamedTuple):
    metadata: ElectionMetadata
    """
    Public information about the election, derived from the Dominion CSV file.
    """

    data: pd.DataFrame
    """
    A Pandas DataFrame, having all the columns in the original data plus one new
    one, "UID", which is a concatenation of the various ballot metadata fields along
    with the name of the election. No two ballots should ever have the same UID.
    """

    def _all_parties_for_contests(self, contests: Iterable[str]) -> Set[str]:
        selections = flatmap(
            lambda contest: self.metadata.contest_map[contest], contests
        )

        # we're filtering out the empty-string from party names
        return {
            selection.party_name
            for selection in selections
            if selection.party_name != ""
        }

    def _ballot_style_from_id(
        self,
        dominion_ballot_style_id: str,
        party_map: Dict[str, Party],
        cd_map: Dict[str, ContestDescription],
    ) -> BallotStyle:

        contest_titles = self.metadata.style_map[dominion_ballot_style_id]

        bs_id = self.metadata.ballot_types[dominion_ballot_style_id]

        party_ids = [
            party_map[p].object_id
            for p in self._all_parties_for_contests(contest_titles)
        ]

        gp_ids = [cd_map[t].electoral_district_id for t in contest_titles]

        return BallotStyle(
            object_id=bs_id,
            geopolitical_unit_ids=gp_ids,
            party_ids=party_ids if party_ids else None,
        )

    def _selection_description_to_data_column(
        self,
        sd: SelectionDescription,
        cd_map: Dict[str, Candidate],
        column_map: Dict[Candidate, str],
    ) -> str:
        # will raise an error if anything is missing, but will that ever happen?
        return column_map[cd_map[sd.candidate_id]]

    def _contest_name_to_description(
        self, name: str, contest_uid_maker: UidMaker, gp_uid_maker: UidMaker,
    ) -> Tuple[ContestDescription, List[Candidate], GeopoliticalUnit, Dict[str, str]]:

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

        candidate_to_column: Dict[str, str] = {}

        for c in self.metadata.contest_map[name]:
            id_str = c.object_id
            id_number = int(id_str[1:])  # total hack: stripping off the first char

            candidate = Candidate(
                object_id=id_str,
                ballot_name=_str_to_internationalized_text_en(c.choice_name),
                party_id=c.party_name if c.party_name != "" else None,
                image_uri=None,
            )

            # To make our lives easier, we're going to use identical object_ids for selections
            # and candidates, and hopefully this won't break anything.
            candidates.append(candidate)
            selections.append(
                SelectionDescription(
                    object_id=candidate.object_id,
                    candidate_id=candidate.object_id,
                    sequence_order=id_number,
                )
            )

            candidate_to_column[candidate.object_id] = c.to_string()

        gp = GeopoliticalUnit(
            object_id=gp_uid_maker.next(), name=name, type=ReportingUnitType.unknown,
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
            gp,
            candidate_to_column,
        )

    def to_election_description(
        self, date: Optional[datetime] = None
    ) -> Tuple[ElectionDescription, List[PlaintextBallot], Dict[str, str]]:
        """
        Converts this data to a ElectionGuard `ElectionDescription` (having all of the metadata
        describing the election), a list of `PlaintextBallot` (corresponding to each of the
        rows in the Dominion CVR), and a dictionary from candidate object identifiers to the
        the name of the candidate (as it appears in the Pandas column).
        """

        if date is None:
            date = datetime.now()

        party_uids = UidMaker("party")
        party_map = {
            p: Party(
                object_id=party_uids.next(),
                ballot_name=_str_to_internationalized_text_en(p),
            )
            for p in self.metadata.all_parties
        }

        # A ballot style is a subset of all of the contests on the ballot. Luckily, we have a column
        # in the data ("ballot type"), which is exactly what we need.

        # "Geopolitical units" are meant to be their own thing (cities, counties, precincts, etc.),
        # but we don't have any data at all about them in the Dominion CVR file. Our current hack
        # is that we're making them one-to-one with contests.

        contest_uids = UidMaker("contest")
        gp_uids = UidMaker("gpunit")

        contest_map: Dict[str, ContestDescription] = {}
        all_candidates: List[Candidate] = []
        all_gps: List[GeopoliticalUnit] = []

        all_candidate_ids_to_columns: Dict[str, str] = {}

        for name in self.metadata.contest_map.keys():
            (
                contest_description,
                candidates,
                gp,
                candidate_id_to_column,
            ) = self._contest_name_to_description(
                name=name, contest_uid_maker=contest_uids, gp_uid_maker=gp_uids,
            )
            contest_map[name] = contest_description
            all_candidates = all_candidates + candidates
            all_gps.append(gp)
            for c in candidate_id_to_column.keys():
                all_candidate_ids_to_columns[c] = candidate_id_to_column[c]

        # ballotstyle_uids = UidMaker("ballotstyle")

        ballotstyle_map: Dict[str, BallotStyle] = {
            bt: self._ballot_style_from_id(bt, party_map, contest_map)
            for bt in self.metadata.ballot_types.keys()
        }

        # And now, for the ballots
        ballots: List[PlaintextBallot] = list()
        ballot_uids = UidMaker("b")

        for index, row in self.data.iterrows():
            ballot_type = row["BallotType"]
            # ballot_id = row["BallotId"]
            pbcontests: List[PlaintextBallotContest] = []

            contest_titles: Set[str] = self.metadata.style_map[ballot_type]
            for title in contest_titles:
                # This is insanely complicated. The challenge is that we have the Dominion data structures,
                # which has its own column names, but we have to connect that with all of the ElectionGuard
                # structures, which don't just let you follow from one to the other. Instead, it's a twisty
                # world of object_ids. Thus, we need mappings to go from one to the next, and have to do all
                # this extra bookkeeping, in Python dictionaries, to make all the connections.

                contest = contest_map[title]
                candidate_ids = [s.candidate_id for s in contest.ballot_selections]
                column_names = [all_candidate_ids_to_columns[c] for c in candidate_ids]
                voter_intents = [row[x] > 0 for x in column_names]
                selections: List[SelectionDescription] = contest.ballot_selections
                plaintexts = [
                    selection_from(
                        description=selections[i],
                        is_placeholder=False,
                        is_affirmative=voter_intents[i],
                    )
                    for i in range(0, len(selections))
                ]
                pbcontests.append(
                    PlaintextBallotContest(
                        object_id=contest.object_id, ballot_selections=plaintexts,
                    )
                )

            ballots.append(
                PlaintextBallot(
                    object_id=ballot_uids.next(),
                    ballot_style=ballotstyle_map[ballot_type].object_id,
                    contests=pbcontests,
                )
            )

        return (
            ElectionDescription(
                name=_str_to_internationalized_text_en(self.metadata.election_name),
                election_scope_id=self.metadata.election_name,
                type=ElectionType.unknown,
                start_date=date,
                end_date=date,
                geopolitical_units=all_gps,
                parties=list(party_map.values()),
                candidates=all_candidates,
                contests=list(contest_map.values()),
                ballot_styles=list(ballotstyle_map.values()),
            ),
            ballots,
            all_candidate_ids_to_columns,
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
        df = pd.read_csv(
            file,
            header=[0, 1, 2, 3],
            quoting=csv.QUOTE_MINIMAL,
            sep=",",
            engine="python",
        )
    except FileNotFoundError:
        return None
    except pd.errors.ParserError:
        return None

    # TODO: At this point, we know the file is a valid CSV and we're *assuming* it's a valid Dominion file.
    #   We shouldn't make that assumption, but checking for it would be really tricky.

    filtered_columns = [
        [fix_strings(e) for e in c if (not e.startswith("Unnamed:") and not e == '""')]
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

    df = df.applymap(fix_strings)
    column_names = [
        filtered_columns[0][1:],
        filtered_columns[1][1:],
    ] + filtered_columns[2:]

    # new_column_names, max_votes_for_map = _fixup_column_names(column_names)

    max_votes_for_map: Dict[str, int] = {}
    vote_for_n_pattern = re.compile(r"\s*\(Vote For=(\d+)\)$")
    new_column_names: List[str] = []
    contests = []
    selection_uid_iter = UidMaker("s")

    for column in column_names:
        vote_for_n = 1  # until proven otherwise
        title = column[0]
        vote_for_n_match: Optional[re.Match] = vote_for_n_pattern.search(title)

        if vote_for_n_match is not None:
            vote_for_n = int(vote_for_n_match.group(1))
            # chop off the "(Vote For=N)" part
            title = title[0 : vote_for_n_match.span()[0]]

        max_votes_for_map[title] = vote_for_n
        new_column = [title] + column[1:]
        new_column_names.append(" | ".join(new_column))

        # Now we're going to extract a mapping from contest titles to all the choices.
        if len(column) > 1:
            contests.append(new_column)

    contest_keys = set()

    all_parties: Set[str] = set()
    contest_map_builder: Dict[str, List[SelectionMetadata]] = {}
    contest_key_to_title: Dict[str, str] = {}
    for contest in contests:
        title = str(contest[0])
        candidate = str(contest[1])
        party = fix_party_string(contest[2]) if len(contest) > 2 else ""

        if party not in all_parties and party != "":
            all_parties.add(party)

        if party != "":
            key = " | ".join([title, candidate, party])
        else:
            key = " | ".join([title, candidate])

        contest_keys.add(key)

        if title not in contest_map_builder:
            contest_map_builder[title] = []

        # goes from ["Representative - District 1"] to a list of SelectionMetadata objects
        uid_int, uid_str = selection_uid_iter.next_int()
        metadata = SelectionMetadata(
            object_id=uid_str,
            sequence_number=uid_int,
            contest_name=title,
            choice_name=candidate,
            party_name=party,
        )
        contest_map_builder[title].append(metadata)

        # goes from "Representative - District 1 | Alice | DEM" to "Representative - District 1"
        contest_key_to_title[metadata.to_string()] = title

    df.columns = new_column_names

    df["Guid"] = df.apply(
        lambda r: dominion_row_to_uid(r, election_name, ballot_metadata_fields), axis=1,
    )

    # If the election official put numbers in as their ballot types, that's going to cause type
    # errors, because we really want to deal with them as strings.
    df["BallotType"] = df["BallotType"].apply(lambda s: str(s))

    # there's probably an easier way to do this, but it does what we want
    ballot_uid_iter = UidMaker("b")
    df["BallotId"] = df.apply(lambda r: ballot_uid_iter.next(), axis=1,)

    if "BallotType" not in df:
        return None

    ballotstyle_uids = UidMaker("ballotstyle")
    all_types = sorted(set(df["BallotType"]))
    ballot_type_to_bsid = {bt: ballotstyle_uids.next() for bt in all_types}

    contest_map: CONTEST_MAP = {
        k: set(contest_map_builder[k]) for k in contest_map_builder.keys()
    }
    style_map: STYLE_MAP = {}

    ballot_id_to_ballot_type: Dict[str, str] = {}

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
        ballot_id_to_ballot_type[row["BallotId"]] = ballot_type
        present_contests = {
            contest_key_to_title[k] for k in contest_keys if _nonempty_elem(row, k)
        }

        if ballot_type not in style_map:
            style_map[ballot_type] = present_contests
        else:
            style_map[ballot_type] = style_map[ballot_type].union(present_contests)

    return DominionCSV(
        ElectionMetadata(
            fix_strings(election_name),
            ballot_type_to_bsid,
            ballot_id_to_ballot_type,
            all_parties,
            style_map,
            contest_map,
            max_votes_for_map,
        ),
        df,
    )
