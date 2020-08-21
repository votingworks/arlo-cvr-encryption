from dataclasses import dataclass
from typing import Dict, Set, List

from electionguard.serializable import Serializable


# A hash function isn't auto-generated unless a data class is frozen, which this certainly can be,
# but we get a warning unless the superclass is also frozen, and Serializable comes from ElectionGuard,
# where it isn't frozen. Saying unsafe_hash=True is our workaround.


@dataclass(eq=True, unsafe_hash=True)
class SelectionMetadata(Serializable):
    """
    This class contains useful information to understand every ballot "selection" (i.e., every ballot
    has one or more contests, each of which has one or more selections). This is necessary to map
    from the selection `object_id` values in the corresponding ElectionGuard structures back to
    the actual text associated with those selections.

    Everything in this structure is suitable for sharing with the public. There are no "secrets" here.
    """

    object_id: str
    """
    Used within ElectionGuard as a unique identifier for this particular selection.
    """

    sequence_number: int
    """
    This value is embedded in `object_id`, and is also a unique identifier.
    """

    contest_name: str
    """
    The name of the seat being challenged (e.g., "City Council, District 5") or of the referendum
    ("Proposition 1").
    """

    choice_name: str
    """
    Either the name of a candidate ("Alice Smith") or of a particular choice in a referendum ("FOR").
    """

    party_name: str
    """
    For candidates who are affiliated with a party, the string associated with that party goes here.
    For nonpartisan contests and referenda, this field is the empty-string.
    """

    def to_string(self) -> str:
        """
        Returns a unique string representation of this selection, suitable for use as the column
        title of a Pandas dataframe or whatever else. Notably ignores the `object_id`.
        """
        return f"{self.contest_name} | {self.to_string_no_contest()}"

    def to_string_no_contest(self) -> str:
        """
        Returns a string representation of this selection, without the contest title.
        """
        return f"{self.choice_name}" + (
            f" | {self.party_name}" if self.party_name != "" else ""
        )


CONTEST_MAP = Dict[str, Set[SelectionMetadata]]
STYLE_MAP = Dict[str, Set[str]]


@dataclass(eq=True)
class ElectionMetadata(Serializable):
    """
    This data structure represents everything that we have that describes the original election.

    Everything in this structure is suitable for sharing with the public. There are no "secrets" here.
    """

    election_name: str
    """
    Name of the election (as provided by the election administrator).
    """

    ballot_types: Dict[str, str]
    """
    Keys are the different "ballot type" names (as provided by the election administrator),
    values are the different object id's used in ElectionGuard for those ballot styles.
    """

    ballot_id_to_ballot_type: Dict[str, str]
    """
    Keys are the ballot id numbers, corresponding to their file names on disk (and their ElectionGuard
    object ids). Values are the ballot types (as provided by the election administrator).
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
    A dictionary mapping from contest titles to a set of `SelectionMetadata` instances.
    """

    max_votes_for_map: Dict[str, int]
    """
    A dictionary mapping from contest titles to the maximum number of selections a voter can legally pick
    (i.e., the k in a k-for-n contest).
    """

    contest_name_order: List[str]
    """
    Names of all of the contests, in the order they appear on the ballot.
    """
