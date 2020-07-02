# Hypothesis "strategies" to generate Dominion ballots
from typing import List, Union, Tuple, Dict, Sequence

from electionguardtest.election import human_names, _DrawType
from hypothesis.strategies import (
    composite,
    booleans,
    integers,
    lists,
    sampled_from,
    tuples,
)

from utils import flatmap


@composite
def counting_groups(draw: _DrawType):
    """
    Some Dominion CVRs don't use counting groups at all, but when they do, it's a string like
    "Regular", "Provisional", "Mail", and so forth. This strategy returns one of these common strings.
    """
    return draw(
        sampled_from(["Regular", "Provisional", "Mail", "Election Day", "In Person"])
    )


def _encode_list_as_csv(
    line: Sequence[Union[str, int]], quotation_style: int, total_columns: int
) -> str:
    # Temporary engineering decision: we're directly implementing the quoting here, rather
    # than using Python's CSV module, which has several "dialects", because we're trying to
    # emulate all the behaviors we see in Dominion's CVR files.

    # Among other issues, that means that we're not doing escaping properly. Probably not
    # a huge issue, since in the real code we're reading the CVRs using Pandas, which does
    # all the right things. Hopefully.

    assert len(line) == total_columns, "wrong number of columns!"

    encoded_line: List[str] = []
    for s in line:
        if quotation_style == 0:
            # no quotation marks at all, even for strings
            encoded_line.append(str(s))
        elif quotation_style == 1:
            encoded_line.append(f'"{str(s)}"')
        else:
            if isinstance(s, int) or (s != "" and s[0].isdigit()):
                # equal-sign prefix before quotation marks seems to only happen on numeric digits
                encoded_line.append(f'="{str(s)}"')
            else:
                encoded_line.append(f'"{str(s)}"')
    return ",".join(encoded_line)


@composite
def dominion_cvrs(draw: _DrawType):
    """
    This strategy creates a multiline text string, with comma-separated-values, corresponding to the
    many different styles of Dominion CVRs that we might see. For the following fields, the returned
    string will simply be a small integer: `TabulatorNum`, `BatchId`, `RecordId`, `ImprintedId`, `PrecinctPortion`.
    `CvrNumber` will be sequential, starting at 1. `BallotType` will represent the ballot styles available.
    `CountingGroup` is sometimes present, sometimes absent, in "real" Dominion CVRs, so you'll get it
    both ways from this strategy, and have a typical string (see `counting_groups`).

    Write-in votes are normally represented as just a 0/1 slot, like any other vote; they're not included
    in the output of this strategy.
    """
    num_cvrs: int = draw(integers(1, 300))
    num_referenda: int = draw(integers(1, 5))
    max_candidates_per_contest: int = draw(integers(1, 5))
    num_candidates_per_contest: List[int] = draw(
        lists(
            integers(1, max_candidates_per_contest),
            min_size=1,
            max_size=max_candidates_per_contest,
        )
    )

    party_strings = [f"PARTY{i}" for i in range(1, max_candidates_per_contest + 1)]
    candidates_and_parties: List[List[Tuple[str, str]]] = [
        draw(
            lists(
                tuples(human_names(), sampled_from(party_strings)),
                min_size=n,
                max_size=n,
            )
        )
        for n in num_candidates_per_contest
    ]
    num_human_contests = len(candidates_and_parties)

    contest_names = [f"Contest{i}" for i in range(1, num_human_contests + 1)]
    referenda_names = [f"Referendum{i}" for i in range(1, num_referenda + 1)]

    num_ballot_styles: int = draw(integers(1, 10))
    total_voter_choice_slots: int = sum(num_candidates_per_contest) + 2 * num_referenda

    # 0 = straight numbers
    # 1 = quotation marks
    # 2 = preceding equals sign + quotation marks
    quotation_style: int = draw(integers(0, 2))
    counting_group_present: bool = draw(booleans())

    total_metadata_columns = 7 + (1 if counting_group_present else 0)
    total_csv_columns = total_metadata_columns + total_voter_choice_slots
    ballot_style_strings = [f"BALLOTSTYLE{i}" for i in range(0, num_ballot_styles + 1)]
    # header row 1: only two fields, plus a bunch of blanks
    # header row 2: a bunch of blanks, then the contest/referenda names

    rows: List[str] = []

    # header row 1: only two fields, plus a bunch of blanks
    rows.append(
        _encode_list_as_csv(
            ["Random Test Election", "5.2.16.1"] + [""] * (total_csv_columns - 2),
            quotation_style,
            total_csv_columns,
        )
    )

    # We need both the "for" and "against" for each referendum.
    repeated_referenda_names = flatmap(lambda name: [name, name], referenda_names)

    # Different contests have different numbers of candidates, so we need different repetition.
    repeated_contest_names = flatmap(
        lambda n: [contest_names[n]] * len(candidates_and_parties[n]),
        range(0, num_human_contests),
    )

    # header row 2: a bunch of blanks, then the contest/referenda names
    rows.append(
        _encode_list_as_csv(
            [""] * total_metadata_columns
            + repeated_contest_names
            + repeated_referenda_names,
            quotation_style,
            total_csv_columns,
        )
    )

    # header row 3: candidate names, or FOR/AGAINST for referenda
    output: List[str] = [""] * total_metadata_columns
    for contest in candidates_and_parties:
        for c in contest:
            output.append(c[0])
    for r in referenda_names:
        output.append("FOR")
        output.append("AGAINST")

    rows.append(_encode_list_as_csv(output, quotation_style, total_csv_columns))

    # header row 4: column names for the CVRs, and party names for the candidates with parties
    output: List[Union[str, int]] = (
        [
            "CvrNumber",
            "TabulatorNum",
            "BatchId",
            "RecordId",
            "ImprintedId",
            "CountingGroup",
            "PrecinctPortion",
            "BallotType",
        ]
        if counting_group_present
        else [
            "CvrNumber",
            "TabulatorNum",
            "BatchId",
            "RecordId",
            "ImprintedId",
            "PrecinctPortion",
            "BallotType",
        ]
    )
    for contest in candidates_and_parties:
        for c in contest:
            output.append(c[1])
    for r in referenda_names:
        output.append("")
        output.append("")

    rows.append(_encode_list_as_csv(output, quotation_style, total_csv_columns))

    # Now we sort out which contests are associated with which ballot styles.
    # We're going to generate each CVR fully populated, and then selectively hide
    # specific contests that are not part of any given ballot style.

    num_contests = num_human_contests + num_referenda
    contest_in_bs: Dict[int, Dict[int, bool]] = {}

    for bs in range(0, num_ballot_styles):
        contest_in_bs[bs] = {}
        for c in range(0, num_contests):
            included: bool = draw(booleans())
            contest_in_bs[bs][c] = included

    for cvr_number in range(1, num_cvrs + 1):
        bs: int = draw(integers(0, num_ballot_styles - 1))
        output: List[Union[str, int]] = (
            [
                cvr_number,
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(counting_groups()),
                draw(integers(0, 3)),
                ballot_style_strings[bs],
            ]
            if counting_group_present
            else [
                cvr_number,
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                ballot_style_strings[bs],
            ]
        )

        for c in range(0, num_human_contests):
            if contest_in_bs[bs][c]:
                # -1 == undervote
                # otherwise, it's the position of the 1 vote
                selection: int = draw(integers(-1, num_candidates_per_contest[c] - 1))
                for pos in range(0, num_candidates_per_contest[c]):
                    output.append(1 if selection == pos else 0)
            else:
                for pos in range(0, num_candidates_per_contest[c]):
                    output.append("")
        for c in range(0, num_referenda):
            if contest_in_bs[bs][c + num_human_contests]:
                # -1 == undervote
                # otherwise, it's the position of the 1 vote
                selection: int = draw(integers(-1, 1))
                for pos in [0, 1]:
                    output.append(1 if selection == pos else 0)
            else:
                output.append("")
                output.append("")
        rows.append(_encode_list_as_csv(output, quotation_style, total_csv_columns))

    # for now, Unix line terminators, and nothing on the last line; we could vary this if we wanted!
    return "\n".join(rows)
