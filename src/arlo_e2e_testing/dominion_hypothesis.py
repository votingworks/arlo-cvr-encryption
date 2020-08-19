# Hypothesis "strategies" to generate Dominion ballots
from io import StringIO
from typing import List, Union, Tuple, Dict, Sequence, NamedTuple, Optional

from electionguard.ballot import PlaintextBallot
from electionguard.election import ElectionDescription, CiphertextElectionContext
from electionguard.group import ElementModQ
from electionguardtest.election import human_names, _DrawType, ciphertext_elections
from hypothesis.strategies import (
    composite,
    booleans,
    integers,
    lists,
    sampled_from,
    tuples,
)

from arlo_e2e.dominion import DominionCSV, read_dominion_csv
from arlo_e2e.utils import flatmap


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


def _append_ballot_selections(
    draw: _DrawType,
    output: List[Union[str, int]],
    num_candidates_per_contest: int,
    max_votes_per_contest: int,
) -> None:
    assert (
        max_votes_per_contest <= num_candidates_per_contest
    ), "malformed contest max votes"

    num_selections = draw(integers(0, max_votes_per_contest))
    choices: List[int] = [1] * num_selections + [0] * (
        num_candidates_per_contest - num_selections
    )
    assert len(choices) == num_candidates_per_contest

    # now we need to randomly permute the choices; this is a hack, but it will do
    swaps: List[int] = draw(
        lists(
            integers(min_value=0, max_value=num_candidates_per_contest - 1),
            min_size=num_candidates_per_contest,
            max_size=num_candidates_per_contest,
        )
    )
    for i in range(0, len(choices)):
        tmp = choices[i]
        choices[i] = choices[swaps[i]]
        choices[swaps[i]] = tmp

    output.extend(choices)


@composite
def dominion_cvrs(draw: _DrawType, max_rows: int = 300, max_votes_per_race: int = 1):
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
    num_cvrs: int = draw(integers(1, max_rows))
    num_referenda: int = draw(integers(1, 5))
    max_candidates_per_contest: int = draw(integers(1, 5))
    num_candidates_per_contest: List[int] = draw(
        lists(
            integers(1, max_candidates_per_contest),
            min_size=1,
            max_size=max_candidates_per_contest,
        )
    )

    max_votes_per_contest_bound: List[int] = draw(
        lists(
            integers(1, max_votes_per_race),
            min_size=len(num_candidates_per_contest),
            max_size=len(num_candidates_per_contest),
        )
    )

    max_votes_per_contest = [
        min(a, b)
        for a, b in zip(num_candidates_per_contest, max_votes_per_contest_bound)
    ]

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

    contest_names: List[str] = [
        f"Contest{i + 1} (Vote For={max_votes_per_contest[i]})"
        for i in range(0, num_human_contests)
    ]
    referenda_names: List[str] = [f"Referendum{i}" for i in range(1, num_referenda + 1)]

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
    rows: List[str] = [
        _encode_list_as_csv(
            ["Random Test Election", "5.2.16.1"] + [""] * (total_csv_columns - 2),
            quotation_style,
            total_csv_columns,
        )
    ]

    # We need both the "for" and "against" for each referendum.
    repeated_referenda_names = list(flatmap(lambda name: [name, name], referenda_names))

    # Different contests have different numbers of candidates, so we need different repetition.
    repeated_contest_names = list(
        flatmap(
            lambda n: [contest_names[n]] * len(candidates_and_parties[n]),
            range(0, num_human_contests),
        )
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
    row3_output = [""] * total_metadata_columns
    for contest in candidates_and_parties:
        for c in contest:
            row3_output.append(c[0])
    for _ in referenda_names:
        row3_output.append("FOR")
        row3_output.append("AGAINST")

    rows.append(_encode_list_as_csv(row3_output, quotation_style, total_csv_columns))

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
    for _ in referenda_names:
        output.append("")
        output.append("")

    rows.append(_encode_list_as_csv(output, quotation_style, total_csv_columns))

    # Now we sort out which contests are associated with which ballot styles.
    # We're going to generate each CVR fully populated, and then selectively hide
    # specific contests that are not part of any given ballot style.

    num_contests = num_human_contests + num_referenda
    contest_in_bs: Dict[int, List[bool]] = {}

    for bs in range(0, num_ballot_styles):
        # If every bool is False, that means we have a ballot style with zero contests, which is unhelpful.
        # Also, generates warnings from ElectionGuard. Solution? Generate all at random, then forcibly set
        # one of them to True.

        contest_in_style = draw(
            lists(booleans(), min_size=num_contests, max_size=num_contests)
        )
        contest_in_bs[bs] = contest_in_style
        contest_in_bs[bs][
            draw(integers(min_value=0, max_value=num_contests - 1))
        ] = True

    for cvr_number in range(1, num_cvrs + 1):
        bs_drawn: int = draw(integers(0, num_ballot_styles - 1))
        output: List[Union[str, int]] = (
            [
                cvr_number,
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(counting_groups()),
                draw(integers(0, 3)),
                ballot_style_strings[bs_drawn],
            ]
            if counting_group_present
            else [
                cvr_number,
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                draw(integers(0, 3)),
                ballot_style_strings[bs_drawn],
            ]
        )

        for c in range(0, num_human_contests):
            if contest_in_bs[bs_drawn][c]:
                _append_ballot_selections(
                    draw,
                    output,
                    num_candidates_per_contest[c],
                    max_votes_per_contest[c],
                )
            else:
                for pos in range(0, num_candidates_per_contest[c]):
                    output.append("")
        for c in range(0, num_referenda):
            if contest_in_bs[bs_drawn][c + num_human_contests]:
                _append_ballot_selections(
                    draw, output, num_candidates_per_contest=2, max_votes_per_contest=1
                )
            else:
                output.append("")
                output.append("")
        rows.append(_encode_list_as_csv(output, quotation_style, total_csv_columns))

    # for now, Unix line terminators, and nothing on the last line; we could vary this if we wanted!
    return "\n".join(rows)


class DominionBallotsAndContext(NamedTuple):
    dominion_cvrs: DominionCSV
    ed: ElectionDescription
    secret_key: ElementModQ
    id_map: Dict[str, str]
    cec: CiphertextElectionContext
    ballots: List[PlaintextBallot]


@composite
def ballots_and_context(draw: _DrawType):
    """
    Wrapper around ElectionGuard's own `ciphertext_elections` strategy and our `dominion_cvrs`, returns
    an instance of `DominionBallotsAndContext` with everything you need for subsequent testing.
    """
    raw_cvrs = draw(dominion_cvrs())
    parsed: Optional[DominionCSV] = read_dominion_csv(StringIO(raw_cvrs))
    assert parsed is not None, "CVR parser shouldn't fail!"
    ed, ballots, id_map = parsed.to_election_description()
    secret_key, cec = draw(ciphertext_elections(ed))

    return DominionBallotsAndContext(parsed, ed, secret_key, id_map, cec, ballots)
