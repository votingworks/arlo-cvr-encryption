import argparse
import os
from multiprocessing import Pool
from typing import Optional, Set, Dict, Tuple
from sys import exit

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.eg_helpers import log_nothing_to_stdout
from arlo_e2e.metadata import SelectionMetadata
from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults, SelectionInfo

if __name__ == "__main__":
    set_serializers()
    set_deserializers()
    log_nothing_to_stdout()

    parser = argparse.ArgumentParser(
        description="Reads an arlo-e2e tally and verifies all the cryptographic artifacts"
    )

    parser.add_argument(
        "-t",
        "--tallies",
        type=str,
        default="tally_output",
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )

    parser.add_argument(
        "--details", action="store_true", help="prints additional details on each race"
    )
    args = parser.parse_args()

    tallydir = args.tallies
    details = args.details

    pool = Pool(os.cpu_count())

    print(f"Loading tallies and ballots from {tallydir}.")
    results: Optional[FastTallyEverythingResults] = load_fast_tally(
        tallydir, check_proofs=True, pool=pool
    )

    if results is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    print(
        f"Verified {results.num_ballots} encrypted ballots for {results.metadata.election_name}."
    )
    print("Tally proofs valid, and consistent with the encrypted ballots.")

    pool.close()

    if not details:
        exit(0)

    print()
    for contest_title in results.metadata.contest_name_order:
        max_votes_per_contest = results.metadata.max_votes_for_map[contest_title]
        explanation = (
            "" if max_votes_per_contest == 1 else f" (VOTE FOR={max_votes_per_contest})"
        )
        print(f"{contest_title}{explanation}")

        selections: Set[SelectionMetadata] = results.metadata.contest_map[contest_title]
        contest_result: Dict[str, Tuple[int, int]] = {}

        for s in selections:
            if s.object_id not in results.tally.map:
                print(
                    f"Internal error: didn't find {s.object_id} for {s.to_string()} in the tally!"
                )
                exit(1)

            tally: SelectionInfo = results.tally.map[s.object_id]
            contest_result[s.to_string_no_contest()] = (
                tally.decrypted_tally,
                s.sequence_number,
            )

        sorted_result = sorted(
            [
                (name, contest_result[name][0], contest_result[name][1])
                for name in contest_result.keys()
            ],
            key=lambda t: t[1],
            reverse=True,
        )

        winners = [
            (name, result, sequence_number, " (*)")
            for name, result, sequence_number in sorted_result[:max_votes_per_contest]
        ]
        losers = [
            (name, result, sequence_number, "")
            for name, result, sequence_number in sorted_result[max_votes_per_contest:]
        ]

        for name, result, sequence_number, asterisk in sorted(
            winners + losers, key=lambda ss: ss[2]
        ):
            print(f"    {name}: {result}{asterisk}")
