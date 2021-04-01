import argparse
from sys import exit
from typing import Optional

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.io import validate_directory_input
from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults

if __name__ == "__main__":
    set_serializers()
    set_deserializers()

    parser = argparse.ArgumentParser(
        description="Reads an arlo-e2e tally and prints statistics about ballot styles and contests"
    )

    parser.add_argument(
        "-t",
        "--tallies",
        type=str,
        default="tally_output",
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )

    args = parser.parse_args()
    tallydir = args.directory

    tallydir = validate_directory_input(tallydir, "tally", error_if_absent=True)

    results: Optional[FastTallyEverythingResults] = load_fast_tally(
        tallydir, check_proofs=False
    )

    if results is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    print(f"Loading complete: {results.num_ballots} ballots found.")

    print("Ballot styles:")
    all_ballot_styles = results.metadata.ballot_types.keys()
    style_count = {
        style: len(results.get_ballot_ids_matching_ballot_styles([style]))
        for style in all_ballot_styles
    }
    for style in sorted(style_count.keys()):
        print(f"  {style}: {style_count[style]} ballot(s)")

    print("\nContests:")
    for contest_title in sorted(results.metadata.contest_map.keys()):
        matching_ballot_types = [
            x
            for x in results.metadata.style_map.keys()
            if contest_title in results.metadata.style_map[x]
        ]
        total = sum([style_count[x] for x in matching_ballot_types])
        print(
            f"  {contest_title} appears on {len(matching_ballot_types)} ballot style(s) or {total} total ballot(s)"
        )
