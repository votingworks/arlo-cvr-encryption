import argparse
from typing import Optional, Dict

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults

if __name__ == "__main__":
    set_serializers()
    set_deserializers()

    parser = argparse.ArgumentParser(
        description="Reads an arlo-e2e tally and prints statistics about ballot styles and contests"
    )

    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        default="tally_output",
        help="directory name for where the tally artifacts can be found",
    )

    args = parser.parse_args()

    tallydir = args.directory

    results: Optional[FastTallyEverythingResults] = load_fast_tally(
        tallydir, check_proofs=False
    )

    if results is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    matching: Dict[str, int] = {
        style: len(results.get_ballots_matching_ballot_styles([style]))
        for style in results.metadata.ballot_types.keys()
    }

    print("Ballot styles:")
    for style in sorted(matching.keys()):
        print(f"  {style}: {matching[style]} ballot(s)")

    print("\nContests:")
    for contest_title in sorted(results.metadata.contest_map.keys()):
        styles_with_contest = results.get_ballot_styles_for_contest_titles(
            [contest_title]
        )
        num_ballots = sum(matching[s] for s in styles_with_contest)
        print(
            f"  {contest_title} appears on {len(styles_with_contest)} ballot style(s) or {num_ballots} total ballot(s)"
        )
