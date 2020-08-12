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

    print(f"Loading tallies from {tallydir}...")
    results: Optional[FastTallyEverythingResults] = load_fast_tally(
        tallydir, check_proofs=False
    )

    if results is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    matching: Dict[str, int] = {}

    print("Ballot styles:")
    for style in sorted(results.metadata.ballot_types.keys()):
        n = len(
            [
                b
                for b in results.encrypted_ballots
                if b.ballot_style == results.metadata.ballot_types[style]
            ]
        )
        print(f"  {style}: {n} ballot(s)")
        matching[style] = n

    print()
    print("Contests:")
    for contest_title in sorted(results.metadata.contest_map.keys()):
        styles_with_contest = [
            k
            for k in results.metadata.style_map.keys()
            if contest_title in results.metadata.style_map[k]
        ]
        num_ballots = sum(matching[s] for s in styles_with_contest)
        print(
            f"  {contest_title} appears on {len(styles_with_contest)} ballot style(s) or {num_ballots} total ballot(s)"
        )
