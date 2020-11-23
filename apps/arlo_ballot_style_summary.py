import argparse
from sys import exit
from typing import Optional, Dict

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.publish import load_ray_tally
from arlo_e2e.ray_tally import RayTallyEverythingResults

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
        default=["tally_output"],
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )

    args = parser.parse_args()

    tallydir = args.directory[0]

    # ray_init_localhost()
    rresults: Optional[RayTallyEverythingResults] = load_ray_tally(
        tallydir, check_proofs=False
    )

    if rresults is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    print(f"Loading complete: {rresults.num_ballots} ballots found.")

    print("Ballot styles:")
    style_count: Dict[str, int] = {}
    for bid in rresults.metadata.ballot_id_to_ballot_type.keys():
        ballot_type = rresults.metadata.ballot_id_to_ballot_type[bid]
        if ballot_type not in style_count:
            style_count[ballot_type] = 1
        else:
            style_count[ballot_type] += 1
    for style in sorted(style_count.keys()):
        print(f"  {style}: {style_count[style]} ballot(s)")

    print("\nContests:")
    for contest_title in sorted(rresults.metadata.contest_map.keys()):
        matching_ballot_types = [
            x
            for x in rresults.metadata.style_map.keys()
            if contest_title in rresults.metadata.style_map[x]
        ]
        total = sum([style_count[x] for x in matching_ballot_types])
        print(
            f"  {contest_title} appears on {len(matching_ballot_types)} ballot style(s) or {total} total ballot(s)"
        )
