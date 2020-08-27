import argparse
from typing import Optional, Set
from sys import exit

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults

if __name__ == "__main__":
    set_serializers()
    set_deserializers()

    parser = argparse.ArgumentParser(
        description="Prints ballot-ids for all ballots having the desired contest(s)"
    )

    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        default=["tally_output"],
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )

    parser.add_argument(
        "contest", type=str, nargs="+", help="text prefix(es) for contest",
    )

    args = parser.parse_args()

    tallydir = args.directory[0]
    contest_prefixes = args.contest

    results: Optional[FastTallyEverythingResults] = load_fast_tally(
        tallydir, check_proofs=False
    )

    if results is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    matching_contest_titles = results.get_contest_titles_matching(contest_prefixes)

    matching_ballot_styles = results.get_ballot_styles_for_contest_titles(
        matching_contest_titles
    )

    matching_ballots = results.get_ballots_matching_ballot_styles(
        matching_ballot_styles
    )

    matching_ballot_ids: Set[str] = {b.object_id for b in matching_ballots}

    print("Matching contest titles:")
    print("  " + "\n  ".join(sorted(matching_contest_titles)))

    print("\nMatching ballot styles having one or more of these contests:")
    print("  " + "\n  ".join(sorted(matching_ballot_styles)))

    print("\nBallot IDs for every ballot in these ballot styles:")
    print("  " + "\n  ".join(sorted(matching_ballot_ids)))
