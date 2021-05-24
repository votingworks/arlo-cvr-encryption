import argparse
import os
from multiprocessing import Pool
from sys import exit
from typing import Optional, Set, Dict, Tuple, Union

from electionguard.serializable import set_serializers, set_deserializers

from arlo_cvre.eg_helpers import log_nothing_to_stdout
from arlo_cvre.io import validate_directory_input
from arlo_cvre.metadata import SelectionMetadata
from arlo_cvre.publish import load_fast_tally, load_ray_tally
from arlo_cvre.ray_helpers import ray_init_cluster
from arlo_cvre.ray_tally import RayTallyEverythingResults
from arlo_cvre.tally import FastTallyEverythingResults, SelectionInfo

if __name__ == "__main__":
    set_serializers()
    set_deserializers()
    log_nothing_to_stdout()

    parser = argparse.ArgumentParser(
        description="Reads an arlo-cvr-encryption tally and verifies all the cryptographic artifacts"
    )

    parser.add_argument(
        "-t",
        "--tallies",
        type=str,
        default="tally_output",
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )

    parser.add_argument(
        "-r",
        "--root-hash",
        "--root_hash",
        type=str,
        default=None,
        help="optional root hash for the tally directory; if the manifest is tampered, an error is indicated",
    )

    parser.add_argument(
        "--totals",
        action="store_true",
        help="prints the verified totals for every race",
    )

    parser.add_argument(
        "--cluster",
        action="store_true",
        help="uses a Ray cluster for distributed computation",
    )
    args = parser.parse_args()

    tallydir = validate_directory_input(args.tallies, "tally", error_if_absent=True)
    totals = args.totals
    use_cluster = args.cluster
    root_hash = args.root_hash

    results: Optional[Union[RayTallyEverythingResults, FastTallyEverythingResults]]

    print(f"Loading and verifying tallies and ballots from {tallydir}.")
    if use_cluster:
        ray_init_cluster()

        ray_results = load_ray_tally(
            tallydir,
            check_proofs=True,
            recheck_ballots_and_tallies=True,
            root_hash=root_hash,
        )

        results = ray_results

    else:
        pool = Pool(os.cpu_count())

        fast_results = load_fast_tally(
            tallydir,
            check_proofs=True,
            pool=pool,
            recheck_ballots_and_tallies=True,
            root_hash=root_hash,
        )

        pool.close()
        results = fast_results

    if results is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    print(
        f"Verified {results.num_ballots} encrypted ballots for {results.metadata.election_name}."
    )
    print("Tally proofs valid, and consistent with the encrypted ballots.")

    if totals:
        print()
        for contest_title in results.metadata.contest_name_order:
            max_votes_per_contest = results.metadata.max_votes_for_map[contest_title]
            explanation = (
                ""
                if max_votes_per_contest == 1
                else f" (VOTE FOR={max_votes_per_contest})"
            )
            print(f"{contest_title}{explanation}")

            selections: Set[SelectionMetadata] = results.metadata.contest_map[
                contest_title
            ]
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
                for name, result, sequence_number in sorted_result[
                    :max_votes_per_contest
                ]
            ]
            losers = [
                (name, result, sequence_number, "")
                for name, result, sequence_number in sorted_result[
                    max_votes_per_contest:
                ]
            ]

            for name, result, sequence_number, asterisk in sorted(
                winners + losers, key=lambda ss: ss[2]
            ):
                print(f"    {name}: {result}{asterisk}")
