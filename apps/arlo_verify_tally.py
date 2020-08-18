import argparse
import os
from multiprocessing import Pool
from typing import Optional, Set

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
    for contest_title in sorted(results.metadata.contest_map.keys()):
        print(contest_title)
        selections: Set[SelectionMetadata] = results.metadata.contest_map[contest_title]
        first = True
        for s in sorted(selections, key=lambda s: s.sequence_number):
            if s.object_id not in results.tally.map:
                print(
                    f"Internal error: didn't find {s.object_id} for {s.to_string()} in the tally!"
                )
                exit(1)

            tally: SelectionInfo = results.tally.map[s.object_id]
            print(f"    {s.to_string_no_contest()}: {tally.decrypted_tally}")