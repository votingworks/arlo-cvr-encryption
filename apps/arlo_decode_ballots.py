import argparse
from sys import exit
from typing import Optional, List

from electionguard.serializable import set_serializers, set_deserializers

from arlo_cvre.arlo_audit import validate_plaintext_and_encrypted_ballot
from arlo_cvre.decrypt import (
    load_proven_ballot,
)
from arlo_cvre.eg_helpers import log_nothing_to_stdout
from arlo_cvre.io import validate_directory_input
from arlo_cvre.publish import load_fast_tally
from arlo_cvre.tally import FastTallyEverythingResults

if __name__ == "__main__":
    set_serializers()
    set_deserializers()
    log_nothing_to_stdout()

    parser = argparse.ArgumentParser(
        description="Validates plaintext ballots and decodes to human-readable form"
    )

    parser.add_argument(
        "-t",
        "--tallies",
        type=str,
        default="tally_output",
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )

    parser.add_argument(
        "-d",
        "--decrypted",
        type=str,
        default="decrypted_ballots",
        help="directory name for where decrypted ballots can be found (default: decrypted_ballots)",
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
        "ballot_id",
        type=str,
        nargs="+",
        help="ballot identifiers to decode",
    )

    args = parser.parse_args()
    tallydir = validate_directory_input(args.tallies, "tally", error_if_absent=True)
    decrypted_dir = args.decrypted
    ballot_ids: List[str] = args.ballot_id
    root_hash = args.root_hash

    decrypted_dir = validate_directory_input(
        decrypted_dir, "decryption", error_if_absent=True
    )

    print(f"Loading tallies from {tallydir}.")
    tally: Optional[FastTallyEverythingResults] = load_fast_tally(
        tallydir, check_proofs=False, root_hash=root_hash
    )

    if tally is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    for bid in ballot_ids:
        if bid not in tally.metadata.ballot_id_to_ballot_type:
            print(f"Ballot id {bid} is not part of the tally")

    encrypted_ballots = [tally.get_encrypted_ballot(bid) for bid in ballot_ids]
    if None in encrypted_ballots:
        print("Missing files on disk. Exiting.")
        exit(1)

    plaintext_ballots = [load_proven_ballot(bid, decrypted_dir) for bid in ballot_ids]

    for encrypted, plaintext in zip(encrypted_ballots, plaintext_ballots):
        validate_plaintext_and_encrypted_ballot(tally, plaintext, encrypted, True)
