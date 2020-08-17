import argparse
from typing import Optional

from electionguard.election import InternalElectionDescription
from electionguard.serializable import set_serializers, set_deserializers
from tqdm import tqdm

from arlo_e2e.decrypt import load_proven_ballot, verify_proven_ballot_proofs
from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults

if __name__ == "__main__":
    set_serializers()
    set_deserializers()

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
        "ballot_id", type=str, nargs="+", help="ballot identifiers to decode",
    )

    args = parser.parse_args()
    tally_dir = args.tallies
    decrypted_dir = args.decrypted
    ballot_ids = args.ballot_id

    print(f"Loading tallies from {tally_dir}.")
    results: Optional[FastTallyEverythingResults] = load_fast_tally(
        tally_dir, check_proofs=False
    )

    if results is None:
        print(f"Failed to load results from {tally_dir}")
        exit(1)

    for bid in ballot_ids:
        if bid not in results.metadata.ballot_id_to_ballot_type:
            print(f"Ballot id {bid} is not part of the tally")

    encrypted_ballots = [results.get_encrypted_ballot(bid) for bid in ballot_ids]
    if None in encrypted_ballots:
        print("Missing files on disk. Exiting.")
        exit(1)

    plaintext_ballots = [load_proven_ballot(bid, decrypted_dir) for bid in ballot_ids]
    if None in plaintext_ballots:
        print("Missing files on disk. Exiting.")
        exit(1)

    ied = InternalElectionDescription(results.election_description)
    extended_base_hash = results.context.crypto_extended_base_hash

    for encrypted, plaintext in tqdm(
        list(zip(encrypted_ballots, plaintext_ballots)), desc="Proof validation"
    ):
        if not verify_proven_ballot_proofs(
            results.context.crypto_extended_base_hash,
            results.context.elgamal_public_key,
            encrypted,
            plaintext,
        ):
            print(f"Ballot {encrypted.object_id}: proof check failed!")

    # TODO: Print something useful about the plaintext ballots. Also verify them.
