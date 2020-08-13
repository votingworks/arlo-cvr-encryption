import argparse
import functools
import os
from multiprocessing import Pool
from os import path
from typing import Optional, List

from electionguard.ballot import CiphertextAcceptedBallot, PlaintextBallot
from electionguard.decrypt_with_secrets import decrypt_ballot_with_secret
from electionguard.election import InternalElectionDescription
from electionguard.group import ElementModQ, ElementModP
from electionguard.serializable import set_serializers, set_deserializers
from tqdm import tqdm

from arlo_e2e.admin import ElectionAdmin
from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults
from arlo_e2e.utils import load_json_helper, mkdir_helper


def _decrypt(
    ied: InternalElectionDescription,
    extended_base_hash: ElementModQ,
    public_key: ElementModP,
    secret_key: ElementModQ,
    ballot: CiphertextAcceptedBallot,
) -> Optional[PlaintextBallot]:
    return decrypt_ballot_with_secret(
        ballot, ied, extended_base_hash, public_key, secret_key
    )


if __name__ == "__main__":
    set_serializers()
    set_deserializers()

    parser = argparse.ArgumentParser(description="Decrypts a list of ballots")

    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        default="tally_output",
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )
    parser.add_argument(
        "-k",
        "--keys",
        type=str,
        nargs=1,
        default="secret_election_keys.json",
        help="file name for where the information is written (default: secret_election_keys.json)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="decrypted_ballots",
        help="directory name for where decrypted ballots will be written (default: decrypted_ballots)",
    )
    parser.add_argument(
        "ballot_id",
        type=str,
        nargs="+",
        help="ballot identifiers for ballots to be decrypted",
    )

    args = parser.parse_args()
    keyfile = args.keys
    tallydir = args.directory
    outputdir = args.output
    ballot_ids = args.ballot_id

    admin_state: Optional[ElectionAdmin] = load_json_helper(".", keyfile, ElectionAdmin)
    if admin_state is None or not admin_state.is_valid():
        print(f"Election administration key material wasn't valid")
        exit(1)

    print(f"Loading tallies from {tallydir}.")
    results: Optional[FastTallyEverythingResults] = load_fast_tally(
        tallydir, check_proofs=False
    )

    if results is None:
        print(f"Failed to load results from {tallydir}")
        exit(1)

    for bid in ballot_ids:
        if bid not in results.metadata.ballot_id_to_ballot_type:
            print(f"Ballot id {bid} is not part of the tally")

    encrypted_ballots = [results.get_encrypted_ballot(bid) for bid in ballot_ids]
    if None in encrypted_ballots:
        exit(1)

    pool = Pool(os.cpu_count())

    ied = InternalElectionDescription(results.election_description)
    extended_base_hash = results.context.crypto_extended_base_hash
    public_key = admin_state.keypair.public_key
    secret_key = admin_state.keypair.secret_key

    wrapped_decrypt = functools.partial(
        _decrypt, ied, extended_base_hash, public_key, secret_key
    )
    inputs = tqdm(encrypted_ballots, desc="Decrypting ballots")
    plaintext_ballots: List[Optional[PlaintextBallot]] = pool.map(
        func=wrapped_decrypt, iterable=inputs
    )
    pool.close()

    if None in plaintext_ballots:
        print(
            f"Decryption: only {len(plaintext_ballots)} of {len(encrypted_ballots)} decrypted successfully, exiting."
        )
        exit(1)

    mkdir_helper(outputdir)
    for ballot in tqdm(plaintext_ballots, desc="Writing ballots"):
        ballot_name_prefix = ballot.object_id[0:4]  # letter b plus first three digits
        subdir = path.join(outputdir, ballot_name_prefix)
        mkdir_helper(subdir)
        ballot.to_json_file(ballot.object_id, subdir)

    pool.close()
