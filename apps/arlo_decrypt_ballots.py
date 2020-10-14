import argparse
import os
from multiprocessing import Pool
from typing import Optional, cast, List
from sys import exit

from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.election import InternalElectionDescription
from electionguard.serializable import set_serializers, set_deserializers
from tqdm import tqdm

from arlo_e2e.admin import ElectionAdmin
from arlo_e2e.decrypt import decrypt_ballots, write_proven_ballot
from arlo_e2e.eg_helpers import log_nothing_to_stdout
from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults
from arlo_e2e.utils import load_json_helper, mkdir_helper

if __name__ == "__main__":
    set_serializers()
    set_deserializers()
    log_nothing_to_stdout()

    parser = argparse.ArgumentParser(description="Decrypts a list of ballots")

    parser.add_argument(
        "-t",
        "--tallies",
        type=str,
        default="tally_output",
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )
    parser.add_argument(
        "-k",
        "--keys",
        type=str,
        default="secret_election_keys.json",
        help="file name for where the information is written (default: secret_election_keys.json)",
    )
    parser.add_argument(
        "-d",
        "--decrypted",
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
    tally_dir = args.tallies
    decrypted_dir = args.decrypted
    ballot_ids = args.ballot_id

    admin_state: Optional[ElectionAdmin] = load_json_helper(".", keyfile, ElectionAdmin)
    if admin_state is None or not admin_state.is_valid():
        print(f"Election administration key material wasn't valid")
        exit(1)

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
        exit(1)

    pool = Pool(os.cpu_count())

    ied = InternalElectionDescription(results.election_description)
    extended_base_hash = results.context.crypto_extended_base_hash
    decryptions = decrypt_ballots(
        ied,
        extended_base_hash,
        admin_state.keypair,
        pool,
        cast(List[CiphertextAcceptedBallot], encrypted_ballots),
    )

    pool.close()

    num_successful_decryptions = {len([d for d in decryptions if d is not None])}

    if num_successful_decryptions != len(encrypted_ballots) in decryptions:
        print(
            f"Decryption: only {num_successful_decryptions} of {len(encrypted_ballots)} decrypted successfully, exiting."
        )
        exit(1)

    mkdir_helper(decrypted_dir)
    for pballot in tqdm(decryptions, desc="Writing ballots"):
        write_proven_ballot(pballot, decrypted_dir)
