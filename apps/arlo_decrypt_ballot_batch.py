import argparse
from sys import exit
from typing import Optional

import ray
from electionguard.serializable import set_serializers, set_deserializers

from arlo_cvre.admin import ElectionAdmin
from arlo_cvre.arlo_audit import (
    get_imprint_ids_from_ballot_retrieval_csv,
    get_ballot_ids_from_imprint_ids,
)
from arlo_cvre.decrypt import decrypt_and_write
from arlo_cvre.eg_helpers import log_nothing_to_stdout
from arlo_cvre.io import (
    wait_for_zero_pending_writes,
    make_file_ref_from_path,
    validate_directory_input,
)
from arlo_cvre.publish import load_ray_tally
from arlo_cvre.ray_helpers import ray_init_cluster, ray_init_localhost
from arlo_cvre.ray_tally import RayTallyEverythingResults

if __name__ == "__main__":
    set_serializers()
    set_deserializers()
    log_nothing_to_stdout()

    parser = argparse.ArgumentParser(
        description="Decrypts a batch of ballots based on a ballot retrieval CSV file"
    )

    parser.add_argument(
        "--cluster",
        action="store_true",
        help="uses a Ray cluster for distributed computation",
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
        "batch_file",
        type=str,
        nargs=1,
        help="filename for the ballot retrieval CSV file (no default)",
    )

    args = parser.parse_args()
    keyfile = args.keys
    tally_dir = validate_directory_input(args.tallies, "tally", error_if_absent=True)
    decrypted_dir = validate_directory_input(args.decrypted, "decryption")
    batch_file = args.batch_file[0]
    use_cluster = args.cluster
    root_hash = args.root_hash

    if use_cluster:
        ray_init_cluster()
    else:
        ray_init_localhost()

    admin_state: Optional[ElectionAdmin] = make_file_ref_from_path(keyfile).read_json(
        ElectionAdmin
    )
    if admin_state is None or not admin_state.is_valid():
        print(f"Election administration key material wasn't valid")
        exit(1)

    print(f"Loading tallies from {tally_dir}.")
    results: Optional[RayTallyEverythingResults] = load_ray_tally(
        tally_dir, check_proofs=False, root_hash=root_hash
    )

    if results is None:
        print(f"Failed to load results from {tally_dir}")
        exit(1)

    print(f"Processing {batch_file}.")
    imprint_ids = get_imprint_ids_from_ballot_retrieval_csv(batch_file)

    if len(imprint_ids) == 0:
        # other errors will have been logged beforehand
        print("Nothing to decrypt")
        exit(1)

    fresults = results.to_fast_tally()
    bids = get_ballot_ids_from_imprint_ids(fresults, imprint_ids)

    decrypt_and_write(admin_state, fresults, bids, decrypted_dir)

    num_failures = wait_for_zero_pending_writes()
    if num_failures > 0:
        print(f"WARNING: Failed to write {num_failures} files. Something bad happened.")

    ray.shutdown()
