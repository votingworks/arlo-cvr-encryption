import argparse
from sys import exit
import time
from typing import Optional

import ray
from electionguard.serializable import set_serializers, set_deserializers

from arlo_cvre.admin import ElectionAdmin
from arlo_cvre.dominion import read_dominion_csv
from arlo_cvre.io import (
    wait_for_zero_pending_writes,
    make_file_ref_from_path,
    validate_directory_input,
)
from arlo_cvre.ray_helpers import (
    ray_init_cluster,
    ray_init_localhost,
    ray_wait_for_workers,
)
from arlo_cvre.ray_tally import ray_tally_everything

if __name__ == "__main__":
    set_serializers()
    set_deserializers()

    parser = argparse.ArgumentParser(
        description="Load a Dominion-style ballot CVR file and write out an Arlo-cvr-encryption tally"
    )
    parser.add_argument(
        "-k",
        "--keys",
        type=str,
        default="secret_election_keys.json",
        help="file name for the election official's key materials (default: secret_election_keys.json)",
    )
    parser.add_argument(
        "-t",
        "--tallies",
        type=str,
        default="tally_output",
        help="directory name for where the tally is written (default: tally_output)",
    )
    parser.add_argument(
        "--cluster",
        action="store_true",
        help="uses a Ray cluster for distributed computation",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="verifies every ballot proof as it's generated (default: False); note, this slows the process significantly",
    )
    parser.add_argument(
        "cvr_file",
        type=str,
        nargs=1,
        help="filename for the Dominion-style ballot CVR file",
    )
    args = parser.parse_args()

    keyfile = args.keys
    cvrfile = args.cvr_file[0]
    tallydir = validate_directory_input(args.tallies, "tally", error_if_exists=True)
    use_cluster = args.cluster
    should_verify_proofs = args.verify

    admin_state: Optional[ElectionAdmin] = make_file_ref_from_path(keyfile).read_json(
        ElectionAdmin
    )
    if admin_state is None or not admin_state.is_valid():
        print(f"Election administration key material wasn't valid")
        exit(1)

    print(f"Starting up, reading {cvrfile}")
    start_time = time.perf_counter()
    cvrs = read_dominion_csv(cvrfile)
    if cvrs is None:
        print(f"Failed to read {cvrfile}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape
    parse_time = time.perf_counter()
    print(
        f"    Parse time: {parse_time - start_time: .3f} sec, {rows / (parse_time - start_time):.3f} ballots/sec"
    )
    print(f"    Found {rows} CVRs in {cvrs.metadata.election_name}.")

    if use_cluster:
        ray_init_cluster()
        ray_wait_for_workers()
    else:
        ray_init_localhost()

    tally_start = time.perf_counter()
    rtally = ray_tally_everything(
        cvrs,
        verbose=False,
        secret_key=admin_state.keypair.secret_key,
        root_dir=tallydir,
        should_verify_proofs=should_verify_proofs,
    )
    tally_end = time.perf_counter()
    print(f"Tally rate:    {rows / (tally_end - tally_start): .3f} ballots/sec")

    num_failures = wait_for_zero_pending_writes()
    if num_failures > 0:
        print(f"WARNING: Failed to write {num_failures} files. Something bad happened.")

    if ray.is_initialized():
        ray.shutdown()
