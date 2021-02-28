import argparse
from os import path
from sys import exit
from timeit import default_timer as timer
from typing import Optional

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.admin import ElectionAdmin
from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.publish import write_ray_tally
from arlo_e2e.ray_helpers import (
    ray_init_cluster,
    ray_init_localhost,
    ray_wait_for_workers,
)
from arlo_e2e.ray_tally import ray_tally_everything
from arlo_e2e.ray_io import wait_for_zero_pending_writes, ray_load_json_file

if __name__ == "__main__":
    set_serializers()
    set_deserializers()

    parser = argparse.ArgumentParser(
        description="Load a Dominion-style ballot CVR file and write out an Arlo-e2e tally"
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
        "cvr_file",
        type=str,
        nargs=1,
        help="filename for the Dominion-style ballot CVR file",
    )
    args = parser.parse_args()

    keyfile = args.keys
    cvrfile = args.cvr_file[0]
    tallydir = args.tallies
    use_cluster = args.cluster

    if path.exists(tallydir):
        print(f"Tally directory ({tallydir}) already exists. Exiting.")
        exit(1)

    admin_state: Optional[ElectionAdmin] = ray_load_json_file(
        ".", keyfile, ElectionAdmin
    )
    if admin_state is None or not admin_state.is_valid():
        print(f"Election administration key material wasn't valid")
        exit(1)

    print(f"Starting up, reading {cvrfile}")
    start_time = timer()
    cvrs = read_dominion_csv(cvrfile)
    if cvrs is None:
        print(f"Failed to read {cvrfile}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape
    parse_time = timer()
    print(
        f"    Parse time: {parse_time - start_time: .3f} sec, {rows / (parse_time - start_time):.3f} ballots/sec"
    )
    print(f"    Found {rows} CVRs in {cvrs.metadata.election_name}.")

    if use_cluster:
        ray_init_cluster()
        ray_wait_for_workers()
    else:
        ray_init_localhost()

    tally_start = timer()
    rtally = ray_tally_everything(
        cvrs,
        verbose=False,
        secret_key=admin_state.keypair.secret_key,
        root_dir=tallydir,
    )
    tally_end = timer()
    print(f"Tally rate:    {rows / (tally_end - tally_start): .3f} ballots/sec")
    write_ray_tally(rtally, tallydir)
    print(f"Tally written to {tallydir}")

    num_failures = wait_for_zero_pending_writes()
    if num_failures > 0:
        print(f"WARNING: Failed to write {num_failures} files. Something bad happened.")
