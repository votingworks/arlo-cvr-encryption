# This is a version of encryption_bench, but only for running Ray remotely.
import argparse
from sys import exit
import time
from typing import Optional

import ray
from electionguard.elgamal import elgamal_keypair_from_secret
from electionguard.group import int_to_q_unchecked
from electionguard.utils import get_optional

from arlo_cvre.dominion import read_dominion_csv
from arlo_cvre.io import wait_for_zero_pending_writes, validate_directory_input
from arlo_cvre.ray_helpers import ray_init_cluster, ray_init_localhost
from arlo_cvre.ray_tally import ray_tally_everything


def run_bench(filename: str, output_dir: Optional[str], use_progressbar: bool) -> None:
    start_time = time.perf_counter()
    print(f"Benchmarking: {filename}")
    cvrs = read_dominion_csv(filename)
    if cvrs is None:
        print(f"Failed to read {filename}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape

    parse_time = time.perf_counter()
    print(
        f"    Parse time: {parse_time - start_time: .3f} sec, {rows / (parse_time - start_time):.3f} ballots/sec"
    )

    assert rows > 0, "can't have zero ballots!"

    # doesn't matter what the key is, so long as it's consistent for both runs
    keypair = get_optional(elgamal_keypair_from_secret(int_to_q_unchecked(31337)))

    rtally_start = time.perf_counter()
    rtally = ray_tally_everything(
        cvrs,
        secret_key=keypair.secret_key,
        verbose=True,
        root_dir=output_dir,
        use_progressbar=use_progressbar,
    )
    rtally_end = time.perf_counter()

    print(f"\nOVERALL PERFORMANCE")
    print(f"    Ray time:    {rtally_end - rtally_start : .3f} sec")
    print(f"    Ray rate:    {rows / (rtally_end - rtally_start): .3f} ballots/sec")

    if output_dir:
        print(f"\nSANITY CHECK")
        assert rtally.all_proofs_valid(
            verbose=True,
            recheck_ballots_and_tallies=False,
            use_progressbar=use_progressbar,
        ), "proof failure!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs a tallying benchmark, using Ray (either locally or on a remote cluster)"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="uses Ray locally (Ray on a cluster by default)",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="displays a progress-bar as the job runs (off by default)",
    )
    parser.add_argument(
        "--dir",
        type=str,
        nargs=1,
        default=None,
        help="directory to store the encrypted ballots on disk, enables equivalence checking (default: memory only)",
    )
    parser.add_argument(
        "cvr_file",
        type=str,
        nargs="+",
        help="filename(s) for the Dominion-style ballot CVR file",
    )

    args = parser.parse_args()
    files = args.cvr_file
    file_dir = (
        validate_directory_input(args.dir[0], "tally", error_if_absent=True)
        if args.dir
        else None
    )
    use_progressbar = args.progress

    if args.local:
        print("Using Ray locally")
        ray_init_localhost()
    else:
        print("Using Ray on a cluster")
        ray_init_cluster()

    for arg in files:
        run_bench(arg, file_dir, use_progressbar)

    print("Writing Ray timelines to disk.")
    ray.timeline("ray-timeline.json")

    num_failures = wait_for_zero_pending_writes()
    if num_failures > 0:
        print(f"WARNING: Failed to write {num_failures} files. Something bad happened.")
