# This is a benchmark that runs as a standalone program. It takes one command-line argument: the name of a "CSV"
# file in Dominion format. It then parses and encrypts the whole thing, including computing the decrypted tallies.
import argparse
import multiprocessing
import shutil
from multiprocessing.pool import Pool
from os import cpu_count
from sys import exit
from timeit import default_timer as timer
from typing import Optional

import ray
from electionguard.elgamal import elgamal_keypair_from_secret
from electionguard.group import int_to_q_unchecked
from electionguard.logs import log_info
from electionguard.utils import get_optional

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.publish import write_fast_tally
from arlo_e2e.ray_helpers import ray_init_cluster
from arlo_e2e.ray_tally import ray_tally_everything
from arlo_e2e.tally import fast_tally_everything


def run_bench(filename: str, pool: Pool, file_dir: Optional[str]) -> None:
    start_time = timer()
    print(f"Benchmarking: {filename}")
    log_info(f"Benchmarking: {filename}")
    cvrs = read_dominion_csv(filename)
    if cvrs is None:
        print(f"Failed to read {filename}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape

    parse_time = timer()
    print(f"    Parse time: {parse_time - start_time: .3f} sec")

    assert rows > 0, "can't have zero ballots!"

    # doesn't matter what the key is, so long as it's consistent for both runs
    keypair = get_optional(elgamal_keypair_from_secret(int_to_q_unchecked(31337)))

    tally_start = timer()
    tally = fast_tally_everything(
        cvrs, pool, verbose=True, secret_key=keypair.secret_key
    )

    if file_dir:
        write_fast_tally(tally, file_dir + "_fast")

    tally_end = timer()
    assert tally.all_proofs_valid(verbose=True), "proof failure!"

    print(f"\nstarting ray.io parallelism")
    rtally_start = timer()
    rtally = ray_tally_everything(
        cvrs,
        secret_key=keypair.secret_key,
        root_dir=file_dir + "_ray" if file_dir else None,
    )
    rtally_end = timer()

    if file_dir:
        rtally_as_fast = rtally.to_fast_tally()
        assert rtally_as_fast.all_proofs_valid(verbose=True), "proof failure!"
        assert tally.equivalent(
            rtally_as_fast, keypair, pool
        ), "tallies aren't equivalent!"

        # Note: tally.equivalent() isn't quite as stringent as asserting that absolutely
        # everything is identical, but it's a pretty good sanity check for our purposes.
        # In tests/test_ray_tally.py, test_ray_and_multiprocessing_agree goes the extra
        # distance to create identical tallies from each system and assert their equality.

    print(f"\nOVERALL PERFORMANCE")
    print(f"    Pool time:   {tally_end - tally_start: .3f} sec")
    print(f"    Pool rate:   {rows / (tally_end - tally_start): .3f} ballots/sec")
    print(f"    Ray time:    {rtally_end - rtally_start : .3f} sec")
    print(f"    Ray rate:    {rows / (rtally_end - rtally_start): .3f} ballots/sec")

    print(
        f"    Ray speedup: {(tally_end - tally_start) / (rtally_end - rtally_start) : .3f} (>1.0 = ray is faster, <1.0 = ray is slower)"
    )

    if file_dir is not None:
        shutil.rmtree(file_dir + "_ray", ignore_errors=True)
        shutil.rmtree(file_dir + "_fast", ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmarks multiprocessing vs. Ray.io performance, encrypting a Dominion CSV file"
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
        help="filename for the Dominion-style ballot CVR file",
    )
    args = parser.parse_args()

    cvrfiles = args.cvr_file
    file_dir = args.dir[0] if args.dir else None

    print(f"CPUs detected: {cpu_count()}, launching thread pool")
    pool = multiprocessing.Pool(cpu_count())
    ray_init_cluster()

    for arg in cvrfiles:
        run_bench(arg, pool, file_dir)

    pool.close()
    ray.shutdown()
    exit(0)

# Benchmarking results: On a 6-core MacPro desktop, Ray pays a non-trivial penalty (5-10%) versus
# Multiprocessing. However, this is only with no IO. Once we have files being written to disk, Ray
# catches up, since it's doing concurrent writes, whereas our Multiprocessing implementation does
# the writes sequentially from a single process. On a big multicore server, Ray starts off slightly
# ahead, which could be an artifact of better NUMA memory behavior. And, of course, Ray allows us
# to run across big clusters, which we couldn't really do with Multiprocesing.
