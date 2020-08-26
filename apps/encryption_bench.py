# This is a benchmark that runs as a standalone program. It takes one command-line argument: the name of a "CSV"
# file in Dominion format. It then parses and encrypts the whole thing, including computing the decrypted tallies.
import multiprocessing
import sys
from os import cpu_count
from multiprocessing.pool import Pool
from timeit import default_timer as timer
from sys import exit

import ray
from electionguard.elgamal import elgamal_keypair_from_secret
from electionguard.group import int_to_q_unchecked
from electionguard.logs import log_info
from electionguard.utils import get_optional

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.ray_tally import ray_tally_everything
from arlo_e2e.tally import fast_tally_everything


def run_bench(filename: str, pool: Pool) -> None:
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

    _, ballots, _ = cvrs.to_election_description()
    assert len(ballots) > 0, "can't have zero ballots!"

    # doesn't matter what the key is, so long as it's consistent for both runs
    keypair = get_optional(elgamal_keypair_from_secret(int_to_q_unchecked(31337)))

    tally_start = timer()
    tally = fast_tally_everything(
        cvrs, pool, verbose=True, secret_key=keypair.secret_key
    )
    tally_end = timer()
    assert tally.all_proofs_valid(verbose=True), "proof failure!"

    print(f"\nstarting ray.io parallelism")
    rtally_start = timer()
    rtally = ray_tally_everything(cvrs, secret_key=keypair.secret_key)
    rtally_end = timer()

    rtally_as_fast = rtally.to_fast_tally()
    assert rtally_as_fast.all_proofs_valid(verbose=True), "proof failure!"
    assert tally.equivalent(rtally_as_fast, keypair), "tallies aren't equivalent!"

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


if __name__ == "__main__":
    print(f"CPUs detected: {cpu_count()}, launching thread pool")
    pool = multiprocessing.Pool(cpu_count())
    ray_init_localhost()

    for arg in sys.argv[1:]:
        run_bench(arg, pool)

    pool.close()
    ray.shutdown()
    exit(0)

# Numbers from a 6-core machine for the encryption phase of a dataset with 58 rows:
#   1.395 ballot/sec with pool
#   1.212 ballots/sec with ray.io

# It's unclear exactly why Ray is slower. This might be related to our sharding strategy.
# The real test of Ray is how well it behaves on giant clusters, so getting "close enough"
# on a single computer is good enough for now.
