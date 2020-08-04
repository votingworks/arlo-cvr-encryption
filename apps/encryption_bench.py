# This is a benchmark that runs as a standalone program. It takes one command-line argument: the name of a "CSV"
# file in Dominion format. It then parses and encrypts the whole thing, including computing the decrypted tallies.
import multiprocessing
import sys
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from timeit import default_timer as timer

import ray
from electionguard.logs import log_info

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

    tally_start = timer()
    tally = fast_tally_everything(cvrs, pool, verbose=True)
    tally_end = timer()
    assert tally.all_proofs_valid(verbose=True), "proof failure!"

    print(f"starting ray.io parallelism")
    rtally_start = timer()
    rtally = ray_tally_everything(cvrs)
    rtally_end = timer()
    assert rtally.all_proofs_valid(verbose=True), "proof failure!"

    print(f"OVERALL PERFORMANCE")
    print(f"    Pool time:   {tally_end - tally_start: .3f} sec")
    print(f"    Pool rate:   {rows / (tally_end - tally_start): .3f} ballots/sec")
    print(f"    Ray time:    {rtally_end - rtally_start : .3f} sec")
    print(f"    Ray rate:    {rows / (rtally_end - rtally_start): .3f} ballots/sec")

    print(
        f"    Ray speedup: {(tally_end - tally_start) / (rtally_end - rtally_start) : .3f} (>1.0 = faster, <1.0 = slower)"
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

# Numbers from a 6-core machine for the encryption phase:
#   0.217 ballot/sec with no pool
#   1.239 ballot/sec with pool
# speedup = 5.71x
