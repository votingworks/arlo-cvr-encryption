# This is a benchmark that runs as a standalone program. It takes one command-line argument: the name of a "CSV"
# file in Dominion format. It then parses and encrypts the whole thing, including computing the decrypted tallies.
import multiprocessing
import sys
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from timeit import default_timer as timer

from electionguard.logs import log_info

from dominion import read_dominion_csv
from eg_tally import fast_tally_everything


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

    tally = fast_tally_everything(cvrs, pool, verbose=True)
    assert tally.all_proofs_valid(verbose=True), "proof failure!"


if __name__ == "__main__":
    print(f"CPUs detected: {cpu_count()}, launching thread pool")
    pool = multiprocessing.Pool(cpu_count())

    for arg in sys.argv[1:]:
        run_bench(arg, pool)

    pool.close()
    exit(0)

# Numbers from a 6-core machine for the encryption phase:
#   0.217 ballot/sec with no pool
#   1.239 ballot/sec with pool
# speedup = 5.71x
