# This is a version of encryption_bench, but only for running Ray remotely.

import sys
from sys import exit
from timeit import default_timer as timer

import ray
from electionguard.elgamal import elgamal_keypair_from_secret
from electionguard.group import int_to_q_unchecked
from electionguard.logs import log_info
from electionguard.utils import get_optional

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.ray_helpers import ray_init_cluster
from arlo_e2e.ray_tally import ray_tally_everything


def run_bench(filename: str) -> None:
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

    print(f"\nstarting ray.io parallelism")
    rtally_start = timer()
    rtally = ray_tally_everything(cvrs, secret_key=keypair.secret_key, verbose=True)
    rtally_end = timer()

    print(f"\nOVERALL PERFORMANCE")
    print(f"    Ray time:    {rtally_end - rtally_start : .3f} sec")
    print(f"    Ray rate:    {rows / (rtally_end - rtally_start): .3f} ballots/sec")

    print(f"\nSANITY CHECK")
    assert rtally.all_proofs_valid(
        verbose=True, recheck_ballots_and_tallies=True
    ), "proof failure!"

    # Note: tally.equivalent() isn't quite as stringent as asserting that absolutely
    # everything is identical, but it's a pretty good sanity check for our purposes.
    # In tests/test_ray_tally.py, test_ray_and_multiprocessing_agree goes the extra
    # distance to create identical tallies from each system and assert their equality.


if __name__ == "__main__":
    ray_init_cluster()

    for arg in sys.argv[1:]:
        run_bench(arg)

    ray.shutdown()
    exit(0)
