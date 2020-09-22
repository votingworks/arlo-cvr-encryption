# This benchmark compares Pandas vs. Modin for crunching lots of CSVs.

import argparse
from sys import exit
from timeit import default_timer as timer

import ray

from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.ray_helpers import ray_init_localhost


def run_bench(filename: str) -> None:
    start_time = timer()
    print(f"Benchmarking: {filename}")
    cvrs = read_dominion_csv(filename)
    if cvrs is None:
        print(f"Failed to read {filename}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape

    parse_time = timer()
    print(
        f"    Parse time: {parse_time - start_time: .3f} sec, {rows / (parse_time - start_time): .3f} ballots/sec"
    )

    assert rows > 0, "can't have zero ballots!"

    ed, pballots, info = cvrs.to_election_description()
    assert len(pballots) == rows, "got wrong number of plaintext ballots!"
    eg_time = timer()
    print(
        f"    EG setup time (scalar): {eg_time - parse_time: .3f} sec, {rows / (eg_time - parse_time): .3f} ballots/sec"
    )

    edr, pballots_refs, infor = cvrs.to_election_description_ray()
    pballotsr = ray.get(pballots_refs)
    assert len(pballotsr) == rows, "got wrong number of plaintext ballots!"
    egr_time = timer()
    print(
        f"    EG setup time (Ray): {egr_time - eg_time: .3f} sec, {rows / (egr_time - eg_time): .3f} ballots/sec"
    )

    assert pballots == pballots, "got mismatching ballots"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmarks multiprocessing vs. Ray.io performance, encrypting a Dominion CSV file"
    )
    parser.add_argument(
        "cvr_file",
        type=str,
        nargs="+",
        help="filename for the Dominion-style ballot CVR file",
    )
    args = parser.parse_args()

    cvrfiles = args.cvr_file

    print("Launching Ray")
    ray_init_localhost()

    print("Starting benchmarks")
    for arg in cvrfiles:
        run_bench(arg)
