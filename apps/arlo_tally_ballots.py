import argparse
from multiprocessing import Pool
from os import cpu_count
from timeit import default_timer as timer
from typing import Optional

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.admin import ElectionAdmin
from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.publish import write_fast_tally
from arlo_e2e.tally import fast_tally_everything
from arlo_e2e.utils import load_json_helper

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
        nargs=1,
        default="secret_election_keys.json",
        help="file name for the election official's key materials (default: secret_election_keys.json)",
    )
    parser.add_argument(
        "cvr_file",
        type=str,
        nargs=1,
        help="filename for the Dominion-style ballot CVR file",
    )
    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        nargs=1,
        default="tally_output",
        help="directory name for where the tally is written (default: tally_output)",
    )
    args = parser.parse_args()

    keyfile = args.keys
    cvrfile = args.cvr_file[0]
    tallydir = args.directory

    admin_state: Optional[ElectionAdmin] = load_json_helper(".", keyfile, ElectionAdmin)
    if admin_state is None or not admin_state.is_valid():
        print(f"Election administration key material wasn't valid")
        exit(1)

    cvrs = read_dominion_csv(cvrfile)
    if cvrs is None:
        print(f"Failed to read {cvrfile}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape
    print(f"Found {rows} CVRs in {cvrs.metadata.election_name}.")

    pool = Pool(cpu_count())

    tally_start = timer()
    tally = fast_tally_everything(
        cvrs, verbose=False, secret_key=admin_state.keypair.secret_key, pool=pool
    )
    tally_end = timer()
    print(f"Tally rate:    {rows / (tally_end - tally_start): .3f} ballots/sec")
    write_fast_tally(tally, tallydir)
    print(f"Tally written to {tallydir}")

    pool.close()

    exit(0)
