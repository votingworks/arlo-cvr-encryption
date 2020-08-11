import argparse
from timeit import default_timer as timer

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.admin import ElectionAdmin
from arlo_e2e.dominion import read_dominion_csv
from arlo_e2e.publish import write_fast_tally
from arlo_e2e.ray_helpers import ray_init_localhost, ray_shutdown_localhost
from arlo_e2e.ray_tally import ray_tally_everything
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
        default=["secret_election_keys.json"],
        help="file name for the election official's key materials (default: secret_election_keys.json)",
    )
    parser.add_argument(
        "cvr_file",
        type=str,
        nargs=1,
        help="filename for the Dominion-style ballot CVR file",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        nargs=1,
        default=["tally_output"],
        help="directory name for where the tally is written (default: tally_output)",
    )
    args = parser.parse_args()

    keyfile = args.keys[0]
    cvrfile = args.cvr_file[0]
    tallydir = args.output[0]

    admin_state: ElectionAdmin = load_json_helper(".", keyfile, ElectionAdmin)
    if admin_state is None or not admin_state.is_valid():
        print(f"Election administration key material wasn't valid")
        exit(1)

    cvrs = read_dominion_csv(cvrfile)
    if cvrs is None:
        print(f"Failed to read {cvrfile}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape
    print(
        f"Found {rows} CVRs in data for {cvrs.metadata.election_name}. Running encryption!"
    )

    ray_init_localhost()
    rtally_start = timer()
    rtally = ray_tally_everything(cvrs, verbose=False, secret_key=admin_state.keypair.secret_key)
    rtally_end = timer()
    print(f"Tally rate:    {rows / (rtally_end - rtally_start): .3f} ballots/sec")
    ray_shutdown_localhost()
    write_fast_tally(rtally, tallydir)
    print(f"Tally written to {tallydir}")

    exit(0)