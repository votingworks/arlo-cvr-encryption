import argparse
from pathlib import PurePath
from sys import exit

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.admin import make_fresh_election_admin, ElectionAdmin
from arlo_e2e.utils import load_json_helper, write_json_helper

if __name__ == "__main__":
    set_serializers()
    set_deserializers()
    parser = argparse.ArgumentParser(
        description="Initialize the election public/private key material."
    )
    parser.add_argument(
        "-k",
        "--keys",
        type=str,
        nargs=1,
        default=["secret_election_keys.json"],
        help="file name for where the information is written (default: secret_election_keys.json)",
    )
    args = parser.parse_args()

    filename = PurePath(args.keys[0])

    # This ultimately bottoms out at secrets.randbelow(), which claims to be cryptographically strong.
    admin_state = make_fresh_election_admin()
    write_json_helper(".", filename, admin_state)

    # Read it back in, just to make sure we're all good.
    admin_state2 = load_json_helper(".", filename, ElectionAdmin)

    if admin_state2 != admin_state:
        print(f"Something went wrong writing to {filename}")
        exit(1)

    if not admin_state2.is_valid():
        print(f"Admin state wasn't valid (shouldn't ever happen!)")
        exit(1)

    print(f"Admin state written to {filename}")
