import argparse
from sys import exit

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.admin import make_fresh_election_admin, ElectionAdmin
from arlo_e2e.ray_io import ray_load_json_file, ray_write_json_file

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
        default="secret_election_keys.json",
        help="file name for where the information is written (default: secret_election_keys.json)",
    )
    args = parser.parse_args()

    # This ultimately bottoms out at secrets.randbelow(), which claims to be cryptographically strong.
    admin_state = make_fresh_election_admin()
    ray_write_json_file(root_dir=".", file_name=args.keys, content_obj=admin_state)

    # Read it back in, just to make sure we're all good.
    admin_state2 = ray_load_json_file(".", args.keys, ElectionAdmin)

    if admin_state2 != admin_state:
        print(f"Something went wrong writing to {args.keys}")
        exit(1)

    if not admin_state2.is_valid():
        print(f"Admin state wasn't valid (shouldn't ever happen!)")
        exit(1)

    print(f"Admin state written to {args.keys}")
