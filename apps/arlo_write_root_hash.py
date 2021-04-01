import argparse
from os import path
from sys import exit
from typing import List, Dict

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.constants import MANIFEST_FILE
from arlo_e2e.io import validate_directory_input
from arlo_e2e.root_qrcode import gen_root_qrcode

# Typical usage, shown with data for Inyo County, 2020 (with arguments split across lines for legibility,
# but it's all one Unix command-line):

# python apps/arlo_write_root_hash.py
#   --election_name 'Inyo County, CA: November 2020'
#   -t inyo2020
#   s3_host=amazonaws.com
#   s3_bucket=arlo-e2e-denver-demo
#   s3_region=us-east-2
#   s3_directory=inyo2020
#   s3_directory_decrypted=inyo2020-d
#   web_prefix=http://arlo-e2e-denver-demo.s3-website.us-east-2.amazonaws.com/inyo2020/
#   web_prefix_decrypted=http://arlo-e2e-denver-demo.s3-website.us-east-2.amazonaws.com/inyo2020-d/


if __name__ == "__main__":
    set_serializers()
    set_deserializers()

    parser = argparse.ArgumentParser(
        description="Writes out a file (root_hash.html) suitable for printing and handing out as the root hash of the election."
    )

    parser.add_argument(
        "--election_name",
        "--election-name",
        "-n",
        type=str,
        help="Human-readable name of the election, e.g., `Harris County General Election, November 2020`",
    )
    parser.add_argument(
        "-t",
        "--tallies",
        type=str,
        default="tally_output",
        help="directory name for where the tally and MANIFEST.json are written (default: tally_output)",
    )

    # inspiration: https://stackoverflow.com/a/52014520/4048276
    parser.add_argument(
        "metadata",
        metavar="KEY=VALUE",
        nargs="*",
        help="Set a number of metadata key-value pairs "
        "(do not put spaces before or after the = sign). "
        "If a value contains spaces, you should define "
        "it with double quotes: "
        'foo="this is a sentence". Note that '
        "values are always treated as strings.",
    )

    args = parser.parse_args()

    election_name: str = args.election_name
    tally_dir: str = args.tallies
    metadata_strs: List[str] = args.metadata

    tally_dir = validate_directory_input(tally_dir, "tally", error_if_absent=True)

    metadata: Dict[str, str] = {}
    for s in metadata_strs:
        items = s.split("=")
        key = items[0].strip()  # remove blanks around keys

        # ='s might happen in the metadata, so we're going to rejoin
        value = "=".join(items[1:])
        metadata[key] = value

    if not path.exists(tally_dir):
        print(f"Local root directory ({tally_dir}) not found. Exiting.")
        exit(1)

    if not path.exists(path.join(tally_dir, MANIFEST_FILE)):
        print(
            f"No MANIFEST.json found in {tally_dir}, cannot generate root hash. Exiting."
        )
        exit(1)

    gen_root_qrcode(election_name=election_name, tally_dir=tally_dir, metadata=metadata)
