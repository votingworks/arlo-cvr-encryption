import argparse
from sys import exit
from typing import List, Dict, Optional

import ray
from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.constants import MANIFEST_FILE
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.html_index import generate_index_html_files
from arlo_e2e.io import validate_directory_input, make_file_ref_from_path
from arlo_e2e.publish import load_ray_tally
from arlo_e2e.ray_helpers import ray_init_localhost
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
        description="Writes out a file (root_hash.html) suitable for printing"
        " and handing out as the root hash of the election."
    )

    parser.add_argument(
        "--index_html",
        "--index-html",
        "-i",
        action="store_true",
        help="generates (or regenerates) the index.html files as well as the root hash QRcode",
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

    election_name: Optional[str] = args.election_name
    metadata_strs: List[str] = args.metadata
    gen_index_html: bool = args.index_html
    tally_dir = validate_directory_input(args.tallies, "tally", error_if_absent=True)
    tally_dir_ref = make_file_ref_from_path(tally_dir)

    ray_init_localhost()  # allows for concurrency when writing out the index.html files

    log_and_print("Loading election", verbose=True)

    if election_name is None or election_name == "":
        tally = load_ray_tally(
            tally_dir,
            check_proofs=False,
            verbose=False,
            recheck_ballots_and_tallies=False,
            root_hash=None,
        )

        if tally is not None:
            election_name = tally.metadata.election_name

        # perhaps better than just bombing out, but really this shouldn't happen
        if election_name is None or election_name == "":
            election_name = "General Election"

    metadata: Dict[str, str] = {}
    for s in metadata_strs:
        items = s.split("=")
        key = items[0].strip()  # remove blanks around keys

        # ='s might happen in the metadata, so we're going to rejoin
        value = "=".join(items[1:])
        metadata[key] = value

    if not (tally_dir_ref + MANIFEST_FILE).exists():
        print(
            f"No {MANIFEST_FILE} found in {str(tally_dir_ref)}, cannot generate root hash. Exiting."
        )
        exit(1)

    gen_root_qrcode(
        election_name=election_name,
        tally_dir_ref=tally_dir_ref,
        metadata=metadata,
        verbose=True,
    )

    if gen_index_html:
        generate_index_html_files(election_name, tally_dir_ref, verbose=True)

    ray.shutdown()
