import argparse

import ray

from arlo_e2e.io import validate_directory_input, make_file_ref_from_path
from arlo_e2e.manifest import build_manifest_for_directory
from arlo_e2e.ray_helpers import (
    ray_init_localhost,
    ray_init_cluster,
    ray_wait_for_workers,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Recursively builds a Merkle hash tree in the requested directory"
    )
    parser.add_argument(
        "dir",
        type=str,
        nargs=1,
        default=None,
        help="directory to compute in (writes / overwrites MANIFEST.json files)",
    )
    parser.add_argument(
        "--cluster",
        action="store_true",
        help="uses a Ray cluster for distributed computation",
    )

    args = parser.parse_args()
    root_dir = validate_directory_input(args.dir[0], "tally", error_if_absent=True)
    root_dir_ref = make_file_ref_from_path(root_dir)
    use_cluster = args.cluster

    if use_cluster:
        ray_init_cluster()
        ray_wait_for_workers()
    else:
        ray_init_localhost()

    build_manifest_for_directory(
        root_dir_ref,
        show_progressbar=True,
        num_write_retries=1,
        logging_enabled=False,
        overwrite_existing_manifests=True,
    )

    ray.shutdown()
