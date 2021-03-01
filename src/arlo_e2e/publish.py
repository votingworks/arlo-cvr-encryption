import csv
from io import StringIO
from multiprocessing.pool import Pool
from os import path
from typing import Final, Optional, TypeVar, Tuple

import pandas as pd
from electionguard.election import (
    ElectionConstants,
    ElectionDescription,
    CiphertextElectionContext,
)
from electionguard.logs import log_error, log_info
from electionguard.serializable import set_deserializers, Serializable, set_serializers
from electionguard.utils import flatmap_optional

from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.html_index import generate_index_html_files
from arlo_e2e.manifest import (
    make_existing_manifest,
    Manifest,
    build_manifest_for_directory,
)
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.ray_io import (
    mkdir_helper,
    ray_write_json_file,
    ray_write_file,
    ray_write_ciphertext_ballot,
)
from arlo_e2e.ray_tally import RayTallyEverythingResults, NUM_WRITE_RETRIES
from arlo_e2e.tally import (
    FastTallyEverythingResults,
    SelectionTally,
    ballot_memos_from_metadata,
)

T = TypeVar("T")
U = TypeVar("U", bound=Serializable)

ELECTION_METADATA: Final[str] = "election_metadata.json"
CVR_METADATA: Final[str] = "cvr_metadata.csv"
ELECTION_DESCRIPTION: Final[str] = "election_description.json"
ENCRYPTED_TALLY: Final[str] = "encrypted_tally.json"
CRYPTO_CONSTANTS: Final[str] = "constants.json"
CRYPTO_CONTEXT: Final[str] = "cryptographic_context.json"
MANIFEST_FILE: Final[str] = "MANIFEST.json"


def _write_tally_shared(
    results_dir: str,
    election_description: ElectionDescription,
    context: CiphertextElectionContext,
    constants: ElectionConstants,
    tally: SelectionTally,
    metadata: ElectionMetadata,
    cvr_metadata: pd.DataFrame,
    num_retries: int = 1,
) -> None:
    set_serializers()
    set_deserializers()

    results_dir = results_dir
    log_info("_write_tally_shared: starting!")
    mkdir_helper(results_dir, num_retries=num_retries)

    log_info("_write_tally_shared: writing election_description")
    ray_write_json_file(
        ELECTION_DESCRIPTION, election_description, num_retries=num_retries
    )

    log_info("_write_tally_shared: writing crypto context")
    ray_write_json_file(CRYPTO_CONTEXT, context, num_retries=num_retries)

    log_info("_write_tally_shared: writing crypto constants")
    ray_write_json_file(CRYPTO_CONSTANTS, constants, num_retries=num_retries)

    log_info("_write_tally_shared: writing tally")
    ray_write_json_file(ENCRYPTED_TALLY, tally, num_retries=num_retries)

    log_info("_write_tally_shared: writing metadata")
    ray_write_json_file(ELECTION_METADATA, metadata, num_retries=num_retries)

    log_info("_write_tally_shared: writing cvr metadata")
    ray_write_file(
        CVR_METADATA,
        cvr_metadata.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC),
        num_retries=num_retries,
    )


def write_fast_tally(
    results: FastTallyEverythingResults, results_dir: str
) -> Optional[Manifest]:
    """
    Writes out a directory with the full contents of the tally structure. Each ciphertext ballot
    will end up in its own file. Everything is JSON. Returns a `Manifest` object that reflects
    everything that was written.
    """
    _write_tally_shared(
        results_dir,
        results.election_description,
        results.context,
        ElectionConstants(),
        results.tally,
        results.metadata,
        results.cvr_metadata,
    )

    log_info("Writing ballots")

    for ballot in results.encrypted_ballots:
        ray_write_ciphertext_ballot(ballot)

    log_info("Writing manifests")

    root_hash = build_manifest_for_directory(results_dir, [], True, 1)
    manifest = flatmap_optional(
        root_hash, lambda h: make_existing_manifest(results_dir, [], h)
    )

    generate_index_html_files(results.metadata.election_name, results_dir)

    return manifest


def write_ray_tally(
    results: RayTallyEverythingResults,
    results_dir: str,
) -> Optional[Manifest]:
    """
    Writes out a directory with the full contents of the tally structure. Basically everything
    except for the ballots themselves. Everything is JSON. Returns a `Manifest` object that reflects
    everything that was written. Assumes that ciphertext ballots were already written.
    """
    log_and_print("Writing final tally and metadata to storage.")
    _write_tally_shared(
        results_dir,
        results.election_description,
        results.context,
        ElectionConstants(),
        results.tally,
        results.metadata,
        results.cvr_metadata,
        num_retries=NUM_WRITE_RETRIES,
    )

    log_and_print("Writing manifests")
    root_hash = build_manifest_for_directory(results_dir, [], True, 1)
    manifest = flatmap_optional(
        root_hash, lambda h: make_existing_manifest(results_dir, [], h)
    )

    generate_index_html_files(
        results.metadata.election_name, results_dir, num_retries=NUM_WRITE_RETRIES
    )

    return manifest


def _load_tally_shared(
    results_dir: str, root_hash: Optional[str]
) -> Optional[
    Tuple[
        Manifest,
        ElectionDescription,
        CiphertextElectionContext,
        SelectionTally,
        ElectionMetadata,
        pd.DataFrame,
    ]
]:
    # Engineering grumble: if ever there was an argument in favor of monadic error handling
    # code, it's the absence of it in the code below. We could wrap this in a try/except
    # block, except none of these things raise exceptions, they just return None. We could
    # use a deeply nested set of calls to flatmap_optional, each defining a new lambda,
    # but that's pretty ugly as well. So what do we do? All of these checks for None.

    set_serializers()
    set_deserializers()

    if not path.exists(results_dir):
        log_error(f"Path ({results_dir}) not found, cannot load the fast-tally")
        return None

    manifest = make_existing_manifest(
        results_dir, subdirectories=None, expected_root_hash=root_hash
    )
    if manifest is None:
        return None

    election_description: Optional[ElectionDescription] = manifest.read_json_file(
        ELECTION_DESCRIPTION, ElectionDescription
    )
    if election_description is None:
        return None

    constants: Optional[ElectionConstants] = manifest.read_json_file(
        CRYPTO_CONSTANTS, ElectionConstants
    )
    if constants is None:
        return None
    if constants != ElectionConstants():
        log_error(
            f"constants are out of date or otherwise don't match the current library: {constants}"
        )
        return None

    cec: Optional[CiphertextElectionContext] = manifest.read_json_file(
        CRYPTO_CONTEXT, CiphertextElectionContext
    )
    if cec is None:
        return None

    encrypted_tally: Optional[SelectionTally] = manifest.read_json_file(
        ENCRYPTED_TALLY, SelectionTally
    )
    if encrypted_tally is None:
        return None

    metadata: Optional[ElectionMetadata] = manifest.read_json_file(
        ELECTION_METADATA, ElectionMetadata
    )
    if metadata is None:
        return None

    cvr_metadata = manifest.read_file(CVR_METADATA)
    if cvr_metadata is None:
        return None

    try:
        df = pd.read_csv(
            StringIO(cvr_metadata),
            sep=",",
            engine="python",
        )
    except pd.errors.ParserError:
        return None

    return manifest, election_description, cec, encrypted_tally, metadata, df


def load_ray_tally(
    results_dir: str,
    check_proofs: bool = True,
    verbose: bool = False,
    recheck_ballots_and_tallies: bool = False,
    root_hash: Optional[str] = None,
) -> Optional[RayTallyEverythingResults]:
    """
    Given the directory name / path-name to a disk representation of a fast-tally structure, this reads
    it back in, makes sure it's well-formed, and optionally checks the cryptographic proofs. If any
    checks fail, `None` is returned. Errors are logged. This is executed across a Ray cluster, resulting
    in significant speedups, as well as having the ballot ciphertexts, themselves, spread across the
    cluster, for improved concurrency later on.
    """

    result = _load_tally_shared(results_dir, root_hash)
    if result is None:
        return None

    (
        manifest,
        election_description,
        cec,
        encrypted_tally,
        metadata,
        cvr_metadata,
    ) = result

    everything = RayTallyEverythingResults(
        metadata,
        cvr_metadata,
        election_description,
        encrypted_tally,
        cec,
        manifest,
        len(cvr_metadata),
    )

    if check_proofs:
        proofs_good = everything.all_proofs_valid(verbose, recheck_ballots_and_tallies)
        if not proofs_good:
            # we don't need to log errors here; that will have happened internally
            return None

    return everything


def load_fast_tally(
    results_dir: str,
    check_proofs: bool = True,
    pool: Optional[Pool] = None,
    verbose: bool = False,
    recheck_ballots_and_tallies: bool = False,
    root_hash: Optional[str] = None,
) -> Optional[FastTallyEverythingResults]:
    """
    Given the directory name / path-name to a disk representation of a fast-tally structure, this reads
    it back in, makes sure it's well-formed, and optionally checks the cryptographic proofs. If any
    checks fail, `None` is returned. Errors are logged. Optional `pool` allows for some parallelism
    in the verification process.
    """

    result = _load_tally_shared(results_dir, root_hash)
    if result is None:
        return None

    (
        manifest,
        election_description,
        cec,
        encrypted_tally,
        metadata,
        cvr_metadata,
    ) = result

    ballot_memos = ballot_memos_from_metadata(cvr_metadata, manifest)

    everything = FastTallyEverythingResults(
        metadata,
        cvr_metadata,
        election_description,
        ballot_memos,
        encrypted_tally,
        cec,
    )

    if check_proofs:
        proofs_good = everything.all_proofs_valid(
            pool, verbose, recheck_ballots_and_tallies
        )
        if not proofs_good:
            # we don't need to log errors here; that will have happened internally
            return None

    return everything
