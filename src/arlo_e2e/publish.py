from io import StringIO
from multiprocessing.pool import Pool
from typing import Optional, TypeVar, Tuple

import pandas as pd
from electionguard.election import (
    ElectionConstants,
    ElectionDescription,
    CiphertextElectionContext,
)
from electionguard.logs import log_error
from electionguard.serializable import set_deserializers, Serializable, set_serializers

from arlo_e2e.constants import (
    ELECTION_METADATA,
    CVR_METADATA,
    ELECTION_DESCRIPTION,
    ENCRYPTED_TALLY,
    CRYPTO_CONSTANTS,
    CRYPTO_CONTEXT,
)
from arlo_e2e.io import make_file_ref_from_path
from arlo_e2e.manifest import (
    load_existing_manifest,
    Manifest,
)
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.ray_tally import RayTallyEverythingResults
from arlo_e2e.tally import (
    FastTallyEverythingResults,
    SelectionTally,
    ballot_memos_from_metadata,
)

T = TypeVar("T")
U = TypeVar("U", bound=Serializable)


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

    if make_file_ref_from_path(results_dir).exists():
        manifest = load_existing_manifest(
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
                StringIO(cvr_metadata.decode("utf-8")),
                sep=",",
                engine="python",
            )
        except pd.errors.ParserError:
            return None

        return manifest, election_description, cec, encrypted_tally, metadata, df

    log_error(f"Path ({results_dir}) not found, cannot load the fast-tally")
    return None


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
