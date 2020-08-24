from multiprocessing.pool import Pool
from os import path
from pathlib import PurePath
from typing import Final, Optional, TypeVar, List, Dict

import ray
from arlo_e2e.manifest import (
    make_fresh_manifest,
    make_existing_manifest,
    ManifestFileWriteSpec,
    Manifest,
)
from arlo_e2e.memo import Memo, make_memo_lambda
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.ray_tally import RayTallyEverythingResults
from arlo_e2e.tally import FastTallyEverythingResults, SelectionTally
from arlo_e2e.utils import all_files_in_directory, mkdir_helper
from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.election import (
    ElectionConstants,
    ElectionDescription,
    CiphertextElectionContext,
)
from electionguard.logs import log_error, log_info
from electionguard.serializable import set_deserializers, Serializable, set_serializers

T = TypeVar("T")
U = TypeVar("U", bound=Serializable)

ELECTION_METADATA: Final[str] = "election_metadata.json"
ELECTION_DESCRIPTION: Final[str] = "election_description.json"
ENCRYPTED_TALLY: Final[str] = "encrypted_tally.json"
CRYPTO_CONSTANTS: Final[str] = "constants.json"
CRYPTO_CONTEXT: Final[str] = "cryptographic_context.json"


@ray.remote
def _r_ballot_to_manifest_write_spec(
    ballot: CiphertextAcceptedBallot,
) -> ManifestFileWriteSpec:
    # This prefix stuff: ballot uids are encoded as 'b' plus a 7-digit number.
    # Ballot #3 should be 'b0000003'. By taking the first 4 digits, and making
    # that into a directory, we get a max of 10,000 files per directory, which
    # should be good enough. We never want to type 'ls' in a directory and have
    # it wedge because it's trying to digest a million filenmes.

    ballot_name = ballot.object_id
    ballot_name_prefix = ballot_name[0:4]  # letter b plus first three digits

    return ManifestFileWriteSpec(
        file_name=ballot_name + ".json",
        content=ballot,
        subdirectories=["ballots", ballot_name_prefix],
    )


def _write_tally_shared(
    results_dir: str,
    election_description: ElectionDescription,
    context: CiphertextElectionContext,
    constants: ElectionConstants,
    tally: SelectionTally,
    metadata: ElectionMetadata,
) -> Manifest:
    set_serializers()
    set_deserializers()

    results_dir = results_dir
    log_info("_write_tally_shared: starting!")
    mkdir_helper(results_dir)

    manifest = make_fresh_manifest(results_dir)

    log_info("_write_tally_shared: writing election_description")
    manifest.write_json_file(ELECTION_DESCRIPTION, election_description)

    log_info("_write_tally_shared: writing crypto context")
    manifest.write_json_file(CRYPTO_CONTEXT, context)

    log_info("_write_tally_shared: writing crypto constants")
    manifest.write_json_file(CRYPTO_CONSTANTS, constants)

    log_info("_write_tally_shared: writing tally")
    manifest.write_json_file(ENCRYPTED_TALLY, tally)

    log_info("_write_tally_shared: writing metadata")
    manifest.write_json_file(ELECTION_METADATA, metadata)

    return manifest


def write_fast_tally(results: FastTallyEverythingResults, results_dir: str) -> None:
    """
    Writes out a directory with the full contents of the tally structure. Each ciphertext ballot
    will end up in its own file. Everything is JSON.
    """
    manifest = _write_tally_shared(
        results_dir,
        results.election_description,
        results.context,
        ElectionConstants(),
        results.tally,
        results.metadata,
    )

    log_info("write_fast_tally: writing ballots")

    for ballot in results.encrypted_ballots:
        # See comment in _r_ballot_to_manifest_write_specs.
        ballot_name = ballot.object_id
        ballot_name_prefix = ballot_name[0:4]
        manifest.write_json_file(
            ballot_name + ".json", ballot, ["ballots", ballot_name_prefix]
        )

    log_info("write_fast_tally: writing MANIFEST.json")
    manifest.write_manifest()


def write_ray_tally(results: RayTallyEverythingResults, results_dir: str) -> None:
    """
    Writes out a directory with the full contents of the tally structure. Each ciphertext ballot
    will end up in its own file. Everything is JSON.
    """
    manifest = _write_tally_shared(
        results_dir,
        results.election_description,
        results.context,
        ElectionConstants(),
        results.tally,
        results.metadata,
    )

    log_info("write_ray_tally: writing ballots")

    assert results.remote_encrypted_ballot_refs is not None, "got no remote ballot refs"

    manifest_specs = [
        _r_ballot_to_manifest_write_spec.remote(ballot)
        for ballot in results.remote_encrypted_ballot_refs
    ]
    manifest.write_remote_json_files(manifest_specs)

    log_info("write_ray_tally: writing MANIFEST.json")
    manifest.write_manifest()


def load_fast_tally(
    results_dir: str,
    check_proofs: bool = True,
    pool: Optional[Pool] = None,
    verbose: bool = False,
) -> Optional[FastTallyEverythingResults]:
    """
    Given the directory name / path-name to a disk representation of a fast-tally structure, this reads
    it back in, makes sure it's well-formed, and optionally checks the cryptographic proofs. If any
    checks fail, `None` is returned. Errors are logged. Optional `pool` allows for some parallelism
    in the verification process.
    """
    set_serializers()
    set_deserializers()

    if not path.exists(results_dir):
        log_error(f"Path ({results_dir}) not found, cannot load the fast-tally")
        return None

    manifest = make_existing_manifest(results_dir)
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

    ballots_dir = path.join(results_dir, "ballots")
    ballot_files: List[PurePath] = all_files_in_directory(ballots_dir)

    # What's with the nexted lambdas? Python lambdas aren't real closures. This is a workaround.
    # https://louisabraham.github.io/articles/python-lambda-closures.html
    encrypted_ballot_memos: Dict[str, Memo[CiphertextAcceptedBallot]] = {
        filename.stem: make_memo_lambda(
            (lambda f, m: lambda: m.read_json_file(f, CiphertextAcceptedBallot))(
                filename, manifest
            )
        )
        for filename in ballot_files
    }

    everything = FastTallyEverythingResults(
        metadata, election_description, encrypted_ballot_memos, encrypted_tally, cec,
    )

    if check_proofs:
        proofs_good = everything.all_proofs_valid(pool, verbose, True)
        if not proofs_good:
            # we don't need to log errors here; that will have happened internally
            return None

    return everything
