import csv
from multiprocessing.pool import Pool
from os import path
from io import StringIO
from pathlib import PurePath
from typing import Final, Optional, TypeVar, List, Dict, Tuple, Sequence

import ray
import pandas as pd
from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.election import (
    ElectionConstants,
    ElectionDescription,
    CiphertextElectionContext,
)
from electionguard.logs import log_error, log_info
from electionguard.serializable import set_deserializers, Serializable, set_serializers

from arlo_e2e.html_index import generate_index_html_files
from arlo_e2e.manifest import (
    make_fresh_manifest,
    make_existing_manifest,
    ManifestFileWriteSpec,
    Manifest,
)
from arlo_e2e.memo import Memo, make_memo_lambda
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.ray_tally import RayTallyEverythingResults, ballots_per_shard
from arlo_e2e.tally import FastTallyEverythingResults, SelectionTally
from arlo_e2e.utils import all_files_in_directory, mkdir_helper, shard_list, flatmap

T = TypeVar("T")
U = TypeVar("U", bound=Serializable)

ELECTION_METADATA: Final[str] = "election_metadata.json"
CVR_METADATA: Final[str] = "cvr_metadata.csv"
ELECTION_DESCRIPTION: Final[str] = "election_description.json"
ENCRYPTED_TALLY: Final[str] = "encrypted_tally.json"
CRYPTO_CONSTANTS: Final[str] = "constants.json"
CRYPTO_CONTEXT: Final[str] = "cryptographic_context.json"


# TODO: this needs to be reworked, probably completely removed from publish.py
#   and worked into ray_tally.py.
@ray.remote
def _r_ballot_to_manifest_write_spec(
    ballot: CiphertextAcceptedBallot,
) -> ManifestFileWriteSpec:  # pragma: no cover
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


# TODO: this can't be private anymore: it's going to be called from ray_tally.py
def _write_tally_shared(
    results_dir: str,
    election_description: ElectionDescription,
    context: CiphertextElectionContext,
    constants: ElectionConstants,
    tally: SelectionTally,
    metadata: ElectionMetadata,
    cvr_metadata: pd.DataFrame,
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

    log_info("_write_tally_shared: writing cvr metadata")
    manifest.write_file(
        CVR_METADATA, cvr_metadata.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC)
    )

    return manifest


def write_fast_tally(results: FastTallyEverythingResults, results_dir: str) -> Manifest:
    """
    Writes out a directory with the full contents of the tally structure. Each ciphertext ballot
    will end up in its own file. Everything is JSON. Returns a `Manifest` object that reflects
    everything that was written.
    """
    manifest = _write_tally_shared(
        results_dir,
        results.election_description,
        results.context,
        ElectionConstants(),
        results.tally,
        results.metadata,
        results.cvr_metadata,
    )

    log_info("write_fast_tally: writing ballots")

    for ballot in results.encrypted_ballots:
        # See comment in _r_ballot_to_manifest_write_spec for more on what's going on here.
        ballot_name = ballot.object_id
        ballot_name_prefix = ballot_name[0:4]
        manifest.write_json_file(
            ballot_name + ".json", ballot, ["ballots", ballot_name_prefix]
        )

    log_info("write_fast_tally: writing MANIFEST.json")
    manifest.write_manifest()
    generate_index_html_files(results.metadata.election_name, results_dir)

    return manifest


# TODO: this will go away, get merged in with ray_tally_everything.
def write_ray_tally(results: RayTallyEverythingResults, results_dir: str) -> Manifest:
    """
    Writes out a directory with the full contents of the tally structure. Each ciphertext ballot
    will end up in its own file. Everything is JSON. Returns a `Manifest` object that reflects
    everything that was written.

    This method is explicitly engineered around the use of Ray.io for cluster computing. We expect
    to have encrypted ballots spread across a huge cluster. Our assumption is that every node has
    access to a common filesystem, perhaps mounted via NFS or SMB, so that we'll have every node
    writing its local ballots out but we'll get a shared result in a single location.

    All the file hash information, necessary to write out `MANIFEST.json` is collected back to
    the main compute node and written from there.
    """
    manifest = _write_tally_shared(
        results_dir,
        results.election_description,
        results.context,
        ElectionConstants(),
        results.tally,
        results.metadata,
        results.cvr_metadata,
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
    generate_index_html_files(results.metadata.election_name, results_dir)

    return manifest


def _load_tally_shared(
    results_dir: str,
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


@ray.remote
def r_load_ballots(
    m: Manifest, filenames: Sequence[PurePath]
) -> Sequence[ray.ObjectRef]:  # pragma: no cover
    """
    Given a list of filenames, loads them, returning a sequence of ray ObjectRef's
    to CiphertextBallots.
    """
    return [
        ray.put(m.read_json_file(f, CiphertextAcceptedBallot))
        for f in filenames
        if f.name != "index.html"
    ]


def load_ray_tally(
    results_dir: str,
    check_proofs: bool = True,
    verbose: bool = False,
    recheck_ballots_and_tallies: bool = False,
) -> Optional[RayTallyEverythingResults]:
    """
    Given the directory name / path-name to a disk representation of a fast-tally structure, this reads
    it back in, makes sure it's well-formed, and optionally checks the cryptographic proofs. If any
    checks fail, `None` is returned. Errors are logged. This is executed across a Ray cluster, resulting
    in significant speedups, as well as having the ballot ciphertexts, themselves, spread across the
    cluster, for improved concurrency later on.
    """

    result = _load_tally_shared(results_dir)
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

    ballots_dir = path.join(results_dir, "ballots")
    ballot_files: List[PurePath] = all_files_in_directory(ballots_dir)

    # We're going to load the ballots across the whole cluster, using a sharding
    # strategy that will make the first round of redoing the tally cheap.
    bps = ballots_per_shard(len(ballot_files))
    sharded_ballot_files: Sequence[Sequence[PurePath]] = shard_list(ballot_files, bps)

    # TODO: shard the manifest itself, so we don't have to copy the whole thing to
    #   every node when we're only using a fraction of it. For a million ballots,
    #   the manifest will have maybe 100 bytes per ballot, so we're asking the
    #   poor database to shove 100MB to every node in the cluster. Fun argument:
    #   we gain a significant savings here by using VMs with more CPU cores per VM,
    #   because we'll only need to do the replication once per VM.
    r_manifest = ray.put(manifest)

    # While we're "unsharding" the references before storing in the results,
    # we'll re-shard them again as part of the verification, and they'll end
    # up exactly the same.

    # TODO: this all needs to be redone, because we don't want to keep the ballots
    #   in memory. Instead, we'll set up the RayTallyEverything structure with
    #   the filenames, and the all_proofs_valid method will do the loading.
    sharded_cballots_refs: Sequence[Sequence[ray.ObjectRef]] = ray.get(
        [r_load_ballots.remote(r_manifest, files) for files in sharded_ballot_files]
    )
    flatter_cballot_refs: List[ray.ObjectRef] = list(
        flatmap(lambda x: x, sharded_cballots_refs)
    )

    everything = RayTallyEverythingResults(
        metadata,
        cvr_metadata,
        election_description,
        encrypted_tally,
        cec,
        flatter_cballot_refs,
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
) -> Optional[FastTallyEverythingResults]:
    """
    Given the directory name / path-name to a disk representation of a fast-tally structure, this reads
    it back in, makes sure it's well-formed, and optionally checks the cryptographic proofs. If any
    checks fail, `None` is returned. Errors are logged. Optional `pool` allows for some parallelism
    in the verification process.
    """

    result = _load_tally_shared(results_dir)
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

    ballots_dir = path.join(results_dir, "ballots")
    ballot_files: List[PurePath] = [
        f for f in all_files_in_directory(ballots_dir) if f.name != "index.html"
    ]

    # What's with the nested lambdas? Python lambdas aren't real closures. This is a workaround.
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
        metadata,
        cvr_metadata,
        election_description,
        encrypted_ballot_memos,
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
