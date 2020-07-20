import json
import os
from multiprocessing.pool import Pool
from os import path, mkdir
from typing import Final, Optional, TypeVar, List, Type, cast

from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.election import (
    ElectionConstants,
    ElectionDescription,
    CiphertextElectionContext,
)
from electionguard.logs import log_error, log_info
from electionguard.publish import set_serializers
from electionguard.serializable import Serializable
from jsons import DecodeError, UnfulfilledArgumentError
from tqdm import tqdm

from arlo_e2e.tally import FastTallyEverythingResults, SelectionTally

T = TypeVar("T")
U = TypeVar("U", bound=Serializable)

ELECTION_DESCRIPTION: Final[str] = "election_description"
ENCRYPTED_TALLY: Final[str] = "encrypted_tally"
CRYPTO_CONSTANTS: Final[str] = "constants"
CRYPTO_CONTEXT: Final[str] = "cryptographic_context"


# Contents of this file loosely based on ElectionGuard's publish.py, and leveraging all the serialization
# built into ElectionGuard. We're doing a bit more error checking than they do, and we're trying to
# avoid directories with a million files if there are a million ballots.


def write_fast_tally(results: FastTallyEverythingResults, results_dir: str) -> None:
    """
    Writes out a directory with the full contents of the tally structure. Each ciphertext ballot
    will end up in its own file. Everything is JSON.
    """
    set_serializers()

    # Note: occurrences of os.sep in this function are a workaround for an ElectionGuard bug,
    #   where it's incorrectly handing file paths. Once that bug is fixed, we can remove every
    #   instance of os.sep here.

    results_dir = results_dir + os.sep
    log_info("write_fast_tally: starting!")
    if not path.exists(results_dir):
        mkdir(results_dir)

    log_info("write_fast_tally: writing election_description")
    results.election_description.to_json_file(ELECTION_DESCRIPTION, results_dir)

    log_info("write_fast_tally: writing crypto context")
    results.context.to_json_file(CRYPTO_CONTEXT, results_dir)

    log_info("write_fast_tally: writing crypto constants")
    ElectionConstants().to_json_file(CRYPTO_CONSTANTS, results_dir)

    log_info("write_fast_tally: writing tally")
    results.tally.to_json_file(ENCRYPTED_TALLY, results_dir)

    log_info("write_fast_tally: writing ballots")
    ballots_dir = path.join(results_dir, "ballots")
    _mkdir_helper(ballots_dir)
    for ballot in tqdm(results.encrypted_ballots):
        ballot_name = ballot.object_id

        # This prefix stuff: ballot uids are encoded as 'b' plus a 7-digit number.
        # Ballot #3 should be 'b0000003'. By taking the first 4 digits, and making
        # that into a directory, we get a max of 10,000 files per directory, which
        # should be good enough. We never want to type 'ls' in a directory and have
        # it wedge because it's trying to digest a million filenmes.

        ballot_name_prefix = ballot_name[0:4]  # letter b plus first three digits
        this_ballot_dir = path.join(ballots_dir, ballot_name_prefix) + os.sep
        _mkdir_helper(this_ballot_dir)
        ballot.to_json_file(ballot_name, this_ballot_dir)


def load_fast_tally(
    results_dir: str,
    check_proofs: bool = True,
    pool: Optional[Pool] = None,
    verbose: bool = False,
) -> Optional[FastTallyEverythingResults]:
    """
    Given the directory name / path-name to a disk represntation of a fast-tally structure, this reads
    it back in, makes sure it's well-formed, and optionally checks the cryptographic proofs. If any
    checks fail, `None` is returned. Errors are logged. Optional `pool` allows for some parallelism
    in the verification process.
    """
    set_serializers()

    if not path.exists(results_dir):
        log_error(f"Path ({results_dir}) not found, cannot load the fast-tally")
        return None

    election_description: Optional[ElectionDescription] = _load_helper(
        path.join(results_dir, ELECTION_DESCRIPTION + ".json"), ElectionDescription
    )
    if election_description is None:
        return None

    constants: Optional[ElectionConstants] = _load_helper(
        path.join(results_dir, CRYPTO_CONSTANTS + ".json"), ElectionConstants
    )
    if constants is None:
        return None
    if constants != ElectionConstants():
        log_error(
            f"constants are out of date or otherwise don't match the current library: {constants}"
        )
        return None

    cec: Optional[CiphertextElectionContext] = _load_helper(
        path.join(results_dir, CRYPTO_CONTEXT + ".json"), CiphertextElectionContext
    )
    if cec is None:
        return None

    encrypted_tally: Optional[SelectionTally] = _load_helper(
        path.join(results_dir, ENCRYPTED_TALLY + ".json"), SelectionTally
    )
    if encrypted_tally is None:
        return None

    ballots_dir = path.join(results_dir, "ballots")
    ballot_files = _all_filenames(ballots_dir)

    encrypted_ballots: List[Optional[CiphertextAcceptedBallot]] = [
        _load_helper(s, CiphertextAcceptedBallot) for s in ballot_files
    ]
    if encrypted_ballots is None or None in encrypted_ballots:
        # if even one of them fails, we're just going to give up and fail the whole thing
        return None

    # mypy isn't smart enough to notice the type change List[Optional[X]] --> List[X],
    # so we need to cast it, below.

    everything = FastTallyEverythingResults(
        election_description,
        cast(List[CiphertextAcceptedBallot], encrypted_ballots),
        encrypted_tally,
        cec,
    )

    if check_proofs:
        proofs_good = everything.all_proofs_valid(pool, verbose, True)
        if not proofs_good:
            # we don't need to log errors here; that will have happened internally
            return None

    return everything


def _mkdir_helper(p: str) -> None:
    if not path.exists(p):
        mkdir(p)


def _load_helper(
    filename: str, class_handle: Optional[Type[U]]
) -> Optional[T]:  # pragma: no cover
    try:
        s = os.stat(filename)
        if s.st_size == 0:
            log_error(f"The file ({filename}) is empty")
            return None

        with open(filename, "r") as subject:
            data = subject.read()
            result: Optional[T] = None
            if class_handle is not None:
                try:
                    result = class_handle.from_json(data)
                except DecodeError as err:
                    log_error(f"Failed to decode an instance of {class_handle}: {err}")
                    return None
                except UnfulfilledArgumentError as err:
                    log_error(f"Decoding failure for {class_handle}: {err}")
                    return None
            else:
                result = json.loads(data)
            if result is None:
                log_error(
                    f"failed to convert file ({filename}) to its proper internal type"
                )
            return result

    except OSError as e:
        log_error(f"Error reading file ({filename}): {e}")
        return None


def _all_filenames(root_dir: str) -> List[str]:
    results: List[str] = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            results.append(path.join(root, file))
    return results
