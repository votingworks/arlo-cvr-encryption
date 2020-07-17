import json
import os
from multiprocessing.pool import Pool
from os import path, mkdir
from typing import Final, Optional, Callable, TypeVar, Dict, Any, List, Type

from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.election import (
    ElectionConstants,
    ElectionDescription,
    CiphertextElectionContext,
)
from electionguard.logs import log_error
from electionguard.publish import set_serializers
from electionguard.serializable import write_json_file, Serializable
from jsons import set_serializer

from arlo_e2e.tally import FastTallyEverythingResults, SelectionInfo

T = TypeVar("T")
U = TypeVar("U", bound=Serializable)

ELECTION_DESCRIPTION: Final[str] = "election_description"
ENCRYPTED_TALLY: Final[str] = "encrypted_tally"
CRYPTO_CONSTANTS: Final[str] = "constants"
CRYPTO_CONTEXT: Final[str] = "cryptographic_context"


# Contents of this file loosely based on ElectionGuard's publish.py, and leveraging all the serialization
# built into ElectionGuard. We're doing a bit more error checking than they do, and we're trying to
# avoid directories with a million files if there are a million ballots.


def _serializers_init():
    set_serializers()


def write_fast_tally(tally: FastTallyEverythingResults, results_dir: str) -> None:
    """
    Writes out a directory with the full contents of the tally structure. Each ciphertext ballot
    will end up in its own file. Everything is JSON.
    """
    _serializers_init()

    if not path.exists(results_dir):
        mkdir(results_dir)
    tally.election_description.to_json_file(ELECTION_DESCRIPTION, results_dir)
    write_json_file(
        json_data=json.dumps(tally.tally),
        file_name=ENCRYPTED_TALLY,
        file_path=results_dir,
    )
    tally.context.to_json_file(CRYPTO_CONTEXT, results_dir)
    ElectionConstants().to_json_file(CRYPTO_CONSTANTS, results_dir)

    ballots_dir = os.path.join(results_dir, "ballots")
    _mkdir_helper(ballots_dir)
    for ballot in tally.encrypted_ballots:
        ballot_name = ballot.object_id

        # This prefix stuff: ballot uids are encoded as 'b' plus a 7-digit number.
        # Ballot #3 should be 'b0000003'. By taking the first 4 digits, and making
        # that into a directory, we get a max of 10,000 files per directory, which
        # should be good enough. We never want to type 'ls' in a directory and have
        # it wedge because it's trying to digest a million filenmes.

        ballot_name_prefix = ballot_name[0:4]  # letter b plus first three digits
        this_ballot_dir = os.path.join(ballots_dir, ballot_name_prefix)
        _mkdir_helper(this_ballot_dir)
        ballot.to_json_file(ballot_name, this_ballot_dir)


def load_fast_tally(
    results_dir: str, check_proofs: bool = True, pool: Optional[Pool] = None
) -> Optional[FastTallyEverythingResults]:
    """
    Given the directory name / path-name to a disk represntation of a fast-tally structure, this reads
    it back in, makes sure it's well-formed, and optionally checks the cryptographic proofs. If any
    checks fail, `None` is returned. Errors are logged. Optional `pool` allows for some parallelism
    in the verification process.
    """
    _serializers_init()

    if not path.exists(results_dir):
        log_error(f"Path ({results_dir}) not found, cannot load the fast-tally")
        return None

    election_description: Optional[ElectionDescription] = _load_helper(
        os.path.join(results_dir, ELECTION_DESCRIPTION + ".json"), ElectionDescription
    )
    if election_description is None:
        return None

    encrypted_tally: Optional[Dict[str, SelectionInfo]] = _load_helper(
        os.path.join(results_dir, ENCRYPTED_TALLY + ".json"), None
    )
    if encrypted_tally is None:
        return None

    cec: Optional[CiphertextElectionContext] = _load_helper(
        os.path.join(results_dir, CRYPTO_CONTEXT + ".json"), CiphertextElectionContext
    )
    if cec is None:
        return None

    constants: Optional[ElectionConstants] = _load_helper(
        os.path.join(results_dir, CRYPTO_CONSTANTS + ".json"), ElectionConstants
    )
    if constants is None:
        return None
    if constants != ElectionConstants():
        log_error(
            f"constants are out of date or otherwise don't match the current library: {constants}"
        )
        return None

    ballots_dir = os.path.join(results_dir, "ballots")
    ballot_files = _all_filenames(ballots_dir)

    encrypted_ballots: List[Optional[CiphertextAcceptedBallot]] = [
        _load_helper(s, CiphertextAcceptedBallot) for s in ballot_files
    ]
    if encrypted_ballots is None or None in encrypted_ballots:
        # if even one of them fails, we're just going to give up and fail the whole thing
        return None

    everything = FastTallyEverythingResults(
        election_description, encrypted_ballots, encrypted_tally, cec
    )

    if check_proofs:
        proofs_good = everything.all_proofs_valid(pool)
        if not proofs_good:
            # we don't need to log errors here; that will have happened internally
            return None

    return everything


def _encrypted_tally_load(s: Any) -> Optional[Dict[str, SelectionInfo]]:
    # TODO: does this even kinda work
    return s


def _mkdir_helper(p: str) -> None:
    if not path.exists(p):
        mkdir(p)


def _load_helper(filename: str, type: Optional[Type[U]]) -> Optional[T]:
    try:
        s = os.stat(filename)
        if s.st_size == 0:
            log_error(f"The file ({filename}) is empty")
            return None

        with open(filename, "r") as subject:
            data = subject.read()
            if type is not None:
                result = type.from_json(data)
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
            results.append(os.path.join(root, file))
    return results
