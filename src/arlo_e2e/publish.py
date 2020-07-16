import json
import os
from os import path, mkdir
from typing import Final, Optional, Callable, TypeVar

from electionguard.election import ElectionConstants, ElectionDescription
from electionguard.logs import log_error
from electionguard.publish import set_serializers
from electionguard.serializable import write_json_file

from arlo_e2e.tally import FastTallyEverythingResults

T = TypeVar("T")

ELECTION_DESCRIPTION: Final[str] = "election_description"
ENCRYPTED_TALLY: Final[str] = "encrypted_tally"
CRYPTO_CONSTANTS: Final[str] = "constants"
CRYPTO_CONTEXT: Final[str] = "cryptographic_context"


# Contents of this file loosely based on ElectionGuard's publish.py, and leveraging all the serialization
# built into ElectionGuard. We're doing a bit more error checking than they do, and we're trying to
# avoid directories with a million files if there are a million ballots.

def write_fast_tally(self: FastTallyEverythingResults, results_dir: str) -> None:
    """
    Writes out a directory with the full contents of the tally structure. Each ciphertext ballot
    will end up in its own file. Everything is JSON.
    """
    set_serializers()
    if not path.exists(results_dir):
        mkdir(results_dir)
    self.election_description.to_json_file(ELECTION_DESCRIPTION, results_dir)
    write_json_file(json_data=json.dumps(self.tally), file_name=ENCRYPTED_TALLY, file_path=results_dir)
    self.context.to_json_file(CRYPTO_CONTEXT, results_dir)
    ElectionConstants().to_json_file(CRYPTO_CONSTANTS, results_dir)

    ballots_dir = os.path.join(results_dir, "ballots")
    _mkdir_helper(ballots_dir)
    for ballot in self.encrypted_ballots:
        ballot_name = ballot.object_id

        # This prefix stuff: ballot uids are encoded as 'b' plus a 7-digit number.
        # Ballot #3 should be 'b0000003'. By taking the first 4 digits, and making
        # that into a directory, we get a max of 10,000 files per directory, which
        # should be good enough. We never want to type 'ls' in a directory and have
        # it wedge because it's trying to digest a million filenmes.

        ballot_name_prefix = ballot_name[0:4]  # letter b plus first three digits
        this_ballot_dir = ballots_dir + path.sep + ballot_name_prefix
        _mkdir_helper(this_ballot_dir)
        ballot.to_json_file(ballot_name, this_ballot_dir)


def load_fast_tally(results_dir: str, check_proofs: bool = True) -> Optional[FastTallyEverythingResults]:
    """
    Given the directory name / path-name to a disk represntation of a fast-tally structure, this reads
    it back in, makes sure it's well-formed, and optionally checks the cryptographic proofs. If any
    checks fail, `None` is returned. Errors are logged.
    """
    set_serializers()
    if not path.exists(results_dir):
        log_error(f"Path ({results_dir}) not found, cannot load the fast-tally")
        return None

    election_description = _load_helper(os.path.join(results_dir, ELECTION_DESCRIPTION + ".json"),
                                        lambda s: ElectionDescription.from_json(s))
    if election_description is None:
        return None

    # TODO: finish loading everything else


def _mkdir_helper(p: str) -> None:
    if not path.exists(p):
        mkdir(p)


def _load_helper(filename: str, str_to_obj: Callable[[str], T]) -> Optional[T]:
    try:
        s = os.stat(filename)
        if s.st_size == 0:
            log_error(f"The file ({filename}) is empty")
            return None

        with open(filename, "r") as subject:
            data = subject.read()
            return str_to_obj(data)

    except OSError as e:
        log_error(f"Error reading file ({filename}): {e}")


