import csv
import functools
import os
from multiprocessing.pool import Pool
from typing import Optional, Dict, List, Union

import ray
from electionguard.ballot import CiphertextAcceptedBallot, PlaintextBallotSelection
from electionguard.chaum_pedersen import ChaumPedersenDecryptionProof
from electionguard.decrypt_with_secrets import (
    ProvenPlaintextBallot,
    decrypt_ballot_with_secret_and_proofs,
    plaintext_ballot_to_dict,
    ciphertext_ballot_to_dict,
)
from electionguard.election import InternalElectionDescription
from electionguard.elgamal import ElGamalCiphertext, ElGamalKeyPair
from electionguard.group import ElementModQ, ElementModP
from electionguard.logs import log_error
from ray.actor import ActorHandle
from tqdm import tqdm

from arlo_e2e.admin import ElectionAdmin
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.html_index import generate_index_html_files
from arlo_e2e.ray_progress import ProgressBar
from arlo_e2e.ray_tally import RayTallyEverythingResults
from arlo_e2e.tally import FastTallyEverythingResults
from arlo_e2e.utils import (
    load_json_helper,
    write_json_helper,
    file_exists_helper,
    BALLOT_FILENAME_PREFIX_DIGITS,
    mkdir_helper,
    write_file_with_retries,
)


@ray.remote
def r_verify_proven_ballot_proofs(
    extended_base_hash: ElementModQ,
    public_key: ElementModP,
    ciphertext_ballot: CiphertextAcceptedBallot,
    pballot: ProvenPlaintextBallot,
) -> bool:
    return verify_proven_ballot_proofs(
        extended_base_hash, public_key, ciphertext_ballot, pballot
    )


def verify_proven_ballot_proofs(
    extended_base_hash: ElementModQ,
    public_key: ElementModP,
    ciphertext_ballot: CiphertextAcceptedBallot,
    pballot: ProvenPlaintextBallot,
) -> bool:
    """
    Returns True if the proofs are consistent with the ciphertext.
    """
    # We're going to check the proof for validity here, even though it takes real time to compute,
    # because we don't expect to be decrypting very many ballots at once, and it's really valuable
    # to do the extra checking for correctness.

    selections: Dict[str, PlaintextBallotSelection] = plaintext_ballot_to_dict(
        pballot.ballot
    )
    ciphertexts: Dict[str, ElGamalCiphertext] = ciphertext_ballot_to_dict(
        ciphertext_ballot
    )
    proofs: Dict[str, ChaumPedersenDecryptionProof] = pballot.proofs
    for id in selections.keys():
        if id not in proofs:  # pragma: no cover
            log_error(f"No proof found for selection id {id}")
            return False
        if id not in ciphertexts:  # pragma: no cover
            log_error(f"No ciphertext found for selection id {id}")
            return False

        if not proofs[id].is_valid(
            selections[id].to_int(), ciphertexts[id], public_key, extended_base_hash
        ):
            log_error(f"Invalid proof for selection id {id}")
            return False
    return True


def _decrypt(
    ied: InternalElectionDescription,
    extended_base_hash: ElementModQ,
    keypair: ElGamalKeyPair,
    ballot: CiphertextAcceptedBallot,
) -> Optional[ProvenPlaintextBallot]:  # pragma: no cover
    secret_key, public_key = keypair

    pballot = decrypt_ballot_with_secret_and_proofs(
        ballot, ied, extended_base_hash, public_key, secret_key
    )

    if pballot is None:
        return None

    if verify_proven_ballot_proofs(extended_base_hash, public_key, ballot, pballot):
        return pballot
    else:
        return None


def decrypt_ballots(
    ied: InternalElectionDescription,
    extended_base_hash: ElementModQ,
    keypair: ElGamalKeyPair,
    pool: Optional[Pool],
    encrypted_ballots: List[CiphertextAcceptedBallot],
) -> List[Optional[ProvenPlaintextBallot]]:
    """
    Given a list of encrypted ballots and all the associated state necessary to decrypt them,
    returns a list of `ProvenPlaintextBallot`, which can be computed in parallel if a multiprocessing
    `pool` is passed along. If any of the decryptions fails, the associated item in the output
    list will be `None`.
    """

    wrapped_decrypt = functools.partial(_decrypt, ied, extended_base_hash, keypair)
    inputs = tqdm(encrypted_ballots, desc="Decrypting ballots")

    decryptions: List[Optional[ProvenPlaintextBallot]] = (
        pool.map(func=wrapped_decrypt, iterable=inputs)
        if pool is not None
        else [wrapped_decrypt(x) for x in inputs]
    )

    return decryptions


def write_proven_ballot(
    pballot: ProvenPlaintextBallot, decrypted_dir: str, num_retries: int = 1
) -> None:
    """
    Writes out a `ProvenPlaintextBallot` in the desired directory.
    """
    ballot_object_id = pballot.ballot.object_id
    ballot_name_prefix = ballot_object_id[
        0:BALLOT_FILENAME_PREFIX_DIGITS
    ]  # letter b plus first few digits
    write_json_helper(
        decrypted_dir,
        ballot_object_id + ".json",
        pballot,
        [ballot_name_prefix],
        num_retries=num_retries,
    )


def load_proven_ballot(
    ballot_object_id: str, decrypted_dir: str
) -> Optional[ProvenPlaintextBallot]:
    """
    Reads a `ProvenPlaintextBallot` from the desired directory. On failure, returns `None`.
    """

    # Special case here because normally load_json_helper would log an error, and we don't
    # want that, since this case might happen frequently.
    if not exists_proven_ballot(ballot_object_id, decrypted_dir):
        return None

    ballot_name_prefix = ballot_object_id[
        0:BALLOT_FILENAME_PREFIX_DIGITS
    ]  # letter b plus first few digits
    return load_json_helper(
        decrypted_dir,
        ballot_object_id + ".json",
        ProvenPlaintextBallot,
        [ballot_name_prefix],
    )


def exists_proven_ballot(ballot_object_id: str, decrypted_dir: str) -> bool:
    """
    Checks if the desired `ballot_object_id` has been decrypted and written to `decrypted_dir`.
    """
    ballot_name_prefix = ballot_object_id[
        0:BALLOT_FILENAME_PREFIX_DIGITS
    ]  # letter b plus first few digits
    return file_exists_helper(
        decrypted_dir,
        ballot_object_id + ".json",
        [ballot_name_prefix],
    )


@ray.remote
def r_decrypt_and_write_one(
    keypair: ElGamalKeyPair,
    results: RayTallyEverythingResults,
    ied: InternalElectionDescription,
    extended_base_hash: ElementModQ,
    ballot_id: str,
    decrypted_dir: str,
    progressbar_actor: ActorHandle,
) -> int:  # pragma: no cover
    """
    Helper method for decrypt_and_write: runs remotely, returns the number of decrypted ballots
    successfully written to disk (usually 1, 0 for failure), suitable for adding up later to
    see how many successes we had.
    """
    encrypted_ballot = results.get_encrypted_ballot(ballot_id)
    if encrypted_ballot is None:
        progressbar_actor.update_completed.remote("Ballots", 1)
        return 0

    plaintext = _decrypt(ied, extended_base_hash, keypair, encrypted_ballot)
    if plaintext is None:
        progressbar_actor.update_completed.remote("Ballots", 1)
        return 0

    write_proven_ballot(plaintext, decrypted_dir, num_retries=10)
    progressbar_actor.update_completed.remote("Ballots", 1)
    return 1


def decrypt_and_write(
    admin_state: ElectionAdmin,
    results: FastTallyEverythingResults,
    ballot_ids: List[str],
    decrypted_dir: str,
) -> bool:
    """
    Top-level command: given all the necessary election state, decrypts the desired ballots
    (by ballot-id strings), and writes them out to the desired output directory. Returns True
    if everything worked, or False if there was some sort of error. Errors are also printed
    to stdout.

    Uses a Ray cluster for speedup.
    """

    if not ray.is_initialized():
        log_and_print("need Ray running for decrypt_and_tally")
        return False

    fail = False
    for bid in ballot_ids:
        if bid not in results.metadata.ballot_id_to_ballot_type:
            print(f"Ballot id {bid} is not part of the tally")
            fail = True

    if fail:
        return False

    progressbar = ProgressBar({"Ballots": len(ballot_ids)})

    # encrypted_ballots = [results.get_encrypted_ballot(bid) for bid in ballot_ids]
    # num_encrypted_ballots = len([x for x in encrypted_ballots if x is not None])
    # if num_encrypted_ballots != len(ballot_ids):
    #     log_and_print(
    #         f"Only successfully loaded {num_encrypted_ballots} of {len(ballot_ids)} encrypted ballots"
    #     )
    #     return False

    r_ied = ray.put(InternalElectionDescription(results.election_description))
    r_extended_base_hash = ray.put(results.context.crypto_extended_base_hash)
    r_keypair = ray.put(admin_state.keypair)
    r_results = ray.put(results)
    r_decrypted_dir = ray.put(decrypted_dir)

    mkdir_helper(decrypted_dir)

    plaintexts_future = [
        r_decrypt_and_write_one.remote(
            r_keypair,
            r_results,
            r_ied,
            r_extended_base_hash,
            bid,
            r_decrypted_dir,
            progressbar.actor,
        )
        for bid in ballot_ids
    ]
    progressbar.print_until_done()
    progressbar.close()
    successful_ops = sum(ray.get(plaintexts_future))

    if successful_ops < len(ballot_ids):
        log_and_print(
            f"Only successfully wrote {successful_ops} of {len(ballot_ids)} proven plaintext ballots"
        )
        return False

    # we also want to write out the relevant metadata rows
    cvr_subset = results.cvr_metadata.loc[
        results.cvr_metadata["BallotId"].isin(ballot_ids)
    ]
    metadata_filename = os.path.join(decrypted_dir, "cvr_metadata.csv")
    cvr_bytes = cvr_subset.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC)
    write_file_with_retries(metadata_filename, cvr_bytes, num_attempts=10)

    generate_index_html_files(
        f"{results.metadata.election_name} (Decrypted Ballots)",
        decrypted_dir,
        num_retries=10,
    )

    return True
