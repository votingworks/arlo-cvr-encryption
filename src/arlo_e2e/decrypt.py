import functools
from multiprocessing.pool import Pool
from typing import Optional, Dict, List

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
from tqdm import tqdm

from arlo_e2e.utils import load_json_helper, write_json_helper, file_exists_helper


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


def write_proven_ballot(pballot: ProvenPlaintextBallot, decrypted_dir: str) -> None:
    """
    Writes out a `ProvenPlaintextBallot` in the desired directory.
    """
    ballot_object_id = pballot.ballot.object_id
    ballot_name_prefix = ballot_object_id[0:4]  # letter b plus first three digits
    write_json_helper(
        decrypted_dir, ballot_object_id + ".json", pballot, [ballot_name_prefix]
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

    ballot_name_prefix = ballot_object_id[0:4]  # letter b plus first three digits
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
    ballot_name_prefix = ballot_object_id[0:4]  # letter b plus first three digits
    return file_exists_helper(
        decrypted_dir,
        ballot_object_id + ".json",
        [ballot_name_prefix],
    )
