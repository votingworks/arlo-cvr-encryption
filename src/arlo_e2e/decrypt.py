import functools
from multiprocessing import Pool
from typing import Optional, Dict, List

from electionguard.ballot import CiphertextAcceptedBallot
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

from arlo_e2e.utils import load_json_helper, write_json_helper


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

    selections: Dict[str, int] = plaintext_ballot_to_dict(pballot.ballot)
    ciphertexts: Dict[str, ElGamalCiphertext] = ciphertext_ballot_to_dict(
        ciphertext_ballot
    )
    proofs: Dict[str, ChaumPedersenDecryptionProof] = pballot.proofs
    for id in selections.keys():
        if id not in proofs:
            log_error(f"No proof found for selection id {id}")
            return False
        if id not in ciphertexts:
            log_error(f"No ciphertext found for selection id {id}")
            return False

        if not proofs[id].is_valid(
            selections[id], ciphertexts[id], public_key, extended_base_hash
        ):
            log_error(f"Invalid proof for selection id {id}")
            return False
    return True


def _decrypt(
    ied: InternalElectionDescription,
    extended_base_hash: ElementModQ,
    keypair: ElGamalKeyPair,
    ballot: CiphertextAcceptedBallot,
) -> Optional[ProvenPlaintextBallot]:
    secret_key, public_key = keypair

    pballot = decrypt_ballot_with_secret_and_proofs(
        ballot, ied, extended_base_hash, public_key, secret_key
    )

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

    decryptions: List[Optional[ProvenPlaintextBallot]] = pool.map(
        func=wrapped_decrypt, iterable=inputs
    ) if pool is not None else [wrapped_decrypt(x) for x in inputs]

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
    ballot_name_prefix = ballot_object_id[0:4]  # letter b plus first three digits
    return load_json_helper(
        decrypted_dir,
        ballot_object_id + ".json",
        ProvenPlaintextBallot,
        [ballot_name_prefix],
    )
