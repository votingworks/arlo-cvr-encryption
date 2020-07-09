import functools
from multiprocessing.pool import Pool
from typing import Tuple, List, Optional, Dict, NamedTuple
from timeit import default_timer as timer

from electionguard.ballot import PlaintextBallot, CiphertextBallot
from electionguard.chaum_pedersen import (
    ConstantChaumPedersenProof,
    make_constant_chaum_pedersen,
)
from electionguard.election import (
    InternalElectionDescription,
    CiphertextElectionContext,
    ElectionDescription,
)
from electionguard.elgamal import (
    ElGamalCiphertext,
    elgamal_add,
    elgamal_keypair_random,
    elgamal_encrypt,
)
from electionguard.encrypt import encrypt_ballot
from electionguard.group import (
    ElementModQ,
    add_q,
    ElementModP,
    rand_q,
    int_to_q_unchecked,
)
from electionguard.logs import log_info, log_error
from electionguard.nonces import Nonces
from tqdm import tqdm

from dominion import DominionCSV


def _encrypt(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input: Tuple[PlaintextBallot, ElementModQ],
) -> CiphertextBallot:
    b, n = input
    result = encrypt_ballot(b, ied, cec, seed_hash, n, should_verify_proofs=False)
    assert result is not None, "ballot encryption failed!"
    return result


def fast_encrypt_ballots(
    ballots: List[PlaintextBallot],
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    nonces: List[ElementModQ],
    pool: Optional[Pool] = None,
    show_progress: bool = True,
) -> List[CiphertextBallot]:
    """
    This function encrypts a list of plaintext ballots, returning a list of ciphertext ballots.
    if the optional `pool` is passed, it will be used to evaluate the encryption in parallel.
    Also, a progress bar is displayed, by default, and can be disabled by setting `show_progress`
    to `False`.
    """

    assert len(ballots) == len(nonces), "need one nonce per ballot"
    wrapped_func = functools.partial(_encrypt, ied, cec, seed_hash)

    inputs = zip(ballots, nonces)
    if show_progress:
        inputs = tqdm(list(inputs))

    # Performance note: this will gain as much parallelism as you've got available ballots.
    # So, if you've got millions of ballots, this function should trivially be able to use
    # as many cores as you've got.

    result: List[CiphertextBallot] = [
        wrapped_func(x) for x in inputs
    ] if pool is None else pool.map(func=wrapped_func, iterable=inputs)

    return result


def _accumulate(
    data: Tuple[str, List[Tuple[ElementModQ, ElGamalCiphertext]]]
) -> Tuple[str, ElementModQ, ElGamalCiphertext]:
    object_id, ciphertexts = data
    # We're doing conventional addition on the nonces, which also exist in the exponents in the ciphertext,
    # and ElGamal accumulation on the ciphertexts themselves. We need the nonces to compute the
    # Chaum-Pedersen proof. (Right?)
    return (
        object_id,
        add_q(*[x[0] for x in ciphertexts]),
        elgamal_add(*[x[1] for x in ciphertexts]),
    )


TALLY_TYPE = Dict[str, Tuple[ElementModQ, ElGamalCiphertext]]


def fast_tally_ballots(
    ballots: List[CiphertextBallot],
    pool: Optional[Pool] = None,
    show_progress: bool = True,
) -> TALLY_TYPE:
    """
    This function does a tally of the given list of ballots, returning a dictionary that maps
    from selection object_ids to the ElGamalCiphertext that corresponds to the encrypted tally
    of that selection. An optional `Pool` may be passed in, and it will be used to evaluate
    the ElGamal accumulation in parallel. If it's absent, then the accumulation will happen
    sequentially. Also, a progress bar is displayed, by default, and can be disabled by
    setting `show_progress` to `False`.
    """
    messages: Dict[str, List[Tuple[ElementModQ, ElGamalCiphertext]]] = {}

    # First, we fill up this dictionary with all the individual ciphertexts. This rearrangement
    # of the data makes it much easier to run the tally in parallel.
    for b in ballots:
        for c in b.contests:
            for s in c.ballot_selections:
                if not s.is_placeholder_selection:
                    if s.object_id not in messages:
                        messages[s.object_id] = []
                    assert (
                        s.nonce is not None
                    ), "we need the encryption nonce to generate the Chaum-Pedersen proof"
                    messages[s.object_id].append((s.nonce, s.message))

    # Now, we're creating the list of tuples that will be the arguments to _accumulate,
    # for running in parallel.
    inputs = [(object_id, messages[object_id]) for object_id in messages.keys()]
    if show_progress:
        inputs = tqdm(list(inputs))

    # Performance note: as written, we'll create one task for every ballot selection. So,
    # if there are millions of ballots but only 100 selections, there will still be only
    # 100-way parallelism available. The way to gain additional parallelism is to subdivide
    # these lists if they're really long. Each list can be passed to _accumulate() separately,
    # for much more parallelism, and then you have to accumulate all those intermediate results.

    result: List[Tuple[str, ElementModQ, ElGamalCiphertext]] = [
        _accumulate(x) for x in inputs
    ] if pool is None else pool.map(func=_accumulate, iterable=inputs)

    return {k: (n, v) for k, n, v in result}


# object_id, seed, nonce, ciphertext
DECRYPT_INPUT_TYPE = Tuple[str, ElementModQ, ElementModQ, ElGamalCiphertext]
DECRYPT_OUTPUT_TYPE = Tuple[str, int, ConstantChaumPedersenProof]


def _decrypt(
    public_key: ElementModP, secret_key: ElementModQ, input: DECRYPT_INPUT_TYPE
) -> Tuple[str, int, ConstantChaumPedersenProof]:
    object_id, seed, nonce, c = input
    plaintext = c.decrypt(secret_key)
    proof = make_constant_chaum_pedersen(c, plaintext, nonce, public_key, seed)
    return object_id, plaintext, proof


DECRYPT_TALLY_OUTPUT_TYPE = Dict[str, Tuple[int, ConstantChaumPedersenProof]]


def fast_decrypt_tally(
    tally: TALLY_TYPE,
    public_key: ElementModP,
    secret_key: ElementModQ,
    proof_seed: ElementModQ,
    pool: Optional[Pool] = None,
    show_progress: bool = True,
) -> DECRYPT_TALLY_OUTPUT_TYPE:
    """
    Given a tally, as we might get from `fast_tally_ballots`, this decrypts the tally
    and returns a dict from selection object_ids to tuples containing the decrypted
    total as well as a Chaum-Pedersen proof that the total corresponds to the ciphertext.
    """
    tkeys = tally.keys()
    proof_seeds: List[ElementModQ] = Nonces(proof_seed)[0 : len(tkeys)]
    inputs = [
        (object_id, seed, tally[object_id][0], tally[object_id][1])
        for seed, object_id in zip(proof_seeds, tally.keys())
    ]

    if show_progress:
        inputs = tqdm(list(inputs))

    wrapped_func = functools.partial(_decrypt, public_key, secret_key)
    result: List[DECRYPT_OUTPUT_TYPE] = [
        wrapped_func(x) for x in inputs
    ] if pool is None else pool.map(func=wrapped_func, iterable=inputs)

    return {k: (p, proof) for k, p, proof in result}


def _log_and_print(s: str, verbose: bool) -> None:
    if verbose:
        print(f"    {s}")
    log_info(s)


class SelectionInfo(NamedTuple):
    object_id: str
    encrypted_tally: ElGamalCiphertext
    decrypted_tally: int
    proof: ConstantChaumPedersenProof

    def is_valid_proof(self, public_key: ElementModP) -> bool:
        """Returns true if the plaintext, ciphertext, and proof are valid and consistent, false if not."""
        valid_proof: bool = self.proof.is_valid(self.encrypted_tally, public_key)
        same_constant: bool = self.proof.constant == self.decrypted_tally

        valid = same_constant and valid_proof
        if not valid:
            log_error(
                f"Chaum-Pedersen proof validation failed: valid_proof: {valid_proof}, same_constant: {same_constant}, selection_info: {str(self)}"
            )

        return valid


def _proof_verify(public_key: ElementModP, s: SelectionInfo) -> bool:
    return s.is_valid_proof(public_key)


class FastTallyEverythingResults(NamedTuple):
    election_description: ElectionDescription
    public_key: ElementModP
    seed_hash: ElementModQ
    encrypted_ballots: List[CiphertextBallot]
    tally: Dict[str, SelectionInfo]

    def all_proofs_valid(
        self, pool: Optional[Pool] = None, verbose: bool = True
    ) -> bool:
        if verbose:
            print("\nVerifying proofs:")

        wrapped_func = functools.partial(_proof_verify, self.public_key)
        start = timer()

        inputs = self.tally.values()
        if verbose:
            inputs = tqdm(list(inputs))

        # Performance note: this will gain as much parallelism as you've got available ballots.
        # So, if you've got millions of ballots, this function should trivially be able to use
        # as many cores as you've got.

        result: List[bool] = [
            wrapped_func(x) for x in inputs
        ] if pool is None else pool.map(func=wrapped_func, iterable=inputs)
        end = timer()
        _log_and_print(f"Verification time: {end - start: .3f} sec", verbose)
        _log_and_print(
            f"Verification rate: {len(self.tally.keys()) / (end - start): .3f} selection/sec",
            verbose,
        )

        return False not in result


def fast_tally_everything(
    cvrs: DominionCSV,
    pool: Pool,
    verbose: bool = True,
    seed_hash: Optional[ElementModQ] = None,
) -> FastTallyEverythingResults:
    rows, cols = cvrs.data.shape

    parse_time = timer()
    _log_and_print(f"Rows: {rows}, cols: {cols}", verbose)

    ed, ballots, id_map = cvrs.to_election_description()
    assert len(ballots) > 0, "can't have zero ballots!"

    secret_key, public_key = elgamal_keypair_random()

    # This computation exists only to cause side-effects in the DLog engine, so the lame nonce is not an issue.
    assert len(ballots) == elgamal_encrypt(
        m=len(ballots), nonce=int_to_q_unchecked(3), public_key=public_key
    ).decrypt(secret_key), "got wrong ElGamal decryption!"

    dlog_prime_time = timer()
    _log_and_print(
        f"DLog prime time (n={len(ballots)}): {dlog_prime_time - parse_time: .3f} sec",
        verbose,
    )

    cec = CiphertextElectionContext(
        number_of_guardians=1,
        quorum=1,
        elgamal_public_key=public_key,
        description_hash=ed.crypto_hash(),
    )

    ied = InternalElectionDescription(ed)

    # Review this: is this cryptographically sound? Is the seed_hash properly a secret? Should
    # it go in the output?
    if seed_hash is None:
        seed_hash = rand_q()
    nonces: List[ElementModQ] = Nonces(seed_hash)[0 : len(ballots)]

    if verbose:
        print("Encrypting:")
    cballots = fast_encrypt_ballots(ballots, ied, cec, seed_hash, nonces, pool, verbose)
    eg_encrypt_time = timer()

    _log_and_print(
        f"Encryption time: {eg_encrypt_time - dlog_prime_time: .3f} sec", verbose
    )
    _log_and_print(
        f"Encryption rate: {rows / (eg_encrypt_time - dlog_prime_time): .3f} ballot/sec",
        verbose,
    )

    if verbose:
        print("\nTallying:")
    tally = fast_tally_ballots(cballots, pool, verbose)
    eg_tabulate_time = timer()

    _log_and_print(
        f"Tabulation time: {eg_tabulate_time - eg_encrypt_time: .3f} sec", verbose
    )
    _log_and_print(
        f"Tabulation rate: {rows / (eg_tabulate_time - eg_encrypt_time): .3f} ballot/sec",
        verbose,
    )

    # tally = tally_ballots(ballot_box._store, ied, cec)
    assert tally is not None, "tally failed!"

    if verbose:
        print("\nDecryption & Proofs: ")
    decrypted_tally = fast_decrypt_tally(
        tally, public_key, secret_key, seed_hash, pool, verbose
    )
    eg_decryption_time = timer()
    _log_and_print(
        f"Decryption time: {eg_decryption_time - eg_tabulate_time: .3f} sec", verbose
    )
    _log_and_print(
        f"Decryption rate: {len(decrypted_tally.keys()) / (eg_decryption_time - eg_tabulate_time): .3f} selection/sec",
        verbose,
    )

    for obj_id in decrypted_tally.keys():
        assert obj_id in id_map, "object_id in results that we don't know about!"
        cvr_sum = int(cvrs.data[id_map[obj_id]].sum())
        decryption, proof = decrypted_tally[obj_id]
        assert cvr_sum == decryption, f"decryption failed for {obj_id}"

    # we need to remove the accumulation nonces before we return
    reported_tally: Dict[str, SelectionInfo] = {
        k: SelectionInfo(
            object_id=k,
            encrypted_tally=tally[k][1],
            decrypted_tally=decrypted_tally[k][0],
            proof=decrypted_tally[k][1],
        )
        for k in tally.keys()
    }

    return FastTallyEverythingResults(
        election_description=ed,
        public_key=public_key,
        seed_hash=seed_hash,
        encrypted_ballots=cballots,
        tally=reported_tally,
    )
