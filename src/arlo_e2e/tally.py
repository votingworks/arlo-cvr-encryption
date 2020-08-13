import functools
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.pool import Pool
from timeit import default_timer as timer
from typing import (
    Tuple,
    List,
    Optional,
    Dict,
    NamedTuple,
    Sequence,
    Union,
    Final,
    Any,
    Set,
    Iterable,
)

from arlo_e2e.dominion import DominionCSV
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.utils import shard_list
from electionguard.ballot import (
    PlaintextBallot,
    CiphertextAcceptedBallot,
    BallotBoxState,
    CiphertextBallot,
    from_ciphertext_ballot,
    _list_eq,
)
from electionguard.chaum_pedersen import (
    ConstantChaumPedersenProof,
    make_constant_chaum_pedersen,
)
from electionguard.election import (
    InternalElectionDescription,
    CiphertextElectionContext,
    ElectionDescription,
    make_ciphertext_election_context,
)
from electionguard.elgamal import (
    ElGamalCiphertext,
    elgamal_add,
    elgamal_keypair_random,
    elgamal_encrypt,
    elgamal_keypair_from_secret,
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
from electionguard.serializable import Serializable
from electionguard.utils import get_optional
from tqdm import tqdm


def _encrypt(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input_tuple: Tuple[PlaintextBallot, ElementModQ],
) -> CiphertextBallot:  # pragma: no cover
    b, n = input_tuple

    # Coverage note: you'll see a directive on this method and on the other methods
    # used for the parallel mapping. For whatever reason, the Python coverage tool
    # can't figure out that they're running, so we'll exclude them.

    # Performance note: Nearly 2x performance boost by disabling proof verification
    # here. We do verify the tally proofs at the end, so doing all this extra work
    # here is in the "would be nice if cycles were free" category, but this is the
    # inner loop of the most performance-sensitive part of our code.
    return get_optional(
        encrypt_ballot(b, ied, cec, seed_hash, n, should_verify_proofs=False)
    )


def _ciphertext_ballot_to_accepted(
    ballot: CiphertextBallot,
) -> CiphertextAcceptedBallot:
    return from_ciphertext_ballot(ballot, BallotBoxState.CAST)


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
    if show_progress:  # pragma: no cover
        inputs = tqdm(list(inputs), desc="Encrypting")

    # Performance note: this will gain as much parallelism as you've got available ballots.
    # So, if you've got millions of ballots, this function can use them. This appears to be
    # the performance bottleneck for the whole computation, which means that this code would
    # benefit most from being distributed on a cluster.

    result: List[CiphertextBallot] = [
        wrapped_func(x) for x in inputs
    ] if pool is None else pool.map(func=wrapped_func, iterable=inputs)

    return result


# object_id -> nonce, ciphertext
TALLY_TYPE = Dict[str, Tuple[Optional[ElementModQ], ElGamalCiphertext]]


def cballot_to_partial_tally(cballot: CiphertextBallot) -> TALLY_TYPE:
    """
    Given a `CiphertextBallot`, extracts the relevant nonces and ciphertext counters used for
    our tallies.
    """
    result: TALLY_TYPE = {}
    for c in cballot.contests:
        for s in c.ballot_selections:
            if not s.is_placeholder_selection:
                result[s.object_id] = (s.nonce, s.ciphertext)
    return result


def sequential_tally(
    ptallies: Sequence[Union[TALLY_TYPE, CiphertextBallot]],
) -> TALLY_TYPE:
    """
    Internal function: sequentially tallies all of the ciphertext ballots, or other partial tallies,
    and returns a partial tally.
    """
    result: TALLY_TYPE = {}
    for ptally in ptallies:
        # we want do our computation purely in terms of TALLY_TYPE, so we'll convert CiphertextBallots
        if isinstance(ptally, CiphertextBallot):
            ptally = cballot_to_partial_tally(ptally)

        for k in ptally.keys():
            if k not in result:
                result[k] = ptally[k]
            else:
                nonce_sum, counter_sum = result[k]
                nonce_partial, counter_partial = ptally[k]
                nonce_sum = (
                    add_q(nonce_sum, nonce_partial)
                    if (nonce_sum is not None and nonce_partial is not None)
                    else None
                )
                counter_sum = elgamal_add(counter_sum, counter_partial)
                result[k] = (nonce_sum, counter_sum)
    return result


BALLOTS_PER_SHARD: Final[int] = 10
"""
For ballot tallying, we'll "shard" the list of ballots up into groups, and that
will be the work unit.
"""


def fast_tally_ballots(
    ballots: Sequence[CiphertextBallot],
    pool: Optional[Pool] = None,
    verbose: bool = False,
) -> TALLY_TYPE:
    """
    This function does a tally of the given list of ballots, returning a dictionary that maps
    from selection object_ids to the ElGamalCiphertext that corresponds to the encrypted tally
    of that selection. An optional `Pool` may be passed in, and it will be used to evaluate
    the ElGamal accumulation in parallel. If it's absent, then the accumulation will happen
    sequentially. Progress bars are not currently supported.
    """

    iter_count = 1
    ballots_iter: Sequence[Union[TALLY_TYPE, CiphertextBallot]] = ballots

    while True:
        if pool is None or len(ballots_iter) <= BALLOTS_PER_SHARD:
            return sequential_tally(ballots_iter)

        shards = shard_list(ballots_iter, BALLOTS_PER_SHARD)
        if verbose:
            print(f"Tally shards: {len(shards)}")
        partial_tallies: Sequence[TALLY_TYPE] = pool.map(
            func=sequential_tally, iterable=shards
        )

        iter_count += 1
        ballots_iter = partial_tallies


# object_id, seed, nonce, ciphertext
DECRYPT_INPUT_TYPE = Tuple[str, ElementModQ, ElementModQ, ElGamalCiphertext]

# object_id, plaintext, proof
DECRYPT_OUTPUT_TYPE = Tuple[str, int, ConstantChaumPedersenProof]


def _decrypt(
    public_key: ElementModP, secret_key: ElementModQ, decrypt_input: DECRYPT_INPUT_TYPE
) -> DECRYPT_OUTPUT_TYPE:  # pragma: no cover
    object_id, seed, nonce, c = decrypt_input
    plaintext = c.decrypt(secret_key)
    proof = make_constant_chaum_pedersen(c, plaintext, nonce, public_key, seed)
    return object_id, plaintext, proof


# object_id -> plaintext, proof
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
        for seed, object_id in zip(proof_seeds, tkeys)
    ]

    if show_progress:  # pragma: no cover
        inputs = tqdm(list(inputs), "Decrypting")

    # Performance note: at this point, the tallies have been computed, so we
    # don't actually have all that much data left to process. There's almost
    # certainly no benefit to distributing this on a cluster.

    wrapped_func = functools.partial(_decrypt, public_key, secret_key)
    result: List[DECRYPT_OUTPUT_TYPE] = [
        wrapped_func(x) for x in inputs
    ] if pool is None else pool.map(func=wrapped_func, iterable=inputs)

    return {k: (p, proof) for k, p, proof in result}


def _log_and_print(s: str, verbose: bool = True) -> None:
    if verbose:  # pragma: no cover
        print(f"    {s}")
    log_info(s)


@dataclass(eq=True)
class SelectionInfo(Serializable):
    """
    A tuple including a selection's object_id, a the encrypted and decrypted tallies, and a proof
    of their correspondence.
    """

    object_id: str
    """
    Selection object_id. To map from this to actual candidates in an election, you need
    the rest of the ElectionGuard `ElectionDescription`.
    """

    encrypted_tally: ElGamalCiphertext
    """
    Encrypted accumulation of every ciphertext counter for this particular selection.
    """

    decrypted_tally: int
    """
    Decrypted tally.
    """

    proof: ConstantChaumPedersenProof
    """
    Proof of the correspondence between `decrypted_tally` and `encrypted_tally`.
    """

    def is_valid_proof(self, public_key: ElementModP) -> bool:
        """Returns true if the plaintext, ciphertext, and proof are valid and consistent, false if not."""
        valid_proof: bool = self.proof.is_valid(self.encrypted_tally, public_key)
        same_constant: bool = self.proof.constant == self.decrypted_tally

        valid = same_constant and valid_proof
        if not valid:  # pragma: no cover
            log_error(
                f"Chaum-Pedersen proof validation failed: valid_proof: {valid_proof}, same_constant: {same_constant}, selection_info: {str(self)}"
            )

        return valid


@dataclass(eq=True)
class SelectionTally(Serializable):
    """
    A mapping from a selection's object_id to a `SelectionInfo` class.
    """

    map: Dict[str, SelectionInfo]

    # @classmethod
    # def from_json(cls, data_str: str):
    #     data = loads(data_str)
    #     return cls(map=data["map"])


def _proof_verify(
    public_key: ElementModP, s: SelectionInfo
) -> bool:  # pragma: no cover
    return s.is_valid_proof(public_key)


class FastTallyEverythingResults(NamedTuple):
    metadata: ElectionMetadata
    """
    All the public metadata we know about this election. Useful when interacting
    with `election_description`.
    """

    election_description: ElectionDescription
    """
    ElectionGuard top-level data structure that describes everything about the election: 
    the candidates, the parties, and so forth.
    """

    encrypted_ballots: List[CiphertextAcceptedBallot]
    """
    All the encrypted ballots.
    """

    tally: SelectionTally
    """
    A mapping from selection object_ids to a structure that includes the encrypted and
    decrypted tallies and a proof of their correspondence.
    """

    context: CiphertextElectionContext
    """
    Cryptographic context used in creating the tally.
    """

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FastTallyEverythingResults)
            and self.metadata == other.metadata
            and self.election_description == other.election_description
            and _list_eq(self.encrypted_ballots, other.encrypted_ballots)
            and self.tally == other.tally
            and self.context == other.context
        )

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def all_proofs_valid(
        self,
        pool: Optional[Pool] = None,
        verbose: bool = True,
        recheck_ballots_and_tallies: bool = False,
    ) -> bool:
        """
        Checks all the proofs used in this tally, returns True if everything is good.
        Any errors found will be logged.
        """
        _log_and_print("Verifying proofs:", verbose)

        wrapped_func = functools.partial(_proof_verify, self.context.elgamal_public_key)
        start = timer()

        inputs = self.tally.map.values()
        if verbose:  # pragma: no cover
            inputs = tqdm(list(inputs), "Tally proof")

        result: List[bool] = [
            wrapped_func(x) for x in inputs
        ] if pool is None else pool.map(func=wrapped_func, iterable=inputs)
        end = timer()
        _log_and_print(f"Verification time: {end - start: .3f} sec", verbose)
        _log_and_print(
            f"Verification rate: {len(self.tally.map.keys()) / (end - start): .3f} selection/sec",
            verbose,
        )

        if False in result:
            return False

        if recheck_ballots_and_tallies:
            # first, check each individual ballot's proofs; in this case, we're going to always
            # show the progress bar, even if verbose is false
            ballot_iter = tqdm(self.encrypted_ballots, desc="Ballot proofs")
            ballot_func = functools.partial(
                _ballot_proof_verify, self.context.elgamal_public_key
            )

            ballot_start = timer()
            ballot_result: List[bool] = [
                ballot_func(x) for x in ballot_iter
            ] if pool is None else pool.map(func=ballot_func, iterable=ballot_iter)

            ballot_end = timer()
            _log_and_print(
                f"Ballot verification rate: {len(self.encrypted_ballots) / (ballot_end - ballot_start): .3f} ballot/sec",
                verbose,
            )

            if False in ballot_result:
                return False

            _log_and_print("Recomputing tallies:", verbose)
            recomputed_tally = fast_tally_ballots(self.encrypted_ballots, pool, verbose)

            tally_success = True
            # Dict[str, Tuple[Optional[ElementModQ], ElGamalCiphertext]]
            for selection in recomputed_tally.keys():
                recomputed_ciphertext: ElGamalCiphertext = recomputed_tally[selection][
                    1
                ]
                provided_ciphertext: ElGamalCiphertext = self.tally.map[
                    selection
                ].encrypted_tally
                if recomputed_ciphertext != provided_ciphertext:
                    log_error(
                        f"Mismatching ciphertext tallies found for selection ({selection}). Recomputed sum: ({recomputed_ciphertext}), provided sum: ({provided_ciphertext})"
                    )
                    tally_success = False

            if not tally_success:
                return False

        return True

    def get_contest_titles_matching(self, prefixes: Iterable[str]) -> Set[str]:
        """
        Returns a set of all contest titles that match any of the given text prefixes. If an
        empty list of prefixes is provided, the result will be the empty-set.
        """

        # Python annoyance: strings are iterable, yielding each individual character.
        # We're okay with List[str] or Set[str], which gets us to Iterable[str]. If the
        # caller passes a bare string, that would preferably be a static error from mypy,
        # but sadly it won't be, thus the need for this assertion.
        assert not isinstance(
            prefixes, str
        ), "passed a string where a list or set of string was expected"

        if not prefixes:
            return set()
        else:
            return {
                contest_title
                for contest_title in sorted(self.metadata.contest_map.keys())
                if [prefix for prefix in prefixes if contest_title.startswith(prefix)]
            }

    def get_ballot_styles_for_contest_titles(
        self, contest_titles: Iterable[str]
    ) -> Set[str]:
        """
        Returns a set of all ballot styles that contain any of the given contest titles.
        """

        assert not isinstance(
            contest_titles, str
        ), "passed a string where a list or set of string was expected"

        contest_title_set = set(contest_titles)
        return {
            b
            for b in self.metadata.style_map.keys()
            if self.metadata.style_map[b].intersection(contest_title_set)
        }

    def get_ballots_matching_ballot_styles(
        self, ballot_styles: Iterable[str]
    ) -> List[CiphertextAcceptedBallot]:
        """
        Returns a list of `CiphertextAcceptedBallot` objects having any of the listed ballot styles.
        """

        assert not isinstance(
            ballot_styles, str
        ), "passed a string where a list or set of string was expected"

        ballot_styles_set = set(ballot_styles)
        ballot_style_ids: Set[str] = {
            self.metadata.ballot_types[style] for style in ballot_styles_set
        }

        return [
            b for b in self.encrypted_ballots if (b.ballot_style in ballot_style_ids)
        ]


def _ballot_proof_verify(
    public_key: ElementModP, ballot: CiphertextAcceptedBallot
) -> bool:  # pragma: no cover
    return ballot.is_valid_encryption(ballot.description_hash, public_key)


def fast_tally_everything(
    cvrs: DominionCSV,
    pool: Optional[Pool] = None,
    verbose: bool = True,
    date: Optional[datetime] = None,
    seed_hash: Optional[ElementModQ] = None,
    master_nonce: Optional[ElementModQ] = None,
    secret_key: Optional[ElementModQ] = None,
) -> FastTallyEverythingResults:
    """
    This top-level function takes a collection of Dominion CVRs and produces everything that
    we might want for arlo-e2e: a list of encrypted ballots, their encrypted and decrypted tally,
    and proofs of the correctness of the whole thing. The election `secret_key` is an optional
    parameter. If absent, a random keypair is generated and used. Similarly, if a `seed_hash` or
    `master_nonce` is not provided, random ones are generated and used.

    For parallelism, a `multiprocessing.pool.Pool` may be provided, and should result in significant
    speedups on multicore computers. If absent, the computation will proceed sequentially.
    """
    rows, cols = cvrs.data.shape

    if date is None:
        date = datetime.now()

    parse_time = timer()
    _log_and_print(f"Rows: {rows}, cols: {cols}", verbose)

    ed, ballots, id_map = cvrs.to_election_description(date=date)
    assert len(ballots) > 0, "can't have zero ballots!"

    if secret_key is None:
        secret_key, public_key = elgamal_keypair_random()
    else:
        tmp = elgamal_keypair_from_secret(secret_key)
        assert tmp is not None, "unexpected failure with keypair computation"
        public_key = tmp.public_key

    # This computation exists only to cause side-effects in the DLog engine, so the lame nonce is not an issue.
    assert len(ballots) == get_optional(
        elgamal_encrypt(
            m=len(ballots), nonce=int_to_q_unchecked(3), public_key=public_key
        )
    ).decrypt(secret_key), "got wrong ElGamal decryption!"

    dlog_prime_time = timer()
    _log_and_print(
        f"DLog prime time (n={len(ballots)}): {dlog_prime_time - parse_time: .3f} sec",
        verbose,
    )

    cec = make_ciphertext_election_context(
        number_of_guardians=1,
        quorum=1,
        elgamal_public_key=public_key,
        description_hash=ed.crypto_hash(),
    )

    ied = InternalElectionDescription(ed)

    # REVIEW THIS: is this cryptographically sound? Is the seed_hash properly a secret? Should
    # it go in the output? The nonces are clearly secret. If you know them, you can decrypt.
    if seed_hash is None:
        seed_hash = rand_q()
    if master_nonce is None:
        master_nonce = rand_q()
    nonces: List[ElementModQ] = Nonces(master_nonce)[0 : len(ballots)]

    # even if verbose is false, we still want to see the progress bar for the encryption
    cballots = fast_encrypt_ballots(ballots, ied, cec, seed_hash, nonces, pool, True)
    eg_encrypt_time = timer()

    _log_and_print(
        f"Encryption time: {eg_encrypt_time - dlog_prime_time: .3f} sec", verbose
    )
    _log_and_print(
        f"Encryption rate: {rows / (eg_encrypt_time - dlog_prime_time): .3f} ballot/sec",
        verbose,
    )

    if verbose:  # pragma: no cover
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
    _log_and_print(
        f"Encryption and tabulation: {rows} ballots / {eg_tabulate_time - dlog_prime_time: .3f} sec = {rows / (eg_tabulate_time - dlog_prime_time): .3f} ballot/sec",
        verbose,
    )

    assert tally is not None, "tally failed!"

    if verbose:  # pragma: no cover
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

    # Sanity-checking logic: make sure we don't have any unexpected keys, and that the decrypted totals
    # match up with the columns in the original plaintext data.
    for obj_id in decrypted_tally.keys():
        assert obj_id in id_map, "object_id in results that we don't know about!"
        cvr_sum = int(cvrs.data[id_map[obj_id]].sum())
        decryption, proof = decrypted_tally[obj_id]
        assert cvr_sum == decryption, f"decryption failed for {obj_id}"

    # Assemble the data structure that we're returning. Having nonces in the ciphertext makes these
    # structures sensitive for writing out to disk, but otherwise they're ready to go.
    reported_tally: Dict[str, SelectionInfo] = {
        k: SelectionInfo(
            object_id=k,
            encrypted_tally=tally[k][1],
            # we need to forcibly convert mpz to int here to make serialization work properly
            decrypted_tally=int(decrypted_tally[k][0]),
            proof=decrypted_tally[k][1],
        )
        for k in tally.keys()
    }

    # strips the ballots of their nonces, which is important because those could allow for decryption
    accepted_ballots = [_ciphertext_ballot_to_accepted(x) for x in cballots]

    return FastTallyEverythingResults(
        metadata=cvrs.metadata,
        election_description=ed,
        encrypted_ballots=accepted_ballots,
        tally=SelectionTally(reported_tally),
        context=cec,
    )
