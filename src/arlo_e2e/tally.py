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
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.memo import Memo, make_memo_value
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
from electionguard.chaum_pedersen import ChaumPedersenDecryptionProof
from electionguard.decrypt_with_secrets import (
    ciphertext_ballot_to_dict,
    decrypt_ciphertext_with_proof,
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
    ElGamalKeyPair,
)
from electionguard.encrypt import encrypt_ballot
from electionguard.group import (
    ElementModQ,
    ElementModP,
    rand_q,
    int_to_q_unchecked,
)
from electionguard.logs import log_error
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


TALLY_TYPE = Dict[str, ElGamalCiphertext]
TALLY_INPUT_TYPE = Union[Dict[str, ElGamalCiphertext], CiphertextBallot]


def sequential_tally(ptallies: Sequence[TALLY_INPUT_TYPE]) -> TALLY_TYPE:
    """
    Internal function: sequentially tallies all of the ciphertext ballots, or other partial tallies,
    and returns a partial tally.
    """
    result: TALLY_TYPE = {}
    for ptally in ptallies:
        # we want do our computation purely in terms of TALLY_TYPE, so we'll convert CiphertextBallots
        if isinstance(ptally, CiphertextBallot):
            ptally = ciphertext_ballot_to_dict(ptally)

        for k in ptally.keys():
            if k not in result:
                result[k] = ptally[k]
            else:
                counter_sum = result[k]
                counter_partial = ptally[k]
                counter_sum = elgamal_add(counter_sum, counter_partial)
                result[k] = counter_sum
    return result


BALLOTS_PER_SHARD: Final[int] = 5
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
    ballots_iter: Sequence[TALLY_INPUT_TYPE] = ballots

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


# object_id, seed, ciphertext
DECRYPT_INPUT_TYPE = Tuple[str, ElementModQ, ElGamalCiphertext]

# object_id, plaintext, proof
DECRYPT_OUTPUT_TYPE = Tuple[str, int, ChaumPedersenDecryptionProof]


def _decrypt(
    cec: CiphertextElectionContext,
    keypair: ElGamalKeyPair,
    decrypt_input: DECRYPT_INPUT_TYPE,
) -> DECRYPT_OUTPUT_TYPE:  # pragma: no cover
    object_id, seed, c = decrypt_input
    plaintext, proof = decrypt_ciphertext_with_proof(
        c, keypair, seed, cec.crypto_extended_base_hash
    )
    return object_id, plaintext, proof


# object_id -> plaintext, proof
DECRYPT_TALLY_OUTPUT_TYPE = Dict[str, Tuple[int, ChaumPedersenDecryptionProof]]


def fast_decrypt_tally(
    tally: TALLY_TYPE,
    cec: CiphertextElectionContext,
    keypair: ElGamalKeyPair,
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
        (object_id, seed, tally[object_id])
        for seed, object_id in zip(proof_seeds, tkeys)
    ]

    if show_progress:  # pragma: no cover
        inputs = tqdm(list(inputs), "Decrypting")

    # Performance note: at this point, the tallies have been computed, so we
    # don't actually have all that much data left to process. There's almost
    # certainly no benefit to distributing this on a cluster.

    wrapped_func = functools.partial(_decrypt, cec, keypair)
    result: List[DECRYPT_OUTPUT_TYPE] = [
        wrapped_func(x) for x in inputs
    ] if pool is None else pool.map(func=wrapped_func, iterable=inputs)

    return {k: (p, proof) for k, p, proof in result}


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

    proof: ChaumPedersenDecryptionProof
    """
    Proof of the correspondence between `decrypted_tally` and `encrypted_tally`.
    """

    def is_valid_proof(
        self, public_key: ElementModP, hash_header: Optional[ElementModQ] = None
    ) -> bool:
        """Returns true if the plaintext, ciphertext, and proof are valid and consistent, false if not."""
        valid_proof: bool = self.proof.is_valid(
            self.decrypted_tally, self.encrypted_tally, public_key, hash_header
        )

        if not valid_proof:  # pragma: no cover
            log_error(
                f"Chaum-Pedersen proof validation failed: valid_proof: {valid_proof}, selection_info: {str(self)}"
            )

        return valid_proof


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
    public_key: ElementModP, hash_header: Optional[ElementModQ], s: SelectionInfo
) -> bool:  # pragma: no cover
    return s.is_valid_proof(public_key, hash_header)


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

    encrypted_ballot_memos: Dict[str, Memo[CiphertextAcceptedBallot]]
    """
    Dictionary from ballot ids to ballots.
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

    def all_files_present(self) -> bool:
        """
        Loads every encrypted ballot, but does not check the proofs. If any file does
        not match the hash in the manifest or otherwise does not load correctly, this
        method returns False.
        """
        if None in [m.contents for m in self.encrypted_ballot_memos.values()]:
            return False

        # At this point, we know that every file on disk loaded correctly. We need
        # make sure that they also match up to the metadata.

        encrypted_ballot_bids = {b.object_id for b in self.encrypted_ballots}
        return encrypted_ballot_bids == self.metadata.ballot_id_to_ballot_type.keys()

    @property
    def num_ballots(self) -> int:
        """
        Returns the number of ballots stored here.
        """
        return len(self.encrypted_ballot_memos.keys())

    @property
    def encrypted_ballots(self) -> List[CiphertextAcceptedBallot]:
        """
        Returns a list of all encrypted ballots. If they're on disk, this will cause
        them all to be loaded, which could take a while. If any file failed to load,
        then an error will have been logged, and it will be missing from this list.

        See `all_files_present` to make sure everything on disk is consistent with
        what we expect from the metadata.
        """
        return [
            m.contents
            for m in self.encrypted_ballot_memos.values()
            if m.contents is not None
        ]

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

    def get_encrypted_ballot(
        self, ballot_id: str
    ) -> Optional[CiphertextAcceptedBallot]:
        """
        Given the ballot identifier name, returns the corresponding `CiphertextAcceptedBallot`
        if it exists, otherwise `None`. If the ballots are on disk, this will cause only
        that one ballot to be loaded and cached.
        """
        if ballot_id not in self.encrypted_ballot_memos:
            log_error(f"ballot_id {ballot_id} missing")
            return None

        return self.encrypted_ballot_memos[ballot_id].contents

    def all_proofs_valid(
        self,
        pool: Optional[Pool] = None,
        verbose: bool = True,
        recheck_ballots_and_tallies: bool = False,
    ) -> bool:
        """
        Checks all the proofs used in this tally, returns True if everything is good.
        Any errors found will be logged. Normally, this only checks the proofs associated
        with the totals. If you want to also recompute the tally (i.e., tabulate the
        encrypted ballots) and verify every individual ballot proof, then set
        `recheck_ballots_and_tallies` to True.
        """

        log_and_print("Verifying proofs:", verbose)

        wrapped_func = functools.partial(
            _proof_verify,
            self.context.elgamal_public_key,
            self.context.crypto_extended_base_hash,
        )
        start = timer()

        inputs = self.tally.map.values()
        if verbose:  # pragma: no cover
            inputs = tqdm(list(inputs), "Tally proof")

        result: List[bool] = [
            wrapped_func(x) for x in inputs
        ] if pool is None else pool.map(func=wrapped_func, iterable=inputs)
        end = timer()
        log_and_print(f"Verification time: {end - start: .3f} sec", verbose)
        log_and_print(
            f"Verification rate: {len(self.tally.map.keys()) / (end - start): .3f} selection/sec",
            verbose,
        )

        if False in result:
            return False

        if recheck_ballots_and_tallies:
            # first, try to load all the ballots and make sure there are no hash errors
            if not self.all_files_present():
                return False

            # next, check each individual ballot's proofs; in this case, we're going to always
            # show the progress bar, even if verbose is false
            ballot_iter = tqdm(self.encrypted_ballots, desc="Ballot proofs")
            ballot_func = functools.partial(_ballot_proof_verify, self.context)

            ballot_start = timer()
            ballot_result: List[bool] = [
                ballot_func(x) for x in ballot_iter
            ] if pool is None else pool.map(func=ballot_func, iterable=ballot_iter)

            ballot_end = timer()
            log_and_print(
                f"Ballot verification rate: {len(self.encrypted_ballots) / (ballot_end - ballot_start): .3f} ballot/sec",
                verbose,
            )

            if False in ballot_result:
                return False

            log_and_print("Recomputing tallies:", verbose)
            recomputed_tally = fast_tally_ballots(self.encrypted_ballots, pool, verbose)

            tally_success = True

            for selection in recomputed_tally.keys():
                recomputed_ciphertext: ElGamalCiphertext = recomputed_tally[selection]
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

        matching_ballot_ids = [
            bid
            for bid in self.metadata.ballot_id_to_ballot_type.keys()
            if self.metadata.ballot_id_to_ballot_type[bid] in ballot_styles
        ]
        matching_ballots = [
            self.get_encrypted_ballot(bid) for bid in matching_ballot_ids
        ]
        matching_ballots_not_none = [x for x in matching_ballots if x is not None]

        # we're going to ignore missing ballots, for now, and march onward; an error will have been logged

        return matching_ballots_not_none


def _ballot_proof_verify(
    cec: CiphertextElectionContext, ballot: CiphertextAcceptedBallot
) -> bool:  # pragma: no cover
    return ballot.is_valid_encryption(
        ballot.description_hash, cec.elgamal_public_key, cec.crypto_extended_base_hash
    )


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
    log_and_print(f"Rows: {rows}, cols: {cols}", verbose)

    ed, ballots, id_map = cvrs.to_election_description(date=date)
    assert len(ballots) > 0, "can't have zero ballots!"

    keypair = (
        elgamal_keypair_random()
        if secret_key is None
        else elgamal_keypair_from_secret(secret_key)
    )
    assert keypair is not None, "unexpected failure with keypair computation"
    secret_key, public_key = keypair

    # This computation exists only to cause side-effects in the DLog engine, so the lame nonce is not an issue.
    assert len(ballots) == get_optional(
        elgamal_encrypt(
            m=len(ballots), nonce=int_to_q_unchecked(3), public_key=public_key
        )
    ).decrypt(secret_key), "got wrong ElGamal decryption!"

    dlog_prime_time = timer()
    log_and_print(
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

    log_and_print(
        f"Encryption time: {eg_encrypt_time - dlog_prime_time: .3f} sec", verbose
    )
    log_and_print(
        f"Encryption rate: {rows / (eg_encrypt_time - dlog_prime_time): .3f} ballot/sec",
        verbose,
    )

    if verbose:  # pragma: no cover
        print("\nTallying:")
    tally: TALLY_TYPE = fast_tally_ballots(cballots, pool, verbose)
    eg_tabulate_time = timer()

    log_and_print(
        f"Tabulation time: {eg_tabulate_time - eg_encrypt_time: .3f} sec", verbose
    )
    log_and_print(
        f"Tabulation rate: {rows / (eg_tabulate_time - eg_encrypt_time): .3f} ballot/sec",
        verbose,
    )
    log_and_print(
        f"Encryption and tabulation: {rows} ballots / {eg_tabulate_time - dlog_prime_time: .3f} sec = {rows / (eg_tabulate_time - dlog_prime_time): .3f} ballot/sec",
        verbose,
    )

    assert tally is not None, "tally failed!"

    if verbose:  # pragma: no cover
        print("\nDecryption & Proofs: ")
    decrypted_tally: DECRYPT_TALLY_OUTPUT_TYPE = fast_decrypt_tally(
        tally, cec, keypair, seed_hash, pool, verbose
    )
    eg_decryption_time = timer()
    log_and_print(
        f"Decryption time: {eg_decryption_time - eg_tabulate_time: .3f} sec", verbose
    )
    log_and_print(
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
            encrypted_tally=tally[k],
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
        encrypted_ballot_memos={
            ballot.object_id: make_memo_value(ballot) for ballot in accepted_ballots
        },
        tally=SelectionTally(reported_tally),
        context=cec,
    )
