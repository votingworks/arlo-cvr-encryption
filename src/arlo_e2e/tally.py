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

import pandas as pd
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
    decrypt_ballot_with_secret,
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

from arlo_e2e.dominion import DominionCSV
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.manifest import Manifest
from arlo_e2e.memo import Memo, make_memo_value, make_memo_lambda
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.utils import shard_list_uniform


def encrypt_ballot_helper(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input_tuple: Tuple[PlaintextBallot, ElementModQ],
) -> CiphertextBallot:  # pragma: no cover
    """
    Given a ballot and the associated metadata, encrypt it. Note that this method
    is meant to be used with `functools.partial`, so we can create a function
    that only takes the final tuple argument while remembering all the rest.
    """
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


def ciphertext_ballot_to_accepted(
    ballot: CiphertextBallot,
) -> CiphertextAcceptedBallot:
    """
    Given a CiphertextBallot, returns a CiphertextAcceptedBallot (and thus, with
    with nonces removed).
    """
    return from_ciphertext_ballot(ballot, BallotBoxState.CAST)


def fast_encrypt_ballots(
    ballots: List[PlaintextBallot],
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    nonces: List[ElementModQ],
    pool: Optional[Pool] = None,
    use_progressbar: bool = True,
) -> List[CiphertextBallot]:
    """
    This function encrypts a list of plaintext ballots, returning a list of ciphertext ballots.
    if the optional `pool` is passed, it will be used to evaluate the encryption in parallel.
    Also, a progress bar is displayed, by default, and can be disabled by setting `use_progressbar`
    to `False`.
    """

    assert len(ballots) == len(nonces), "need one nonce per ballot"
    wrapped_func = functools.partial(encrypt_ballot_helper, ied, cec, seed_hash)

    inputs = zip(ballots, nonces)
    if use_progressbar:  # pragma: no cover
        inputs = tqdm(list(inputs), desc="Ballots")

    # Performance note: this will gain as much parallelism as you've got available ballots.
    # So, if you've got millions of ballots, this function can use them. This appears to be
    # the performance bottleneck for the whole computation, which means that this code would
    # benefit most from being distributed on a cluster.

    result: List[CiphertextBallot] = (
        [wrapped_func(x) for x in inputs]
        if pool is None
        else pool.map(func=wrapped_func, iterable=inputs)
    )

    return result


TALLY_TYPE = Dict[str, ElGamalCiphertext]
TALLY_INPUT_TYPE = Union[Dict[str, ElGamalCiphertext], CiphertextBallot]


def sequential_tally(ptallies: Sequence[Optional[TALLY_INPUT_TYPE]]) -> TALLY_TYPE:
    """
    Internal function: sequentially tallies all of the ciphertext ballots, or other partial tallies,
    and returns a partial tally. If any input tally happens to be `None` or an empty dict,
    the result is an empty dict.
    """
    # log_and_print(f"Sequential, local tally with {len(ptallies)} inputs")

    num_nones = sum([1 for p in ptallies if p is None or p == {}])
    if num_nones > 0 in ptallies:
        log_and_print(
            f"Found {num_nones} failed partial tallies, returning an empty tally"
        )
        return {}

    result: TALLY_TYPE = {}
    for ptally in ptallies:
        # we want do our computation purely in terms of TALLY_TYPE, so we'll convert CiphertextBallots
        if isinstance(ptally, CiphertextBallot):
            ptally = ciphertext_ballot_to_dict(ptally)

        if ptally is None:
            # should never happen, but paranoia to keep the type system happy
            return {}

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
) -> TALLY_TYPE:
    """
    This function does a tally of the given list of ballots, returning a dictionary that maps
    from selection object_ids to the ElGamalCiphertext that corresponds to the encrypted tally
    of that selection. An optional `Pool` may be passed in, and it will be used to evaluate
    the ElGamal accumulation in parallel. If it's absent, then the accumulation will happen
    sequentially. Progress bars are not currently supported.
    """

    iter_count = 1
    initial_tallies: Sequence[TALLY_INPUT_TYPE] = ballots

    while True:
        if pool is None or len(initial_tallies) <= BALLOTS_PER_SHARD:
            log_and_print(
                f"tally iteration {iter_count} (FINAL): {len(initial_tallies)} partial tallies"
            )
            return sequential_tally(initial_tallies)

        shards = list(shard_list_uniform(initial_tallies, BALLOTS_PER_SHARD))
        log_and_print(
            f"tally iteration {iter_count}: {len(initial_tallies)} partial tallies --> {len(shards)} shards"
        )
        partial_tallies: Sequence[TALLY_TYPE] = pool.map(
            func=sequential_tally, iterable=shards
        )

        iter_count += 1
        initial_tallies = partial_tallies


@dataclass(eq=True, unsafe_hash=True)
class DecryptOutput:
    object_id: str
    plaintext: int
    decryption_proof: ChaumPedersenDecryptionProof


@dataclass(eq=True, unsafe_hash=True)
class DecryptInput:
    object_id: str
    seed: ElementModQ
    ciphertext: ElGamalCiphertext

    def decrypt(
        self,
        cec: CiphertextElectionContext,
        keypair: ElGamalKeyPair,
    ) -> DecryptOutput:
        plaintext, proof = decrypt_ciphertext_with_proof(
            self.ciphertext, keypair, self.seed, cec.crypto_extended_base_hash
        )
        return DecryptOutput(self.object_id, plaintext, proof)


# object_id -> plaintext, proof
DECRYPT_TALLY_OUTPUT_TYPE = Dict[str, Tuple[int, ChaumPedersenDecryptionProof]]


def _equivalent_decrypt_helper(
    ied: InternalElectionDescription,
    base_hash: ElementModQ,
    public_key: ElementModP,
    secret_key: ElementModQ,
    cballot: CiphertextAcceptedBallot,
) -> PlaintextBallot:  # pragma: no cover
    return get_optional(
        decrypt_ballot_with_secret(
            cballot, ied, base_hash, public_key, secret_key, True, True
        )
    )


# needed for functools.partial, below
def _decrypt(
    cec: CiphertextElectionContext, keypair: ElGamalKeyPair, di: DecryptInput
) -> DecryptOutput:
    return di.decrypt(cec, keypair)


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
        DecryptInput(object_id, seed, tally[object_id])
        for seed, object_id in zip(proof_seeds, tkeys)
    ]

    # Performance note: at this point, the tallies have been computed, so we
    # don't actually have all that much data left to process. There's almost
    # certainly no benefit to distributing this on a cluster.

    if show_progress:  # pragma: no cover
        inputs = tqdm(list(inputs), "Decrypting")

    wrapped_func = functools.partial(_decrypt, cec, keypair)
    result: List[DecryptOutput] = (
        [wrapped_func(x) for x in inputs]
        if pool is None
        else pool.map(func=wrapped_func, iterable=inputs)
    )

    return {r.object_id: (r.plaintext, r.decryption_proof) for r in result}


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

    def to_tally_map(self) -> TALLY_TYPE:
        """
        Converts the SelectionTally to TALLY_TYPE, which is just a dictionary from
        object_ids to ElGamalCiphertexts.
        """

        # This is what we want to write, but mypy confuses the colon for a type declaration
        # rather than key:value syntax for making a dict.
        # result: TALLY_TYPE = {k: map[k].encrypted_tally for k in self.map.keys()}

        result: TALLY_TYPE = {}
        for k in self.map.keys():
            result[k] = self.map[k].encrypted_tally
        return result


def verify_tally_selection_proof(
    public_key: ElementModP, hash_header: Optional[ElementModQ], s: SelectionInfo
) -> bool:  # pragma: no cover
    """
    Given a tally selection, verifies its internal proof is correct.
    """
    return s.is_valid_proof(public_key, hash_header)


def tallies_match(provided_tally: TALLY_TYPE, recomputed_tally: TALLY_TYPE) -> bool:
    """
    Helper function for comparing tallies. Logs useful errors if something doesn't match.
    """
    tally_success = True

    for selection in recomputed_tally.keys():
        recomputed_ciphertext: ElGamalCiphertext = recomputed_tally[selection]
        provided_ciphertext: ElGamalCiphertext = provided_tally[selection]
        if recomputed_ciphertext != provided_ciphertext:
            log_error(
                f"Mismatching ciphertext tallies found for selection ({selection}). Recomputed sum: ({recomputed_ciphertext}), provided sum: ({provided_ciphertext})"
            )
            tally_success = False

    return tally_success


class FastTallyEverythingResults(NamedTuple):
    metadata: ElectionMetadata
    """
    All the public metadata we know about this election. Useful when interacting
    with `election_description`.
    """

    cvr_metadata: pd.DataFrame
    """
    A Pandas DataFrame containing all the metadata about every CVR, but
    excluding all of the voters' individual ballot selections.
    """

    election_description: ElectionDescription
    """
    ElectionGuard top-level data structure that describes everything about the election: 
    the candidates, the parties, and so forth.
    """

    encrypted_ballot_memos: Dict[str, Memo[Optional[CiphertextAcceptedBallot]]]
    """
    Dictionary from ballot ids to memoized ballots. (This allows those ballots to
    be loaded lazily, on-demand.)
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

        wrapped_func = functools.partial(
            verify_tally_selection_proof,
            self.context.elgamal_public_key,
            self.context.crypto_extended_base_hash,
        )
        start = timer()

        inputs = self.tally.map.values()
        if verbose:  # pragma: no cover
            inputs = tqdm(list(inputs), "Tally proof")

        result: List[bool] = (
            [wrapped_func(x) for x in inputs]
            if pool is None
            else pool.map(func=wrapped_func, iterable=inputs)
        )
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
            ballot_func = functools.partial(verify_ballot_proof, self.context)

            ballot_start = timer()
            ballot_result: List[bool] = (
                [ballot_func(x) for x in ballot_iter]
                if pool is None
                else pool.map(func=ballot_func, iterable=ballot_iter)
            )

            ballot_end = timer()
            log_and_print(
                f"Ballot verification rate: {len(self.encrypted_ballots) / (ballot_end - ballot_start): .3f} ballot/sec",
                verbose,
            )

            if False in ballot_result:
                return False

            log_and_print("Recomputing tallies:", verbose)
            recomputed_tally = fast_tally_ballots(self.encrypted_ballots, pool)
            tally_success = tallies_match(self.tally.to_tally_map(), recomputed_tally)

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
        Warning: can be slow, will load all of the files from disk if necessary.
        """

        matching_ballot_ids = self.get_ballot_ids_matching_ballot_styles(ballot_styles)

        matching_ballots = [
            self.get_encrypted_ballot(bid) for bid in matching_ballot_ids
        ]
        matching_ballots_not_none = [x for x in matching_ballots if x is not None]

        # we're going to ignore missing ballots, for now, and march onward; an error will have been logged

        return matching_ballots_not_none

    def get_ballot_ids_matching_ballot_styles(
        self, ballot_styles: Iterable[str]
    ) -> List[str]:
        """
        Returns a list of ballot-id strings having any of the listed ballot styles.
        This will run quickly, as it doesn't need to look at any ballots on disk.
        """
        assert not isinstance(
            ballot_styles, str
        ), "passed a string where a list or set of string was expected"

        matching_ballot_ids = [
            bid
            for bid in self.metadata.ballot_id_to_ballot_type.keys()
            if self.metadata.ballot_id_to_ballot_type[bid] in ballot_styles
        ]

        return matching_ballot_ids

    def equivalent(
        self,
        other: "FastTallyEverythingResults",
        keys: ElGamalKeyPair,
        pool: Optional[Pool] = None,
    ) -> bool:
        """
        The built-in equality checking (__eq__) will determine if two tally results are absolutely
        identical, but with the non-determinism built into ElGamal encryption, we need a somewhat
        more general equality checker that knows how to decrypt the ciphertexts first. Note that
        this method doesn't check the Chaum-Pedersen proofs, and assumes that the tally decryptions
        already present are correct. That makes this method much faster when used in a testing
        context, but more limited if used elsewhere.
        """

        same_metadata = self.metadata == other.metadata

        my_ied = InternalElectionDescription(self.election_description)
        other_ied = InternalElectionDescription(other.election_description)

        same_ied = my_ied == other_ied

        my_cballots = self.encrypted_ballots
        other_cballots = other.encrypted_ballots

        wrapped_func = functools.partial(
            _equivalent_decrypt_helper,
            my_ied,
            self.context.crypto_extended_base_hash,
            keys.public_key,
            keys.secret_key,
        )

        my_cballots_tqdm = tqdm(my_cballots, desc="Equivalent (1/2)")
        other_cballots_tqdm = tqdm(other_cballots, desc="Equivalent (2/2)")

        my_pballots: List[PlaintextBallot] = sorted(
            [wrapped_func(cballot) for cballot in my_cballots_tqdm]
            if not pool
            else pool.map(func=wrapped_func, iterable=my_cballots_tqdm),
            key=lambda x: x.object_id,
        )
        other_pballots: List[PlaintextBallot] = sorted(
            [wrapped_func(cballot) for cballot in other_cballots_tqdm]
            if not pool
            else pool.map(func=wrapped_func, iterable=other_cballots_tqdm),
            key=lambda x: x.object_id,
        )

        same_ballots = my_pballots == other_pballots
        my_decrypted_tallies = {
            k: self.tally.map[k].decrypted_tally for k in self.tally.map.keys()
        }
        other_decrypted_tallies = {
            k: other.tally.map[k].decrypted_tally for k in other.tally.map.keys()
        }
        same_tallies = my_decrypted_tallies == other_decrypted_tallies

        same_cvr_metadata = self.cvr_metadata.equals(other.cvr_metadata)

        success = (
            same_metadata
            and same_ballots
            and same_tallies
            and same_cvr_metadata
            and same_ied
        )
        return success


def verify_ballot_proof(
    cec: CiphertextElectionContext, ballot: CiphertextAcceptedBallot
) -> bool:  # pragma: no cover
    """
    Given a ballot, verify its Chaum-Pedersen proofs.
    """
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
    use_progressbar: bool = True,
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
    cballots = fast_encrypt_ballots(
        ballots, ied, cec, seed_hash, nonces, pool, use_progressbar=use_progressbar
    )
    eg_encrypt_time = timer()

    log_and_print(
        f"Encryption time: {eg_encrypt_time - dlog_prime_time: .3f} sec", verbose
    )
    log_and_print(
        f"Encryption rate: {rows / (eg_encrypt_time - dlog_prime_time): .3f} ballot/sec",
        verbose,
    )

    tally: TALLY_TYPE = fast_tally_ballots(cballots, pool)
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
        print("Decryption & Proofs: ")
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
    accepted_ballots = [ciphertext_ballot_to_accepted(x) for x in cballots]

    return FastTallyEverythingResults(
        metadata=cvrs.metadata,
        cvr_metadata=cvrs.dataframe_without_selections(),
        election_description=ed,
        encrypted_ballot_memos={
            ballot.object_id: make_memo_value(ballot) for ballot in accepted_ballots
        },
        tally=SelectionTally(reported_tally),
        context=cec,
    )


def ballot_memos_from_metadata(
    cvr_metadata: pd.DataFrame, manifest: Manifest
) -> Dict[str, Memo[Optional[CiphertextAcceptedBallot]]]:
    """
    Helper function: given the CVR metadata and a manifest, returns a dict from ballot id's to memos that will
    lazily load the ballots themselves.
    """
    ballot_memos: Dict[str, Memo[Optional[CiphertextAcceptedBallot]]] = {}

    for index, row in cvr_metadata.iterrows():
        ballot_id = row["BallotId"]
        ballot_memo = (
            lambda b, m: make_memo_lambda(lambda: m.load_ciphertext_ballot(b))
        )(ballot_id, manifest)
        ballot_memos[ballot_id] = ballot_memo

    return ballot_memos
