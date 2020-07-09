import functools
from multiprocessing import Pool
from typing import Dict, Tuple, Iterator, Optional, List, Iterable

from electionguard.ballot import (
    CiphertextAcceptedBallot,
    CiphertextBallot,
    PlaintextBallot,
)
from electionguard.ballot_store import BallotStore
from electionguard.chaum_pedersen import (
    ConstantChaumPedersenProof,
    make_constant_chaum_pedersen,
)
from electionguard.election import (
    CiphertextElectionContext,
    InternalElectionDescription,
)
from electionguard.elgamal import ElGamalCiphertext, elgamal_add
from electionguard.encrypt import encrypt_ballot
from electionguard.group import ElementModQ, add_q, ElementModP
from electionguard.nonces import Nonces
from electionguard.tally import CiphertextTally
from tqdm import tqdm


def decrypt_with_secret(
    tally: CiphertextTally, secret_key: ElementModQ
) -> Dict[str, int]:
    """
    Given an ElectionGuard "Tally" structure, returns a dict that
    maps from contest object_ids to their plaintext integer results.
    """

    # Borrowed from electionguard/tests/test_tally.py
    plaintext_selections: Dict[str, int] = {}
    for _, contest in tally.cast.items():
        for object_id, selection in contest.tally_selections.items():
            plaintext = selection.message.decrypt(secret_key)
            plaintext_selections[object_id] = plaintext

    return plaintext_selections


def _encrypt(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input: Tuple[PlaintextBallot, ElementModQ],
) -> CiphertextBallot:
    b, n = input
    result = encrypt_ballot(b, ied, cec, seed_hash, n)
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
                if s.object_id not in messages:
                    messages[s.object_id] = []
                messages[s].append((s.nonce, s.message))

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

    return {k: v for k, v in result}


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


def fast_decrypt_tally(
    tally: TALLY_TYPE,
    public_key: ElementModP,
    secret_key: ElementModQ,
    proof_seed: ElementModQ,
    pool: Optional[Pool] = None,
    show_progress: bool = True,
) -> Dict[str, Tuple[int, ConstantChaumPedersenProof]]:
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

    return {k: v for k, v in result}


class UidMaker:
    """
    We're going to need a lot object-ids for the ElectionGuard library, so
    the easiest thing is to have a tool that will wrap up a "prefix" with
    a "counter", letting us ask for the "next" UID any time we want one.
    """

    prefix: str = ""
    counter: int = 0

    def __init__(self, prefix: str):
        self.prefix = prefix
        self.counter = 0

    def next_int(self) -> Tuple[int, str]:
        """
        Fetches a tuple of the current counter and a UID string built from
        that counter. Also increments the internal counter.
        """
        c = self.counter
        self.counter += 1
        return c, f"{self.prefix}{c}"

    def next(self) -> str:
        """
        Fetches a UID string and increments the internal counter.
        """
        return self.next_int()[1]


class BallotStoreProgressBar(BallotStore, Iterable):
    """
    This is a wrapper for a `BallotStore` that delegates everything to the internal BallotStore,
    but automatically fires up a `tqdm` progress bar when that BallotStore is being iterated.
    """

    # Even though we're inheriting from BallotStore, our superclass BallotStore will be empty,
    # and we're actually *delegating* to the BallotStore passed as an argument. The only
    # way we're differing from the passed BallotStore is that the iterator will quietly
    # implement a progress bar.
    _bs: BallotStore

    def __init__(self, ballot_store: BallotStore):
        self._bs = ballot_store
        super().__init__()

    def __iter__(self) -> Iterator[Optional[CiphertextAcceptedBallot]]:
        # First we get the iterator, then by converting to a list first, tqdm can figure
        # out the length, making the progress bar more useful.
        return iter(tqdm(list(iter(self._bs))))

    def set(
        self, ballot_id: str, ballot: Optional[CiphertextAcceptedBallot] = None
    ) -> bool:
        result: bool = self._bs.set(ballot_id, ballot)
        return result

    def all(self) -> List[Optional[CiphertextAcceptedBallot]]:
        result: List[Optional[CiphertextAcceptedBallot]] = self._bs.all()
        return result

    def get(self, ballot_id: str) -> Optional[CiphertextAcceptedBallot]:
        result: Optional[CiphertextAcceptedBallot] = self._bs.get(ballot_id)
        return result

    def exists(self, ballot_id: str) -> Tuple[bool, Optional[CiphertextAcceptedBallot]]:
        result: Tuple[bool, Optional[CiphertextAcceptedBallot]] = self._bs.exists(
            ballot_id
        )
        return result
