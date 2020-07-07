from typing import Dict, Tuple, Iterator, Optional, List, Iterable

from electionguard.ballot import CiphertextAcceptedBallot
from electionguard.ballot_store import BallotStore
from electionguard.group import ElementModQ
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
