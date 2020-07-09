from typing import Dict, Tuple

from electionguard.group import ElementModQ
from electionguard.tally import CiphertextTally


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
