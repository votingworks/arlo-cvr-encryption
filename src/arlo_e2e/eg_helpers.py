from typing import Dict, Tuple

from electionguard.group import ElementModQ
from electionguard.logs import (
    log_info,
    get_file_handler,
    log_handlers,
    log_remove_handler,
    log_add_handler,
)
from electionguard.tally import CiphertextTally


def decrypt_tally_with_secret(
    tally: CiphertextTally, secret_key: ElementModQ
) -> Dict[str, int]:
    """
    Given an ElectionGuard "Tally" structure, returns a dict that
    maps from contest object_ids to their plaintext integer results.
    Useful for testing, but it's preferable (and way faster) to use
    the methods in `tally.py` or `ray_tally.py`.
    """

    # Borrowed from electionguard/tests/test_tally.py
    plaintext_selections: Dict[str, int] = {}
    for _, contest in tally.cast.items():
        for object_id, selection in contest.tally_selections.items():
            plaintext = selection.ciphertext.decrypt(secret_key)
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
        return c, f"{self.prefix}{c:07d}"

    def next(self) -> str:
        """
        Fetches a UID string and increments the internal counter.
        """
        return self.next_int()[1]


def log_nothing_to_stdout() -> None:
    """
    Hacks the ElectionGuard logging infrastructure. The full log still goes to `electionguard.log`,
    but no log info at all goes to stdout. Useful for command-line tools so they're not writing
    things we don't want to bother the users about.
    """

    fh = get_file_handler()
    # sh = get_stream_handler()

    for h in log_handlers():
        log_remove_handler(h)

    log_add_handler(fh)

    assert len(log_handlers()) == 1, "we failed to modify the logger!"


def log_and_print(s: str, verbose: bool = True) -> None:
    """
    Given the string, feeds it to `log_info` and, if `verbose` is True (by default, it is),
    also prints the string to stdout.
    """
    if verbose:  # pragma: no cover
        print(f"    {s}")
    log_info(s)
