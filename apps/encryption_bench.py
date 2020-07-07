# This is a benchmark that runs as a standalone program. It takes one command-line argument: the name of a "CSV"
# file in Dominion format. It then parses and encrypts the whole thing, including computing the decrypted tallies.
import functools
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
from timeit import default_timer as timer

import sys
from typing import Tuple, Optional, TypeVar, List, Callable, Sequence, Iterator
from tqdm import tqdm

from electionguard.ballot import PlaintextBallot, CiphertextBallot
from electionguard.ballot_box import BallotBox
from electionguard.election import (
    CiphertextElectionContext,
    InternalElectionDescription,
)
from electionguard.elgamal import elgamal_keypair_random
from electionguard.encrypt import EncryptionDevice, encrypt_ballot
from electionguard.group import ElementModQ
from electionguard.nonces import Nonces
from electionguard.tally import tally_ballots

from dominion import read_dominion_csv
from eg_helpers import decrypt_with_secret, BallotStoreProgressBar
from utils import parallel_map_with_progress


def encrypt_func(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input: Tuple[PlaintextBallot, ElementModQ],
) -> Optional[CiphertextBallot]:
    b, n = input
    result = encrypt_ballot(b, ied, cec, seed_hash, n)
    assert result is not None, "ballot encryption failed!"
    # print(".", flush=True, end="")
    return result


def run_bench(filename: str) -> None:
    # these must be global so we can get the parallel map to work

    start_time = timer()

    print(f"Benchmarking: {filename}")
    cvrs = read_dominion_csv(filename)
    if cvrs is None:
        print(f"Failed to read {filename}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape

    print(f"{filename}: rows: {rows}, cols: {cols}")

    eg_build_time = timer()
    print(f"    Parse time: {eg_build_time - start_time: .3f} sec")

    ed, ballots, id_map = cvrs.to_election_description()
    assert len(ballots) > 0, "can't have zero ballots!"

    secret_key, public_key = elgamal_keypair_random()
    cec = CiphertextElectionContext(
        number_of_guardians=1,
        quorum=1,
        elgamal_public_key=public_key,
        description_hash=ed.crypto_hash(),
    )

    ied = InternalElectionDescription(ed)
    ballot_box = BallotBox(ied, cec)

    seed_hash = EncryptionDevice("Location").get_hash()
    # not cryptographically sound, but suitable for the benchmark
    nonces: List[ElementModQ] = Nonces(secret_key)[0 : len(ballots)]

    wrapped_func = functools.partial(encrypt_func, ied, cec, seed_hash)

    print("    Encrypting: ")
    ebs = parallel_map_with_progress(wrapped_func, zip(ballots, nonces))
    eg_encrypt_time = timer()
    print(f"    Encryption time: {eg_encrypt_time - eg_build_time: .3f} sec")
    print(
        f"    Encryption rate: {(eg_encrypt_time - eg_build_time) / rows: .3f} sec/ballot"
    )

    print("    Casting: ")
    for eb in tqdm(ebs):
        assert eb is not None, "errors should have terminated before getting here"
        cast_result = ballot_box.cast(eb)
        assert cast_result is not None, "ballot box casting failed!"
    eg_cast_time = timer()
    print(f"    Casting time: {(eg_cast_time - eg_encrypt_time): .3f} sec")
    print(
        f"    Casting rate: {(eg_cast_time - eg_encrypt_time) / rows: .3f} sec/ballot"
    )

    print("    Tallying: ")
    tally = tally_ballots(BallotStoreProgressBar(ballot_box._store), ied, cec)
    # tally = tally_ballots(ballot_box._store, ied, cec)
    assert tally is not None, "tally failed!"
    results = decrypt_with_secret(tally, secret_key)
    eg_tabulate_time = timer()
    print("    Decryption complete.")

    for obj_id in results.keys():
        assert obj_id in id_map, "object_id in results that we don't know about!"
        cvr_sum = int(cvrs.data[id_map[obj_id]].sum())
        decryption = results[obj_id]
        assert cvr_sum == decryption, f"decryption failed for {obj_id}"

    print(f"    Tabulation time: {eg_tabulate_time - eg_cast_time: .3f} sec")
    print(
        f"    Tabulation rate: {(eg_tabulate_time - eg_cast_time) / rows: .3f} sec/ballot"
    )


if __name__ == "__main__":
    print(f"CPUs detected: {cpu_count()}, launching thread pool")

    for arg in sys.argv[1:]:
        run_bench(arg)
