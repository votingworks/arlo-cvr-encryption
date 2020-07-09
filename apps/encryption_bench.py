# This is a benchmark that runs as a standalone program. It takes one command-line argument: the name of a "CSV"
# file in Dominion format. It then parses and encrypts the whole thing, including computing the decrypted tallies.
import sys
from multiprocessing import cpu_count
from multiprocessing import Pool
from timeit import default_timer as timer
from typing import List

from electionguard.election import (
    CiphertextElectionContext,
    InternalElectionDescription,
)
from electionguard.elgamal import elgamal_keypair_random, elgamal_encrypt
from electionguard.encrypt import EncryptionDevice
from electionguard.group import ElementModQ, int_to_q_unchecked
from electionguard.nonces import Nonces

from dominion import read_dominion_csv
from eg_helpers import fast_encrypt_ballots, fast_tally_ballots, fast_decrypt_tally


def run_bench(filename: str, pool: Pool) -> None:
    start_time = timer()
    print(f"Benchmarking: {filename}")
    cvrs = read_dominion_csv(filename)
    if cvrs is None:
        print(f"Failed to read {filename}, terminating.")
        exit(1)
    rows, cols = cvrs.data.shape

    parse_time = timer()
    print(f"    Rows: {rows}, cols: {cols}")
    print(f"    Parse time: {parse_time - start_time: .3f} sec")

    ed, ballots, id_map = cvrs.to_election_description()
    assert len(ballots) > 0, "can't have zero ballots!"

    secret_key, public_key = elgamal_keypair_random()
    print(f"    Priming discrete log engine for n={len(ballots)}")
    assert len(ballots) == elgamal_encrypt(
        m=len(ballots), nonce=int_to_q_unchecked(3), public_key=public_key
    ).decrypt(secret_key), "got wrong ElGamal decryption!"

    dlog_prime_time = timer()
    print(f"    DLog prime time: {dlog_prime_time - parse_time: .3f} sec")

    cec = CiphertextElectionContext(
        number_of_guardians=1,
        quorum=1,
        elgamal_public_key=public_key,
        description_hash=ed.crypto_hash(),
    )

    ied = InternalElectionDescription(ed)
    seed_hash = EncryptionDevice("Location").get_hash()
    # not cryptographically sound, but suitable for the benchmark
    nonces: List[ElementModQ] = Nonces(secret_key)[0 : len(ballots)]

    print("    Encrypting: ")
    cballots = fast_encrypt_ballots(ballots, ied, cec, seed_hash, nonces, pool)
    eg_encrypt_time = timer()

    print(f"    Encryption time: {eg_encrypt_time - dlog_prime_time: .3f} sec")
    print(
        f"    Encryption rate: {(eg_encrypt_time - dlog_prime_time) / rows: .3f} sec/ballot"
    )

    print("    Tallying: ")
    tally = fast_tally_ballots(cballots, pool)
    eg_tabulate_time = timer()

    print(f"    Tabulation time: {eg_tabulate_time - eg_encrypt_time: .3f} sec")
    print(
        f"    Tabulation rate: {(eg_tabulate_time - eg_encrypt_time) / rows: .3f} sec/ballot"
    )

    # tally = tally_ballots(ballot_box._store, ied, cec)
    assert tally is not None, "tally failed!"

    print("    Decryption & Proofs: ")
    results = fast_decrypt_tally(tally, public_key, secret_key, seed_hash, pool)
    eg_decryption_time = timer()
    print(f"    Decryption time: {eg_decryption_time - eg_tabulate_time: .3f} sec")
    print(
        f"    Decryption rate: {(eg_decryption_time - eg_tabulate_time) / rows: .3f} sec/ballot"
    )

    for obj_id in results.keys():
        assert obj_id in id_map, "object_id in results that we don't know about!"
        cvr_sum = int(cvrs.data[id_map[obj_id]].sum())
        decryption, proof = results[obj_id]
        assert cvr_sum == decryption, f"decryption failed for {obj_id}"
        assert proof.is_valid(
            tally[obj_id][1], public_key
        ), f"Chaum-Pedersen proof validation failed!"


if __name__ == "__main__":
    print(f"CPUs detected: {cpu_count()}, launching thread pool")
    pool = Pool(cpu_count())

    for arg in sys.argv[1:]:
        run_bench(arg, pool)
