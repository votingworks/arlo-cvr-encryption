# Uses Ray to achieve cluster parallelism for tallying; APIs are a bit different than in tally.py
# to work best with the APIs that Ray gives us.
from typing import Optional, List, Tuple, Sequence, Dict, Set, Final, Union

import ray
from timeit import default_timer as timer

from electionguard.ballot import PlaintextBallot, CiphertextBallot
from electionguard.election import CiphertextElectionContext, InternalElectionDescription
from electionguard.elgamal import elgamal_keypair_random, elgamal_keypair_from_secret, elgamal_encrypt, \
    ElGamalCiphertext, elgamal_add
from electionguard.encrypt import encrypt_ballot
from electionguard.group import ElementModQ, int_to_q_unchecked, rand_q, add_q
from electionguard.nonces import Nonces
from electionguard.utils import get_optional

from arlo_e2e.dominion import DominionCSV
from arlo_e2e.ray_helpers import shard_list
from arlo_e2e.tally import FastTallyEverythingResults, _log_and_print, TALLY_TYPE, _accumulate


TALLY_BALLOTS_PER_SHARD: Final[int] = 10


@ray.remote
def remote_encrypt(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input_tuple: Tuple[PlaintextBallot, ElementModQ],
) -> CiphertextBallot:
    b, n = input_tuple
    return get_optional(
        encrypt_ballot(b, ied, cec, seed_hash, n, should_verify_proofs=False)
    )


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


@ray.remote
def remote_reduce_tally(
        ptallies: Sequence[ray.ObjectID]
) -> TALLY_TYPE:
    result: TALLY_TYPE = {}
    for tally_remote in ptallies:
        # we want do our computation purely in terms of TALLY_TYPE, so we'll convert CiphertextBallots
        tally = ray.get(tally_remote)
        if isinstance(tally, CiphertextBallot):
            tally = cballot_to_partial_tally(tally)

        for k in tally.keys():
            if k not in result:
                result[k] = tally[k]
            else:
                nonce_sum, counter_sum = result[k]
                nonce_partial, counter_partial = tally[k]
                nonce_sum = add_q(nonce_sum, nonce_partial) if (nonce_sum is not None and nonce_partial is not None) else None
                counter_sum = elgamal_add(counter_sum, counter_partial)
                result[k] = (nonce_sum, counter_sum)
    return result


def tally_ballots(
        cballots: List[ray.ObjectID]
) -> TALLY_TYPE:
    """
    The front-end for parallel ballot tallies. The input is a list of ballots, which could be
    really be a list of futures that haven't yet been computed yet. The list is sharded up
    into groups, and each group is tallied independently. Then all those tallies are tallied,
    until we ultimately get down to a singular tally of every ballot.
    """
    _log_and_print(f"Initial tally of {len(cballots)} ballots!")
    if len(cballots) == 1:
        return cballot_to_partial_tally(ray.get(cballots[0]))

    partial_tallies = cballots
    while len(partial_tallies) > 1:
        shards = shard_list(partial_tallies, TALLY_BALLOTS_PER_SHARD)
        partial_tallies: List[ray.ObjectID] = [remote_reduce_tally.remote(shard) for shard in shards]
        _log_and_print(f"Partial tallies left: {len(partial_tallies)}")

    return ray.get(partial_tallies[0])


def ray_tally_everything(
        cvrs: DominionCSV,
        verbose: bool = True,
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

    For parallelism, Ray is used. Make sure you've called `ray.init()` before calling this.
    """
    rows, cols = cvrs.data.shape

    ed, ballots, id_map = cvrs.to_election_description()
    assert len(ballots) > 0, "can't have zero ballots!"

    if secret_key is None:
        secret_key, public_key = elgamal_keypair_random()

    else:
        tmp = elgamal_keypair_from_secret(secret_key)
        assert tmp is not None, "unexpected failure with keypair computation"
        public_key = tmp.public_key

    cec = ray.put(CiphertextElectionContext(
        number_of_guardians=1,
        quorum=1,
        elgamal_public_key=public_key,
        description_hash=ed.crypto_hash(),
    ))

    ied = ray.put(InternalElectionDescription(ed))

    # REVIEW THIS: is this cryptographically sound? Is the seed_hash properly a secret? Should
    # it go in the output? The nonces are clearly secret. If you know them, you can decrypt.
    if seed_hash is None:
        seed_hash = rand_q()
    if master_nonce is None:
        master_nonce = rand_q()
    nonces: List[ElementModQ] = Nonces(master_nonce)[0 : len(ballots)]

    inputs = zip(ballots, nonces)

    # immediately returns a list of futures and launches the computation,
    # so the actual type is List[ray.ObjectID], not List[CiphertextBallot].
    cballots = [remote_encrypt.remote(ied, cec, seed_hash, t) for t in inputs]

    tally = tally_ballots(cballots)

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
