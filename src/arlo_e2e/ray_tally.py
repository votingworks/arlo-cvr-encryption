# Uses Ray to achieve cluster parallelism for tallying. Note that this code is patterned closely after the
# code in tally.py, and should yield identical results, just much faster on big cluster computers.

from timeit import default_timer as timer
from typing import Optional, List, Tuple, Sequence, Dict, Final, Union

import ray
from arlo_e2e.dominion import DominionCSV
from arlo_e2e.ray_helpers import shard_list
from arlo_e2e.tally import (
    FastTallyEverythingResults,
    _log_and_print,
    TALLY_TYPE,
    DECRYPT_INPUT_TYPE,
    DECRYPT_OUTPUT_TYPE,
    DECRYPT_TALLY_OUTPUT_TYPE,
    SelectionInfo,
    _ciphertext_ballot_to_accepted,
    SelectionTally,
)
from electionguard.ballot import PlaintextBallot, CiphertextBallot
from electionguard.chaum_pedersen import make_constant_chaum_pedersen
from electionguard.election import (
    CiphertextElectionContext,
    InternalElectionDescription,
    make_ciphertext_election_context,
)
from electionguard.elgamal import (
    elgamal_keypair_random,
    elgamal_keypair_from_secret,
    elgamal_add,
)
from electionguard.encrypt import encrypt_ballot
from electionguard.group import ElementModQ, rand_q, add_q, ElementModP
from electionguard.nonces import Nonces
from electionguard.utils import get_optional

# High-level design: What Ray gives us is the ability to call a remote method -- decorated with
# @ray.remote, called with methodname.remote(args), returning a ray.ObjectID immediately. That
# ObjectID is a future or promise for a computation that hasn't (necessarily) happened yet.
# We can then pass it as an argument to another remote method, all without worrying about
# whether the computation has happened or where the ultimate value might be stored.

# Ray tries to be clever enough to do a topological sort on the dependency graph, as it's
# being constructed, and dispatch work to your cluster. It also claims to have some affinity
# features, so it will try to dispatch work to the data, rather than forcing the data to
# migrate to where the compute is.

# One nice feature about Ray is that all the values contained inside a ray.ObjectID are
# immutable. That makes them easy to replicate, anybody who has the answer is as good
# as anybody else, and if you need it recomputed, because a computer failed, then it shouldn't
# be a problem. Functional programming for the win!

# Also, if you've got a big value that you want to spread around, which is what we need to
# do with ElectionGuard ElectionDefinition or CiphertextElectionContext objects, you can
# preemptively shove them into a ray.ObjectID with ray.put(), and then they'll presumably
# be replicated out once. This should improve the performance of our r_encrypt()
# method, and perhaps other such things.

BALLOTS_PER_SHARD: Final[int] = 10
"""
For ballot tallying, we'll "shard" the list of ballots up into groups, and that
will be the work unit.
"""


# Design thoughts: we could redo r_encrypt to take a list of input tuples, and return a list of ballots.
# This would allow for bigger data shards. At least right now, it's convenient that the remote call gives
# us back a list of ObjectIDs, so we can shard that up for the tallying process. Tallying is much faster,
# per ballot, than encrypting.


@ray.remote
def r_encrypt(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input_tuple: Tuple[PlaintextBallot, ElementModQ],
) -> CiphertextBallot:
    # l_ied: InternalElectionDescription = ray.get(ied)
    # l_cec: CiphertextElectionContext = ray.get(cec)
    # l_seed_hash: ElementModQ = ray.get(seed_hash)
    b, n = input_tuple
    return get_optional(
        encrypt_ballot(b, ied, cec, seed_hash, n, should_verify_proofs=False)
    )


def _cballot_to_partial_tally(cballot: CiphertextBallot) -> TALLY_TYPE:
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
def r_tally(ptallies: Sequence[ray.ObjectID]) -> TALLY_TYPE:
    result: TALLY_TYPE = {}
    for tally_remote in ptallies:
        # we want do our computation purely in terms of TALLY_TYPE, so we'll convert CiphertextBallots
        tally = ray.get(tally_remote)
        if isinstance(tally, CiphertextBallot):
            tally = _cballot_to_partial_tally(tally)

        for k in tally.keys():
            if k not in result:
                result[k] = tally[k]
            else:
                nonce_sum, counter_sum = result[k]
                nonce_partial, counter_partial = tally[k]
                nonce_sum = (
                    add_q(nonce_sum, nonce_partial)
                    if (nonce_sum is not None and nonce_partial is not None)
                    else None
                )
                counter_sum = elgamal_add(counter_sum, counter_partial)
                result[k] = (nonce_sum, counter_sum)
    return result


def ray_tally_ballots(cballots: List[ray.ObjectID]) -> TALLY_TYPE:
    """
    The front-end for parallel ballot tallies. The input is a list of ballots, which could be
    really be a list of futures that haven't yet been computed yet. The list is sharded up
    into groups, and each group is tallied independently. Then all those tallies are tallied,
    until we ultimately get down to a singular tally of every ballot.

    :param cballots: a list of ray ObjectIDs that may contain either `CiphertextBallot` or `TALLY_TYPE`
    :return: a ray ObjectID containing `TALLY_TYPE`
    """
    assert len(cballots) > 0, "cannot tally an empty list of ballots"
    if len(cballots) == 1:
        ballot0: Union[CiphertextBallot, TALLY_TYPE] = ray.get(cballots[0])
        if isinstance(ballot0, CiphertextBallot):
            return _cballot_to_partial_tally(ballot0)
        else:
            return ballot0

    shards: Sequence[Sequence[ray.ObjectID]] = shard_list(cballots, BALLOTS_PER_SHARD)
    partial_tallies: List[ray.ObjectID] = [r_tally.remote(shard) for shard in shards]

    assert len(partial_tallies) < len(cballots), "recursion broken"
    return ray_tally_ballots(partial_tallies)


@ray.remote
def r_decrypt(
    public_key: ElementModP, secret_key: ElementModQ, decrypt_input: DECRYPT_INPUT_TYPE
) -> DECRYPT_OUTPUT_TYPE:
    # there aren't really enough elements to be worth the bother, but might as well get some speedup
    object_id, seed, nonce, c = decrypt_input
    plaintext = c.decrypt(secret_key)
    proof = make_constant_chaum_pedersen(c, plaintext, nonce, public_key, seed)
    return object_id, plaintext, proof


def ray_decrypt_tally(
    tally: TALLY_TYPE,
    public_key: ray.ObjectID,
    secret_key: ray.ObjectID,
    proof_seed: ElementModQ,
) -> DECRYPT_TALLY_OUTPUT_TYPE:
    """
    Given a tally, this decrypts the tally
    and returns a dict from selection object_ids to tuples containing the decrypted
    total as well as a Chaum-Pedersen proof that the total corresponds to the ciphertext.

    :param tally: an election tally
    :param public_key: a Ray ObjectID containing an `ElementModP`
    :param secret_key: a Ray ObjectID containing an `ElementModQ`
    :param proof_seed: an ElementModQ
    """
    tkeys = tally.keys()
    proof_seeds: List[ElementModQ] = Nonces(proof_seed)[0 : len(tkeys)]
    inputs = [
        (object_id, seed, tally[object_id][0], tally[object_id][1])
        for seed, object_id in zip(proof_seeds, tkeys)
    ]

    # We can't be lazy here: we need to have all this data in hand so we can
    # rearrange it into a dictionary and return it.
    result: List[DECRYPT_OUTPUT_TYPE] = ray.get(
        [r_decrypt.remote(public_key, secret_key, x) for x in inputs]
    )

    return {k: (p, proof) for k, p, proof in result}


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

    cec = make_ciphertext_election_context(
        number_of_guardians=1,
        quorum=1,
        elgamal_public_key=public_key,
        description_hash=ed.crypto_hash(),
    )
    r_cec = ray.put(cec)

    ied = InternalElectionDescription(ed)
    r_ied = ray.put(ied)

    if seed_hash is None:
        seed_hash = rand_q()
    r_seed_hash = ray.put(seed_hash)
    r_secret_key = ray.put(secret_key)
    r_public_key = ray.put(public_key)

    if master_nonce is None:
        master_nonce = rand_q()
    nonces: List[ElementModQ] = Nonces(master_nonce)[0 : len(ballots)]

    inputs = zip(ballots, nonces)

    start_time = timer()

    # immediately returns a list of futures and launches the computation,
    # so the actual type is List[ray.ObjectID], not List[CiphertextBallot].
    cballot_refs: List[ray.ObjectID] = [
        r_encrypt.remote(r_ied, r_cec, r_seed_hash, t) for t in inputs
    ]

    tally: TALLY_TYPE = ray_tally_ballots(cballot_refs)

    # At this point, all of the CiphertextBallots are spread out across the cluster.
    # We need to bring them back here, so we can ultimately write them out.
    cballots: List[CiphertextBallot] = ray.get(cballot_refs)
    assert (
        len(cballots) == rows
    ), f"missing ballots? only have {len(cballots)} of {rows}"

    tabulate_time = timer()

    _log_and_print(
        f"Encryption and tabulation: {rows} ballots / {tabulate_time - start_time: .3f} sec = {rows / (tabulate_time - start_time): .3f} ballot/sec",
        verbose,
    )

    assert tally is not None, "tally failed!"

    decrypted_tally = ray_decrypt_tally(tally, r_public_key, r_secret_key, seed_hash)

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
