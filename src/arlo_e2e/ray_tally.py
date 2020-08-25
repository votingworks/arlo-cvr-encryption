# Uses Ray to achieve cluster parallelism for tallying. Note that this code is patterned closely after the
# code in tally.py, and should yield identical results, just much faster on big cluster computers.
from datetime import datetime
from timeit import default_timer as timer
from typing import Optional, List, Tuple, Sequence, Dict, Final, NamedTuple

import ray
from arlo_e2e.dominion import DominionCSV
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.memo import make_memo_value
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.tally import (
    FastTallyEverythingResults,
    TALLY_TYPE,
    DECRYPT_INPUT_TYPE,
    DECRYPT_OUTPUT_TYPE,
    DECRYPT_TALLY_OUTPUT_TYPE,
    SelectionInfo,
    _ciphertext_ballot_to_accepted,
    SelectionTally,
    sequential_tally,
)
from arlo_e2e.utils import shard_list
from electionguard.ballot import (
    PlaintextBallot,
    CiphertextBallot,
    CiphertextAcceptedBallot,
)
from electionguard.decrypt_with_secrets import decrypt_ciphertext_with_proof
from electionguard.election import (
    CiphertextElectionContext,
    InternalElectionDescription,
    make_ciphertext_election_context,
    ElectionDescription,
)
from electionguard.elgamal import (
    elgamal_keypair_random,
    elgamal_keypair_from_secret,
    ElGamalKeyPair,
)
from electionguard.encrypt import encrypt_ballot
from electionguard.group import ElementModQ, rand_q
from electionguard.nonces import Nonces
from electionguard.utils import get_optional

# Also, if you've got a big value that you want to spread around, which is what we need to
# do with ElectionGuard ElectionDefinition or CiphertextElectionContext objects, you can
# preemptively shove them into a ray.ObjectRef with ray.put(), and then they'll presumably
# be replicated out once. This should improve the performance of our r_encrypt()
# method, and perhaps other such things.
from ray import ObjectRef

# High-level design: What Ray gives us is the ability to call a remote method -- decorated with
# @ray.remote, called with methodname.remote(args), returning a ray.ObjectRef immediately. That
# ObjectRef is a future or promise for a computation that hasn't (necessarily) happened yet.
# We can then pass it as an argument to another remote method, all without worrying about
# whether the computation has happened or where the ultimate value might be stored.
# Ray tries to be clever enough to do a topological sort on the dependency graph, as it's
# being constructed, and dispatch work to your cluster. It also claims to have some affinity
# features, so it will try to dispatch work to the data, rather than forcing the data to
# migrate to where the compute is.
# One nice feature about Ray is that all the values contained inside a ray.ObjectRef are
# immutable. That makes them easy to replicate, anybody who has the answer is as good
# as anybody else, and if you need it recomputed, because a computer failed, then it shouldn't
# be a problem. Functional programming for the win!

BALLOTS_PER_SHARD: Final[int] = 10
"""
For ballot tallying, we'll "shard" the list of ballots up into groups, and that
will be the work unit.
"""


# Design thoughts: we could redo r_encrypt to take a list of input tuples, and return a list of ballots.
# This would allow for bigger data shards. At least right now, it's convenient that the remote call gives
# us back a list of ObjectRef, so we can shard that up for the tallying process. Tallying is much faster,
# per ballot, than encrypting, so the sharding we do there is more essential than it would be here.


@ray.remote
def r_strip_nonce(cballot: CiphertextBallot) -> CiphertextAcceptedBallot:
    return _ciphertext_ballot_to_accepted(cballot)


@ray.remote
def r_encrypt(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input_tuple: Tuple[PlaintextBallot, ElementModQ],
) -> CiphertextBallot:
    """
    Remotely encrypts a ballot.
    """
    b, n = input_tuple
    return get_optional(
        encrypt_ballot(b, ied, cec, seed_hash, n, should_verify_proofs=False)
    )


@ray.remote
def r_tally(ptallies: Sequence[ray.ObjectRef]) -> TALLY_TYPE:
    """
    Remotely tallies a sequence of either encrypted ballots or partial tallies.
    On the remote node, the computation will be sequential.
    """
    return sequential_tally(ray.get(ptallies))


def ray_tally_ballots(ptallies: Sequence[ray.ObjectRef],) -> ray.ObjectRef:
    """
    Launches a parallel tally reduction tree, with a fanout of BALLOTS_PER_SHARD. Returns
    a Ray ObjectRef reference to the future result, which the caller will then need to call
    `ray.get()` to retrieve.
    """

    if len(ptallies) <= BALLOTS_PER_SHARD:
        return r_tally.remote(ptallies)
    else:
        shards: Sequence[Sequence[ray.ObjectRef]] = shard_list(
            ptallies, BALLOTS_PER_SHARD
        )
        partial_tallies: List[ray.ObjectRef] = [
            ray_tally_ballots(shard) for shard in shards
        ]
        return r_tally.remote(partial_tallies)


@ray.remote
def r_decrypt(
    cec: CiphertextElectionContext,
    keypair: ElGamalKeyPair,
    decrypt_input: DECRYPT_INPUT_TYPE,
) -> DECRYPT_OUTPUT_TYPE:
    """
    Remotely decrypts an ElGamalCiphertext (and its related data -- see DECRYPT_INPUT_TYPE)
    and returns the plaintext along with a Chaum-Pedersen proof (see DECRYPT_OUTPUT_TYPE).
    """
    object_id, seed, c = decrypt_input
    plaintext, proof = decrypt_ciphertext_with_proof(
        c, keypair, seed, cec.crypto_extended_base_hash
    )
    return object_id, plaintext, proof


def ray_decrypt_tally(
    tally: TALLY_TYPE,
    cec: ray.ObjectRef,
    keypair: ray.ObjectRef,
    proof_seed: ElementModQ,
) -> DECRYPT_TALLY_OUTPUT_TYPE:
    """
    Given a tally, this decrypts the tally
    and returns a dict from selection object_ids to tuples containing the decrypted
    total as well as a Chaum-Pedersen proof that the total corresponds to the ciphertext.

    :param tally: an election tally
    :param cec: a Ray ObjectRef containing a `CiphertextElectionContext`
    :param keypair: a Ray ObjectRef containing an `ElGamalKeyPair`
    :param proof_seed: an ElementModQ
    """
    tkeys = tally.keys()
    proof_seeds: List[ElementModQ] = Nonces(proof_seed)[0 : len(tkeys)]
    inputs: List[DECRYPT_INPUT_TYPE] = [
        (object_id, seed, tally[object_id])
        for seed, object_id in zip(proof_seeds, tkeys)
    ]

    # We can't be lazy here: we need to have all this data in hand so we can
    # rearrange it into a dictionary and return it.
    result: List[DECRYPT_OUTPUT_TYPE] = ray.get(
        [r_decrypt.remote(cec, keypair, x) for x in inputs]
    )

    return {k: (p, proof) for k, p, proof in result}


def ray_tally_everything(
    cvrs: DominionCSV,
    verbose: bool = True,
    date: Optional[datetime] = None,
    seed_hash: Optional[ElementModQ] = None,
    master_nonce: Optional[ElementModQ] = None,
    secret_key: Optional[ElementModQ] = None,
) -> "RayTallyEverythingResults":
    """
    This top-level function takes a collection of Dominion CVRs and produces everything that
    we might want for arlo-e2e: a list of encrypted ballots, their encrypted and decrypted tally,
    and proofs of the correctness of the whole thing. The election `secret_key` is an optional
    parameter. If absent, a random keypair is generated and used. Similarly, if a `seed_hash` or
    `master_nonce` is not provided, random ones are generated and used.

    For parallelism, Ray is used. Make sure you've called `ray.init()` or `ray_localhost_init()`
    before calling this.
    """
    rows, cols = cvrs.data.shape

    if date is None:
        date = datetime.now()

    ed, ballots, id_map = cvrs.to_election_description(date=date)
    assert len(ballots) > 0, "can't have zero ballots!"

    keypair = (
        elgamal_keypair_random()
        if secret_key is None
        else elgamal_keypair_from_secret(secret_key)
    )
    assert keypair is not None, "unexpected failure with keypair computation"
    secret_key, public_key = keypair

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
    r_keypair = ray.put(keypair)

    if master_nonce is None:
        master_nonce = rand_q()
    nonces: List[ElementModQ] = Nonces(master_nonce)[0 : len(ballots)]

    inputs = zip(ballots, nonces)

    start_time = timer()

    # This immediately returns a list of futures and launches the computation,
    # so the actual type is List[ray.ObjectRef], not List[CiphertextBallot].
    cballot_refs: List[ray.ObjectRef] = [
        r_encrypt.remote(r_ied, r_cec, r_seed_hash, t) for t in inputs
    ]

    # We're now starting a computation on the tally even though we don't have
    # the ballots computed yet. Ray will deal with scheduling the computation.
    tally: TALLY_TYPE = ray.get(ray_tally_ballots(cballot_refs))

    # Below: original code to bring all the encrypted ballots back to the
    # main compute node. We don't want to do this, because for millions of
    # ballots, we'd rapidly run out of available memory. Instead, we're
    # maintaining a list of remote references to those ballots (cballot_refs)
    # and processing them remotely.

    # cballots: List[CiphertextBallot] = ray.get(cballot_refs)
    # assert (
    #     len(cballots) == rows
    # ), f"missing ballots? only have {len(cballots)} of {rows}"
    #
    assert tally is not None, "tally failed!"

    decrypted_tally: DECRYPT_TALLY_OUTPUT_TYPE = ray_decrypt_tally(
        tally, r_cec, r_keypair, seed_hash
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
    accepted_ballot_refs = [r_strip_nonce.remote(x) for x in cballot_refs]

    tabulate_time = timer()

    log_and_print(
        f"Encryption and tabulation: {rows} ballots / {tabulate_time - start_time: .3f} sec = {rows / (tabulate_time - start_time): .3f} ballot/sec",
        verbose,
    )

    return RayTallyEverythingResults(
        metadata=cvrs.metadata,
        election_description=ed,
        remote_encrypted_ballot_refs=accepted_ballot_refs,
        tally=SelectionTally(reported_tally),
        context=cec,
    )


class RayTallyEverythingResults(NamedTuple):
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

    tally: SelectionTally
    """
    A mapping from selection object_ids to a structure that includes the encrypted and
    decrypted tallies and a proof of their correspondence.
    """

    context: CiphertextElectionContext
    """
    Cryptographic context used in creating the tally.
    """

    remote_encrypted_ballot_refs: List[ObjectRef]
    """
    List of remote references to CiphertextAcceptedBallots.
    """

    def to_fast_tally(self) -> FastTallyEverythingResults:
        """
        Converts from the "Ray" tally result to the "Fast" tally result used elsewhere. This will collect
        all of the possibly-remote ballots into a single data structure on the caller's node, so could
        potentially take a while to run.
        """
        return FastTallyEverythingResults(
            metadata=self.metadata,
            election_description=self.election_description,
            tally=self.tally,
            context=self.context,
            encrypted_ballot_memos={
                ballot.object_id: make_memo_value(ballot)
                for ballot in ray.get(self.remote_encrypted_ballot_refs)
            },
        )
