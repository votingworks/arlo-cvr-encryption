# Uses Ray to achieve cluster parallelism for tallying. Note that this code is patterned closely after the
# code in tally.py, and should yield identical results, just much faster on big cluster computers.

import pandas as pd
from datetime import datetime
from math import sqrt, ceil
from timeit import default_timer as timer
from typing import Optional, List, Tuple, Sequence, Dict, NamedTuple

import ray
from electionguard.ballot import PlaintextBallot, CiphertextAcceptedBallot
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
from electionguard.group import ElementModQ, rand_q, ElementModP
from electionguard.nonces import Nonces
from electionguard.utils import get_optional

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
    ciphertext_ballot_to_accepted,
    SelectionTally,
    sequential_tally,
    tallies_match,
)
from arlo_e2e.utils import shard_list, flatmap


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

# Our general plan is that we're going to "shard" up the plaintext ballots into lists of ballots,
# and that's the unit of work that we'll dispatch out to remote workers. Those ciphertexts will
# remain on the remote nodes, giving us significant efficiencies as part of the tallying, since
# the initial round of tallying will just be to tally all the ballots in each shard, which will
# still be resident on the node where they were computed. We might call this "tally affinity".

# So, round one of the tally just accumulates everything in each initial shard, which was all
# computed on teh same node. As such, the amount of communication required in the first round of
# the tally is *zero*. In the *second* round, we'd have some fraction as much data to move around
# the network (what fraction? 1 / ballots_per_shard), since the size of a tally subtotal is roughly
# the same as the size of one ciphertext ballot.

# The exact number of ballots per shard should vary with the number of ballots. With huge numbers
# of ballots, tally affinity is going to be really important and the bandwidth savings will
# be significant, whereas for small numbers of ballots we'd rather have smaller batches that
# can run on many more nodes. Solution: we've got a dedicated function to compute the
# ballots_per_shard. We want something roughly on the order of the square root of the number
# of ballots (so, 10k ballots -> 200 shards of 50 ballots per shard). We'll cap the ballots per
# shard at 100, so if we get millions of ballots, we can scale to mammoth clusters while still
# getting most of the benefits of tally affinity.

# The code for ray_tally_ballots and ray_tally_ballot_shards is subtle but really interesting.
# These two functions are mutually recursive, but neither does any actual work. All they're
# doing is establishing futures. This is all lazy computation, so the recursion completes before
# any tallying has begun! Instead, when we try to add up the final list of sub-tallies, they
# won't exist yet, so they'll be dispatched out to nodes for computation. And that will, in
# turn, cause a cascade of dispatches, which will ultimately bottom out at the sharded list
# of encrypted ballots being tallied.

# Nomenclature in this file: methods starting with "ray_" are meant to be called from the
# outside and should "just work". Methods starting with "r_" are "Ray remote methods" that
# are really only for use here.


def ballots_per_shard(num_ballots: int) -> int:
    """
    Computes the number of ballots per shard that we'll use. Scales in proportion
    to the square root of the number of ballots. The result will never be less
    than 4 or greater than 100.
    """
    return min(100, max(4, int(ceil(sqrt(num_ballots) / 2))))


@ray.remote
def r_encrypt(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input_tuples: Sequence[Tuple[PlaintextBallot, ElementModQ]],
) -> List[ray.ObjectRef]:  # pragma: no cover
    """
    Remotely encrypts a list of ballots and their associated nonces. Returns a list of
    Ray ObjectRefs to `CiphertextBallot` objects.
    """
    return [
        ray.put(
            ciphertext_ballot_to_accepted(
                get_optional(
                    encrypt_ballot(
                        b, ied, cec, seed_hash, n, should_verify_proofs=False
                    )
                )
            )
        )
        for b, n in input_tuples
    ]


@ray.remote
def r_tally(ptallies: Sequence[ray.ObjectRef]) -> TALLY_TYPE:  # pragma: no cover
    """
    Remotely tallies a sequence of either encrypted ballots or partial tallies.
    On the remote node, the computation will be sequential.

    If Ray supported type parameters, the actual type of the input would be
    `Sequence[ObjectRef[CiphertextBallot]]]]`.
    """
    return sequential_tally(ray.get(ptallies))


def ray_tally_ballots(ptallies: Sequence[ray.ObjectRef], bps: int) -> ray.ObjectRef:
    """
    Launches a parallel tally reduction tree, with a fanout based on `bps` ballots per shard. Returns
    a Ray ObjectRef reference to the future result, which the caller will then need to call
    `ray.get()` to retrieve. The input is expected to be a sequence of references to ballots.

    If Ray supported type parameters, the actual type of the input would be
    `Sequence[ObjectRef[CiphertextBallot]]]]`.
    """

    if len(ptallies) <= bps:
        return r_tally.remote(ptallies)
    else:
        shards: Sequence[ray.ObjectRef] = [
            ray.put(x) for x in shard_list(ptallies, bps)
        ]
        return ray_tally_ballot_shards(shards, bps)


def ray_tally_ballot_shards(
    partial_tally_shards: Sequence[ray.ObjectRef], bps: int
) -> ray.ObjectRef:
    """
    Launches a parallel tally reduction tree, with a fanout based on `bps` ballots per shard. Returns
    a Ray ObjectRef reference to the future result, which the caller will then need to call
    `ray.get()` to retrieve. The input is expected to be a sequence of Ray ObjectRefs to shards
    (i.e., a sequence of references to sequences of references to ballots).

    If Ray supported type parameters, the actual type of the input would be
    `Sequence[ObjectRef[Sequence[ObjectRef[CiphertextBallot]]]]`.
    """

    partial_tallies: List[ray.ObjectRef] = [
        ray_tally_ballots(ray.get(shard), bps) for shard in partial_tally_shards
    ]
    return r_tally.remote(partial_tallies)


@ray.remote
def r_decrypt(
    cec: CiphertextElectionContext,
    keypair: ElGamalKeyPair,
    decrypt_input: DECRYPT_INPUT_TYPE,
) -> DECRYPT_OUTPUT_TYPE:  # pragma: no cover
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

    inputs = list(zip(ballots, nonces))
    bps = ballots_per_shard(len(ballots))
    sharded_inputs: Sequence[
        Sequence[Tuple[PlaintextBallot, ElementModQ]]
    ] = shard_list(inputs, bps)

    start_time = timer()

    # If Ray had type parameters, the actual type of cballot_refs would
    # be List[ObjectRef[List[ObjectRef[CiphertextBallot]]]].
    cballot_refs: List[ray.ObjectRef] = [
        r_encrypt.remote(r_ied, r_cec, r_seed_hash, t) for t in sharded_inputs
    ]

    # We're now starting a computation on the tally even though we don't have
    # the ballots computed yet. Ray will deal with scheduling the computation.
    tally: TALLY_TYPE = ray.get(ray_tally_ballot_shards(cballot_refs, bps))

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

    tabulate_time = timer()

    log_and_print(
        f"Encryption and tabulation: {rows} ballots / {tabulate_time - start_time: .3f} sec = {rows / (tabulate_time - start_time): .3f} ballot/sec",
        verbose,
    )

    # cballot_refs is a list of references to remote slices of ballots. We want to get all the *references*
    # back here to the main node without moving the ballots themselves. The only time we'll ever consolidate
    # the actual ballots on a single node is with RayTallyEverythingResults.to_fast_tally, which is meant
    # to be the sort of thing you'd use on a single node to hold everything.

    # If Ray had type parameters, the actual type of flat_ballot_refs would be List[List[ObjectRef[CiphertextBallot]]].
    # and the actual type of flatter_ballot_refs would be List[ObjectRef[CiphertextBallot]].
    flat_ballot_refs: List[List[ray.ObjectRef]] = ray.get(cballot_refs)
    flatter_ballot_refs: List[ray.ObjectRef] = list(
        flatmap(lambda x: x, flat_ballot_refs)
    )

    return RayTallyEverythingResults(
        metadata=cvrs.metadata,
        cvr_metadata=cvrs.dataframe_without_selections(),
        election_description=ed,
        remote_encrypted_ballot_refs=flatter_ballot_refs,
        tally=SelectionTally(reported_tally),
        context=cec,
    )


@ray.remote
def r_verify_tally_selection_proofs(
    public_key: ElementModP,
    hash_header: ElementModQ,
    selections: List[SelectionInfo],
) -> bool:  # pragma: no cover
    """
    Given a list of tally selections, verifies that every one's internal proof is correct.
    """
    results = [s.is_valid_proof(public_key, hash_header) for s in selections]
    return all(results)


@ray.remote
def r_verify_ballot_proofs(
    public_key: ElementModP,
    hash_header: ElementModQ,
    cballot_refs: List[ray.ObjectRef],
) -> bool:  # pragma: no cover
    """
    Given a list of ballots, verify their Chaum-Pedersen proofs.
    """
    cballots: List[CiphertextAcceptedBallot] = ray.get(cballot_refs)
    results = [
        b.is_valid_encryption(b.description_hash, public_key, hash_header)
        for b in cballots
    ]

    return all(results)


class RayTallyEverythingResults(NamedTuple):
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

    tally: SelectionTally
    """
    A mapping from selection object_ids to a structure that includes the encrypted and
    decrypted tallies and a proof of their correspondence.
    """

    context: CiphertextElectionContext
    """
    Cryptographic context used in creating the tally.
    """

    remote_encrypted_ballot_refs: List[ray.ObjectRef]
    """
    List of remote references to CiphertextAcceptedBallots.
    """

    @property
    def num_ballots(self) -> int:
        """
        Returns the number of ballots stored here.
        """
        return len(self.remote_encrypted_ballot_refs)

    @property
    def encrypted_ballots(self) -> List[CiphertextAcceptedBallot]:
        """
        Returns a list of all encrypted ballots, pulling them from their remote
        Ray locations to the local node. This might be very slow for large numbers
        of ballots.
        """
        result: List[CiphertextAcceptedBallot] = ray.get(
            self.remote_encrypted_ballot_refs
        )
        return result

    def equivalent(
        self, other: "RayTallyEverythingResults", keys: ElGamalKeyPair
    ) -> bool:
        """
        Returns whether the two tallies are "equivalent". This might be very
        slow for large numbers of ballots. Currently does not take any advantage
        of Ray for speed.
        """
        return self.to_fast_tally().equivalent(other.to_fast_tally(), keys)

    def all_proofs_valid(
        self,
        verbose: bool = False,
        recheck_ballots_and_tallies: bool = False,
    ) -> bool:
        """
        Checks all the proofs used in this tally, returns True if everything is good.
        Any errors found will be logged. Normally, this only checks the proofs associated
        with the totals. If you want to also recompute the tally (i.e., tabulate the
        encrypted ballots) and verify every individual ballot proof, then set
        `recheck_ballots_and_tallies` to True.
        """

        log_and_print("Verifying proofs:", verbose)

        r_public_key = ray.put(self.context.elgamal_public_key)
        r_hash_header = ray.put(self.context.crypto_extended_base_hash)

        start = timer()
        selections = self.tally.map.values()
        sharded_selections: Sequence[Sequence[SelectionInfo]] = shard_list(
            selections, ballots_per_shard(len(selections))
        )
        results: List[bool] = ray.get(
            [
                r_verify_tally_selection_proofs.remote(
                    r_public_key, r_hash_header, ray.put(s)
                )
                for s in sharded_selections
            ]
        )
        end = timer()

        log_and_print(f"Verification time: {end - start: .3f} sec", verbose)
        log_and_print(
            f"Verification rate: {len(self.tally.map.keys()) / (end - start): .3f} selection/sec",
            verbose,
        )

        if False in results:
            return False

        if recheck_ballots_and_tallies:
            # next, check each individual ballot's proofs; in this case, we're going to always
            # show the progress bar, even if verbose is false
            num_ballots = len(self.remote_encrypted_ballot_refs)
            bps = ballots_per_shard(num_ballots)
            cballot_shards = [
                ray.put(s) for s in shard_list(self.remote_encrypted_ballot_refs, bps)
            ]

            ballot_start = timer()
            ballot_results = ray.get(
                [
                    r_verify_ballot_proofs.remote(r_public_key, r_hash_header, shard)
                    for shard in cballot_shards
                ]
            )
            ballot_end = timer()

            log_and_print(
                f"Ballot verification rate: {num_ballots / (ballot_end - ballot_start): .3f} ballot/sec",
                verbose,
            )

            if False in ballot_results:
                return False

            log_and_print("Recomputing tallies:", verbose)
            recomputed_tally: TALLY_TYPE = ray.get(
                ray_tally_ballot_shards(cballot_shards, bps)
            )

            tally_success = tallies_match(self.tally.to_tally_map(), recomputed_tally)

            if not tally_success:
                return False

        return True

    def to_fast_tally(self) -> FastTallyEverythingResults:
        """
        Converts from the "Ray" tally result to the "Fast" tally result used elsewhere. This will collect
        all of the possibly-remote ballots into a single data structure on the caller's node, so could
        take a while to run. Great for tests and for small numbers of ballots. If you've got a million
        ballots, this could explode the memory of the node where it's running.
        """
        return FastTallyEverythingResults(
            metadata=self.metadata,
            cvr_metadata=self.cvr_metadata,
            election_description=self.election_description,
            tally=self.tally,
            context=self.context,
            encrypted_ballot_memos={
                ballot.object_id: make_memo_value(ballot)
                for ballot in ray.get(self.remote_encrypted_ballot_refs)
            },
        )
