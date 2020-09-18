# Uses Ray to achieve cluster parallelism for tallying. Note that this code is patterned closely after the
# code in tally.py, and should yield identical results, just much faster on big cluster computers.

from datetime import datetime
from math import sqrt, ceil, floor
from timeit import default_timer as timer
from typing import Optional, List, Tuple, Sequence, Dict, NamedTuple, cast

import pandas as pd
from electionguard.ballot import PlaintextBallot, CiphertextAcceptedBallot
from electionguard.decrypt_with_secrets import (
    decrypt_ciphertext_with_proof,
    ciphertext_ballot_to_dict,
)
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
from electionguard.utils import get_optional, flatmap_optional

import ray
from ray import ObjectRef
from ray.actor import ActorHandle

from arlo_e2e.dominion import DominionCSV
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.manifest import Manifest, make_fresh_manifest, manifest_name_to_filename
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.ray_progress import ProgressBar
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
    ballot_memos_from_metadata,
)
from arlo_e2e.utils import shard_list, mkdir_helper

# Our general plan is that we're going to "shard" up the plaintext ballots into lists of ballots,
# and that's the unit of work that we'll dispatch out to remote workers. Those ciphertexts will
# be written to disk and then we're only working with the much smaller partial tallies, and even
# then, only remote references to them.

# So, round one of the tally just accumulates everything in each initial shard, which was all
# computed on teh same node, which we've implemented as part of the encryption method. After that,
# we're moving data around the network in a tree-like reduction.

# The exact number of ballots per shard should vary with the number of ballots. With huge numbers
# of ballots, tally affinity is going to save us significant bandwidth on the first round of
# the tallying, since it happens as part of r_encrypt_tally_and_write(). After that, we'll
# have something of a reduction tree. Experimentally, this runs much faster than the encryption
# phase.

# Exactly how many ballots (or partial tallies) are computed in a given batch is controlled by
# ballots_per_shard(), which is meant to be relatively small when we have a small number of
# ballots -- giving us as much parallelism as possible -- and a bit larger when we have a huge
# number of ballots -- giving us bigger shards and better locality.

# Nomenclature in this file: methods starting with "ray_" are meant to be called from the
# main node. Methods starting with "r_" are "Ray remote methods". Variables starting with
# "r_" are ObjectRefs to remote values.

# BTW: super-important engineering rule when working with Ray:
#
#     Thou shalt never call ray.get() or ray.put() when on a remote node!
#
# Even though these will work and do exactly what you want when the problem sizes are small, you
# end up in a world of hurt once the problem size scales up. Dealing with these problems ultimately
# shaped a lot of how the code here works.


def ballots_per_shard(num_ballots: int) -> int:
    """
    Computes the number of ballots per shard that we'll use. Scales in proportion
    to the square root of the number of ballots. The result will never be less
    than 2 or greater than 30.
    """
    return min(30, max(2, int(ceil(sqrt(num_ballots) / 30))))


@ray.remote
class ManifestAggregatorActor:
    """
    This is a Ray "actor" to which we'll send all of the partial manifests from within
    r_encrypt_tally_and_write. They're accumulated as they show up.
    """

    aggregate: Manifest

    def __init__(self, root_dir: str) -> None:
        self.aggregate = make_fresh_manifest(root_dir)

    def add(self, m: Manifest) -> None:
        """
        Aggregates this manifest.
        """
        self.aggregate.merge_from(m)

    def result(self) -> Manifest:
        """
        Gets the aggregate of all the partial manifests.
        """

        return self.aggregate


@ray.remote
def r_encrypt_tally_and_write(
    ied: InternalElectionDescription,
    cec: CiphertextElectionContext,
    seed_hash: ElementModQ,
    input_tuples: Sequence[Tuple[PlaintextBallot, ElementModQ]],
    root_dir: Optional[str] = None,
    manifest_aggregator: Optional[ActorHandle] = None,
    progressbar_actor: Optional[ActorHandle] = None,
) -> TALLY_TYPE:  # pragma: no cover
    """
    Remotely encrypts a list of ballots and their associated nonces. If a `root_dir`
    is specified, the encrypted ballots are written to disk, otherwise no disk activity.
    What's returned is a `RemoteTallyResult`. If the ballots were written, the
    `partial_manifest` will be included here, otherwise `None`. The partial tally
    is always included.
    """

    # While it seems like bad software engineering to smash together everything
    # here in a single function, we're trying to minimize the amount of data
    # we're keeping in memory and especially the amount of data that we're moving
    # through Ray's remote object storage system. The CiphertextBallot objects
    # can be a megabyte each, and subsequent computations don't need them
    # directly. Instead, they can be written to disk and simplified to "partial
    # tallies", which are an order of magnitude smaller. We can even do the
    # first round of tallying here on the remote node, further shrinking the
    # volume of data we need to move across the network.

    # All said and done, assuming we're being passed 100 ballots, we're writing
    # 100MB to disk, and extracting a single partial tally, which will be in
    # the tens or hundreds of kbytes.

    # Also, a reasonable question to ask is why we're dealing with the ManifestAggregatorActor
    # rather than just returning a partial manifest as a result. Tried that first,
    # but because we're handling TALLY_TYPE objects via object-refs, this approach
    # avoids entangling the partial tallies with the partial manifests.

    manifest = flatmap_optional(root_dir, lambda d: make_fresh_manifest(d))
    cballots: List[TALLY_TYPE] = []

    for b, n in input_tuples:
        cballot = ciphertext_ballot_to_accepted(
            get_optional(
                encrypt_ballot(b, ied, cec, seed_hash, n, should_verify_proofs=False)
            )
        )

        if manifest is not None:
            manifest.write_ciphertext_ballot(cballot)

        cballots.append(ciphertext_ballot_to_dict(cballot))

        if progressbar_actor is not None:
            progressbar_actor.update.remote(1)

    tally = sequential_tally(cballots)

    if manifest is not None and manifest_aggregator is not None:
        manifest_aggregator.add.remote(manifest)

    return tally


def partial_tally(*ptallies: Optional[TALLY_TYPE]) -> Optional[TALLY_TYPE]:
    """
    This is a front-end for `sequential_tally`, which can be called locally
    (for remote: see `r_partial_tally`).

    The input is a sequence of TALLY_TYPE and the result is also TALLY_TYPE.

    If any of the partial tallies is `None`, the result is an empty dict,
    but still `TALLY_TYPE`.
    """
    assert not isinstance(
        ptallies, Dict
    ), "type failure: got a dict when we should have gotten a sequence"

    if None in ptallies:
        return None

    if len(ptallies) > 0:
        assert isinstance(
            ptallies[0], Dict
        ), "type failure: we were expecting a dict (TALLY_TYPE), not an objectref"

    result: TALLY_TYPE = sequential_tally(ptallies)
    return result


@ray.remote
def r_partial_tally(
    *ptallies: Optional[TALLY_TYPE],
) -> Optional[TALLY_TYPE]:  # pragma: no cover
    """
    This is a front-end for `partial_tally`, that can be called remotely via Ray.
    """
    return partial_tally(*ptallies)


def ray_tally_ballots(ptallies: Sequence[ObjectRef], bps: int) -> TALLY_TYPE:
    """
    Launches a parallel tally reduction tree, with a fanout based on `bps` ballots per shard. Returns
    a Ray ObjectRef reference to the future result, which the caller will then need to call
    `ray.get()` to retrieve. The input is expected to be a sequence of references to ballots.

    If Ray supported type parameters, the actual type of the input would be
    `Sequence[ObjectRef[TALLY_TYPE]]]]`.
    """

    iter_count = 1
    initial_tallies = ptallies

    # The shards used for encryption can be pretty small, since there's so much work
    # being done per shard. For tallying, it's a lot less work, so having a bigger
    # number here speeds things up by having fewer rounds of tally reduction.
    bps = max(10, bps)

    assert not isinstance(
        ptallies, Dict
    ), "type failure: got a dict when we should have gotten a sequence"
    assert bps > 1, f"bps = {bps}, should be greater than 1"

    while True:
        assert not isinstance(
            initial_tallies, Dict
        ), "type error: got a dict when we were expecting a sequence"
        num_tallies = len(initial_tallies)
        if num_tallies <= bps:
            # Run locally; no need for remote dispatch when we're this close to done.
            log_and_print(f"Tally iteration (FINAL): {num_tallies} partial tallies")
            local_tally_copy: Sequence[TALLY_TYPE] = ray.get(initial_tallies)
            result = partial_tally(*local_tally_copy)
            assert result is not None, "unexpected failure in partial_tally"
            return result

        shards: Sequence[Sequence[ObjectRef]] = shard_list(initial_tallies, bps)

        log_and_print(
            f"Tally iteration {iter_count:2d}: {num_tallies:6d} partial tallies --> {len(shards)} shards (bps = {bps})"
        )
        partial_tallies: Sequence[ObjectRef] = [
            r_partial_tally.remote(*shard) for shard in shards
        ]

        # To avoid deeply nested tasks, we're going to wait for this to finish.
        # If you comment out the call to ray.wait(), everything still works, but
        # you can get warnings about too many tasks.
        ray.wait(partial_tallies, num_returns=len(partial_tallies), timeout=None)

        iter_count += 1
        initial_tallies = partial_tallies


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
    cec: ObjectRef,
    keypair: ObjectRef,
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


# TODO: add code here to deal with the manifest and writing files out; borrow from publish.py
def ray_tally_everything(
    cvrs: DominionCSV,
    verbose: bool = True,
    date: Optional[datetime] = None,
    seed_hash: Optional[ElementModQ] = None,
    master_nonce: Optional[ElementModQ] = None,
    secret_key: Optional[ElementModQ] = None,
    root_dir: Optional[str] = None,
) -> "RayTallyEverythingResults":
    """
    This top-level function takes a collection of Dominion CVRs and produces everything that
    we might want for arlo-e2e: a list of encrypted ballots, their encrypted and decrypted tally,
    and proofs of the correctness of the whole thing. The election `secret_key` is an optional
    parameter. If absent, a random keypair is generated and used. Similarly, if a `seed_hash` or
    `master_nonce` is not provided, random ones are generated and used.

    For parallelism, Ray is used. Make sure you've called `ray.init()` or `ray_localhost_init()`
    before calling this.

    If `root_dir` is specified, then the tally is written out to the specified directory, and
    the resulting `RayTallyEverythingResults` object will support the methods that allow those
    ballots to be read back in again. Conversely, if `root_dir` is `None`, then nothing is
    written to disk, and the result will not have access to individual ballots.
    """

    rows, cols = cvrs.data.shape

    if date is None:
        date = datetime.now()

    start_time = timer()
    ed, ballots, id_map = cvrs.to_election_description(date=date)
    setup_time = timer()
    num_ballots = len(ballots)
    assert num_ballots > 0, "can't have zero ballots!"
    log_and_print(
        f"ElectionGuard setup time: {setup_time - start_time: .3f} sec, {num_ballots / (setup_time - start_time):.3f} ballots/sec"
    )

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

    if root_dir is not None:
        mkdir_helper(root_dir)
        r_manifest_aggregator = ManifestAggregatorActor.remote(root_dir)  # type: ignore
    else:
        r_manifest_aggregator = None

    r_root_dir = ray.put(root_dir)

    if seed_hash is None:
        seed_hash = rand_q()
    r_seed_hash = ray.put(seed_hash)
    r_keypair = ray.put(keypair)

    if master_nonce is None:
        master_nonce = rand_q()
    nonces: List[ElementModQ] = Nonces(master_nonce)[0 : len(ballots)]

    inputs = list(zip(ballots, nonces))
    bps = ballots_per_shard(len(ballots))
    assert bps > 1, f"bps = {bps}, should be greater than 1"
    log_and_print(
        f"Total available shards for parallelism: {int(floor(len(inputs) / bps))}",
        verbose,
    )

    sharded_inputs: Sequence[
        Sequence[Tuple[PlaintextBallot, ElementModQ]]
    ] = shard_list(inputs, bps)

    log_and_print("Launching Ray.io remote encryption!")

    start_time = timer()

    pb = ProgressBar(num_ballots, "Ballots")

    partial_tally_refs = [
        r_encrypt_tally_and_write.remote(
            r_ied,
            r_cec,
            r_seed_hash,
            shard,
            r_root_dir,
            r_manifest_aggregator,
            pb.actor,
        )
        for shard in sharded_inputs
    ]

    pb.print_until_done()

    log_and_print("Remote tallying.")
    tally: Optional[TALLY_TYPE] = ray_tally_ballots(partial_tally_refs, bps)
    assert tally is not None, "tally failed!"

    log_and_print("Tally decryption.")
    decrypted_tally: DECRYPT_TALLY_OUTPUT_TYPE = ray_decrypt_tally(
        tally, r_cec, r_keypair, seed_hash
    )

    log_and_print("Validating tally.")

    # Sanity-checking logic: make sure we don't have any unexpected keys, and that the decrypted totals
    # match up with the columns in the original plaintext data.
    for obj_id in decrypted_tally.keys():
        assert obj_id in id_map, "object_id in results that we don't know about!"
        cvr_sum = int(cvrs.data[id_map[obj_id]].sum())
        decryption, proof = decrypted_tally[obj_id]
        assert cvr_sum == decryption, f"decryption failed for {obj_id}"

    final_manifest: Optional[Manifest] = None

    if root_dir is not None:
        final_manifest = ray.get(r_manifest_aggregator.result.remote())
        assert isinstance(
            final_manifest, Manifest
        ), "type error: bad result from manfiest aggregation"

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
        f"Encryption and tabulation: {rows} ballots, {rows / (tabulate_time - start_time): .3f} ballot/sec",
        verbose,
    )

    return RayTallyEverythingResults(
        metadata=cvrs.metadata,
        cvr_metadata=cvrs.dataframe_without_selections(),
        election_description=ed,
        num_ballots=rows,
        manifest=final_manifest,
        tally=SelectionTally(reported_tally),
        context=cec,
    )


@ray.remote
def r_verify_tally_selection_proofs(
    public_key: ElementModP,
    hash_header: ElementModQ,
    *selections: SelectionInfo,
) -> bool:  # pragma: no cover
    """
    Given a list of tally selections, verifies that every one's internal proof is correct.
    """
    results = [s.is_valid_proof(public_key, hash_header) for s in selections]
    return all(results)


def manifest_ballot_name_to_cballot(
    manifest: Manifest, manifest_name: str
) -> Optional[CiphertextAcceptedBallot]:
    """
    Helper to fetch a ballot from disk.
    """
    filename = manifest_name_to_filename(manifest_name)
    ballot = manifest.read_json_file(filename, CiphertextAcceptedBallot)
    return ballot


@ray.remote
def r_verify_ballot_proofs(
    manifest: Manifest,
    public_key: ElementModP,
    hash_header: ElementModQ,
    *cballot_filenames: str,
) -> Optional[TALLY_TYPE]:  # pragma: no cover
    """
    Given a list of ballots, verify their Chaum-Pedersen proofs and redo the tally.
    Returns `None` if anything didn't verify correctly, otherwise a partial tally
    of the ballots (of type `TALLY_TYPE`).
    """

    # We're never moving ciphertext ballots through Ray's remote object system. Instead,
    # we've got filenames coming in. We load the ciphertext ballots, verify them, and
    # we're immediately done with them.  This puts a lot of pressure on the filesystem
    # but S3 buckets, Azure blob storage, etc. can handle it.

    cballots: List[Optional[CiphertextAcceptedBallot]] = [
        manifest.load_ciphertext_ballot(name) for name in cballot_filenames
    ]

    if None in cballots:
        return None

    cballots_not_none: List[CiphertextAcceptedBallot] = cast(
        List[CiphertextAcceptedBallot], cballots
    )

    results = [
        b.is_valid_encryption(b.description_hash, public_key, hash_header)
        for b in cballots_not_none
    ]
    encryptions_valid = all(results)

    if not encryptions_valid:
        return None

    ptally = sequential_tally([ciphertext_ballot_to_dict(b) for b in cballots_not_none])

    return ptally


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

    manifest: Optional[Manifest]
    """
    Cryptographic manifest for all the written ballots.
    """

    num_ballots: int
    """
    Number of ballots.
    """

    @property
    def encrypted_ballots(self) -> List[CiphertextAcceptedBallot]:
        """
        Returns a list of all encrypted ballots. This only works if the ballots
        were written to disk. This will be very slow for large numbers of ballots.
        """

        assert (
            self.manifest is not None
        ), "cannot get to encrypted ballots because they weren't written"

        result: List[Optional[CiphertextAcceptedBallot]] = [
            self.manifest.load_ciphertext_ballot(name)
            for name in self.cvr_metadata["BallotId"]
        ]

        # We're just going to skip over the None values, which is what happens
        # if a hash is invalid or a file is missing, even though we might
        # perhaps want to scream a bit about it.
        return [b for b in result if b is not None]

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

        log_and_print("Verifying proofs.", verbose)

        r_public_key = ray.put(self.context.elgamal_public_key)
        r_hash_header = ray.put(self.context.crypto_extended_base_hash)

        start = timer()
        selections = self.tally.map.values()
        sharded_selections: Sequence[Sequence[SelectionInfo]] = shard_list(
            selections, ballots_per_shard(len(selections))
        )
        results: List[bool] = ray.get(
            [
                r_verify_tally_selection_proofs.remote(r_public_key, r_hash_header, *s)
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
            if self.manifest is None:
                log_and_print("cannot recheck ballots and tallies without a manifest")
                return False

            # next, check each individual ballot's proofs; in this case, we're going to always
            # show the progress bar, even if verbose is false
            num_ballots = self.num_ballots
            bps = ballots_per_shard(num_ballots)

            cballot_manifest_name_shards: Sequence[Sequence[str]] = shard_list(
                self.cvr_metadata["BallotId"], bps
            )

            r_manifest = ray.put(self.manifest)

            ballot_start = timer()
            ballot_results: List[ObjectRef] = [
                r_verify_ballot_proofs.remote(
                    r_manifest, r_public_key, r_hash_header, *shard
                )
                for shard in cballot_manifest_name_shards
            ]
            ray.wait(
                ballot_results,
                num_returns=len(cballot_manifest_name_shards),
                timeout=None,
            )
            ballot_end = timer()

            log_and_print(
                f"Ballot verification rate: {num_ballots / (ballot_end - ballot_start): .3f} ballot/sec",
                verbose,
            )

            log_and_print("Recomputing tallies.", verbose)

            recomputed_tally: Optional[TALLY_TYPE] = ray_tally_ballots(
                ballot_results, bps
            )
            if not recomputed_tally:
                return False

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
        assert (
            self.manifest is not None
        ), "cannot convert to fast tally without a manifest"

        ballot_memos = ballot_memos_from_metadata(self.cvr_metadata, self.manifest)

        return FastTallyEverythingResults(
            metadata=self.metadata,
            cvr_metadata=self.cvr_metadata,
            election_description=self.election_description,
            tally=self.tally,
            context=self.context,
            encrypted_ballot_memos=ballot_memos,
        )
