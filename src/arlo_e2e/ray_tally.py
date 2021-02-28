# Uses Ray to achieve cluster parallelism for tallying. Note that this code is patterned closely after the
# code in tally.py, and should yield identical results, just much faster on big cluster computers.

from datetime import datetime
from multiprocessing.pool import Pool
from timeit import default_timer as timer
from typing import (
    Optional,
    List,
    Dict,
    NamedTuple,
    Tuple,
    Any,
    Final,
)

import pandas as pd
import ray
from electionguard.ballot import CiphertextAcceptedBallot
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
from electionguard.logs import log_error
from electionguard.nonces import Nonces
from ray import ObjectRef
from ray.actor import ActorHandle

from arlo_e2e.dominion import DominionCSV, BallotPlaintextFactory
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.manifest import Manifest
from arlo_e2e.metadata import ElectionMetadata
from arlo_e2e.ray_helpers import ray_wait_for_workers
from arlo_e2e.ray_io import mkdir_helper, ray_write_ciphertext_ballot
from arlo_e2e.ray_map_reduce import MapReduceContext, RayMapReducer
from arlo_e2e.tally import (
    FastTallyEverythingResults,
    TALLY_TYPE,
    DECRYPT_TALLY_OUTPUT_TYPE,
    SelectionInfo,
    ciphertext_ballot_to_accepted,
    SelectionTally,
    sequential_tally,
    tallies_match,
    ballot_memos_from_metadata,
    DecryptOutput,
    DecryptInput,
)
from arlo_e2e.utils import shard_iterable_uniform

# When we're writing files to s3fs, we'll rarely see failures, but with enough files, it's a certainty.
# This is how many times we'll retry each write until it works.
NUM_WRITE_RETRIES: Final = 10

# These constants define how we shard up the ballot processing for the map-reduce pipeline
MAX_CONCURRENT_TASKS: Final = 5000
BALLOTS_PER_SHARD: Final = 4
PARTIAL_TALLIES_PER_SHARD: Final = 10

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


def partial_tally(
    progressbar_actor: Optional[ActorHandle],
    *ptallies: Optional[TALLY_TYPE],
) -> Optional[TALLY_TYPE]:
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
    if progressbar_actor:
        progressbar_actor.update_num_concurrent.remote("Tallies", 1)

    num_ptallies = len(ptallies)

    if num_ptallies > 0:
        assert isinstance(
            ptallies[0], Dict
        ), "type failure: we were expecting a dict (TALLY_TYPE), not an objectref"

    result: TALLY_TYPE = sequential_tally(ptallies)
    if progressbar_actor:
        progressbar_actor.update_completed.remote("Tallies", num_ptallies)
        progressbar_actor.update_num_concurrent.remote("Tallies", -1)
    return result


@ray.remote
def r_decrypt(
    cec: CiphertextElectionContext, keypair: ElGamalKeyPair, di: DecryptInput
) -> Optional[DecryptOutput]:  # pragma: no cover
    """
    Remotely decrypts an ElGamalCiphertext (and its related data -- see DecryptInput)
    and returns the plaintext along with a Chaum-Pedersen proof (see DecryptOutput).
    """
    try:
        plaintext, proof = decrypt_ciphertext_with_proof(
            di.ciphertext, keypair, di.seed, cec.crypto_extended_base_hash
        )
        return DecryptOutput(di.object_id, plaintext, proof)
    except Exception as e:
        log_and_print(f"Unexpected exception in r_decrypt: {e}", True)
        return None


def ray_decrypt_tally(
    tally: TALLY_TYPE,
    cec: ObjectRef,  # ObjectRef[CiphertextElectionContext]
    keypair: ObjectRef,  # ObjectRef[ElGamalKeyPair]
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
    inputs: List[DecryptInput] = [
        DecryptInput(object_id, seed, tally[object_id])
        for seed, object_id in zip(proof_seeds, tkeys)
    ]

    # We can't be lazy here: we need to have all this data in hand so we can
    # rearrange it into a dictionary and return it.
    result: List[Optional[DecryptOutput]] = ray.get(
        [r_decrypt.remote(cec, keypair, x) for x in inputs]
    )

    if None in result:
        log_and_print(
            f"Unexpected failure from in ray_decrypt_tally, returning an empty dict",
            True,
        )
        return {}

    # mypy can't figure this that None isn't here any more, so we need to check for None again
    return {
        r.object_id: (r.plaintext, r.decryption_proof) for r in result if r is not None
    }


# a dict-style plaintext ballot (the output of the CSV parser), and an index into a nonce-generator
TALLY_MAP_INPUT_TYPE = Tuple[Dict[str, Any], int]


class BallotTallyContext(MapReduceContext[TALLY_MAP_INPUT_TYPE, Optional[TALLY_TYPE]]):
    _ied: InternalElectionDescription
    _cec: CiphertextElectionContext
    _seed_hash: ElementModQ
    _root_dir: Optional[str]
    _bpf: BallotPlaintextFactory
    _nonces: Nonces

    def map(self, tuple: TALLY_MAP_INPUT_TYPE) -> Optional[TALLY_TYPE]:
        pballot_dict, nonce_index = tuple
        pballot = self._bpf.row_to_plaintext_ballot(pballot_dict)
        cballot_option = encrypt_ballot(
            pballot,
            self._ied,
            self._cec,
            self._seed_hash,
            self._nonces[nonce_index],
            should_verify_proofs=False,
        )
        if cballot_option is None:
            log_error(f"failed to encrypt ballot {nonce_index}: {pballot_dict}")
            return None

        cballot = ciphertext_ballot_to_accepted(cballot_option)

        if self._root_dir is not None:
            ray_write_ciphertext_ballot(cballot, num_retries=NUM_WRITE_RETRIES)

        return ciphertext_ballot_to_dict(cballot)

    def reduce(self, ptallies: List[Optional[TALLY_TYPE]]) -> Optional[TALLY_TYPE]:
        num_ptallies = len(ptallies)

        if None in ptallies:
            return None

        if num_ptallies > 0:
            assert isinstance(
                ptallies[0], Dict
            ), "type failure: we were expecting a dict (TALLY_TYPE), not an objectref"

        return sequential_tally(ptallies)

    def zero(self) -> Optional[TALLY_TYPE]:
        return {}  # an empty tally dict

    def __init__(
        self,
        ied: InternalElectionDescription,
        cec: CiphertextElectionContext,
        seed_hash: ElementModQ,
        root_dir: Optional[str],
        bpf: BallotPlaintextFactory,
        nonces: Nonces,
    ):
        self._ied = ied
        self._cec = cec
        self._seed_hash = seed_hash
        self._root_dir = root_dir
        self._bpf = bpf
        self._nonces = nonces


def ray_tally_everything(
    cvrs: DominionCSV,
    verbose: bool = True,
    use_progressbar: bool = True,
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

    ray_wait_for_workers(min_workers=2)

    if date is None:
        date = datetime.now()

    if root_dir is not None:
        mkdir_helper(root_dir, num_retries=NUM_WRITE_RETRIES)

    start_time = timer()

    # Performance note: by using to_election_description_ray rather than to_election_description, we're
    # only getting back a list of dictionaries rather than a list of PlaintextBallots. We're pushing that
    # work out into the nodes, where it will run in parallel. The BallotPlaintextFactory wraps up all
    # the (immutable) state necessary to convert from these dicts to PlaintextBallots and is meant to
    # be sent to every node in the cluster.

    ed, bpf, ballot_dicts, id_map = cvrs.to_election_description_ray(date=date)
    setup_time = timer()
    num_ballots = len(ballot_dicts)
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
    r_keypair = ray.put(keypair)

    secret_key, public_key = keypair

    cec = make_ciphertext_election_context(
        number_of_guardians=1,
        quorum=1,
        elgamal_public_key=public_key,
        description_hash=ed.crypto_hash(),
    )
    r_cec = ray.put(cec)

    ied = InternalElectionDescription(ed)

    if seed_hash is None:
        seed_hash = rand_q()

    if master_nonce is None:
        master_nonce = rand_q()

    nonces = Nonces(master_nonce)

    btc = BallotTallyContext(ied, cec, seed_hash, root_dir, bpf, nonces)

    nonce_indices = range(num_ballots)
    inputs = zip(ballot_dicts, nonce_indices)

    rmr = RayMapReducer(
        context=btc,
        use_progressbar=use_progressbar,
        input_description="Ballots",
        reduction_description="Tallies",
        max_tasks=MAX_CONCURRENT_TASKS,
        map_shard_size=BALLOTS_PER_SHARD,
        reduce_shard_size=PARTIAL_TALLIES_PER_SHARD,
    )

    tally = rmr.map_reduce_iterable(inputs, num_inputs=len(ballot_dicts))

    assert tally is not None, "tally failed!"

    log_and_print("Tally decryption.")
    decrypted_tally: DECRYPT_TALLY_OUTPUT_TYPE = ray_decrypt_tally(
        tally, r_cec, r_keypair, seed_hash
    )

    log_and_print("Validating tally.")

    # Sanity-checking logic: make sure we don't have any unexpected keys, and that the decrypted totals
    # match up with the columns in the original plaintext data.
    tally_keys = set(decrypted_tally.keys())
    expected_keys = set(id_map.keys())

    assert tally_keys.issubset(
        expected_keys
    ), f"bad tally keys (actual keys: {sorted(tally_keys)}, expected keys: {sorted(expected_keys)})"

    for obj_id in decrypted_tally.keys():
        cvr_sum = int(cvrs.data[id_map[obj_id]].sum())
        decryption, proof = decrypted_tally[obj_id]
        assert cvr_sum == decryption, f"decryption failed for {obj_id}"

    # Assemble the data structure that we're returning. Having nonces in the ciphertext makes these
    # structures sensitive for writing out to disk, but otherwise they're ready to go.
    log_and_print("Constructing results.")
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

    # TODO: generate manifest

    return RayTallyEverythingResults(
        metadata=cvrs.metadata,
        cvr_metadata=cvrs.dataframe_without_selections(),
        election_description=ed,
        num_ballots=rows,
        manifest=None,  # TODO
        tally=SelectionTally(reported_tally),
        context=cec,
    )


class BallotVerifyContext(MapReduceContext[str, Optional[TALLY_TYPE]]):
    _public_key: ElementModP
    _hash_header: ElementModQ
    _cec: CiphertextElectionContext
    _root_dir: str
    _manifest: Manifest

    def map(self, filename: str) -> Optional[TALLY_TYPE]:
        cballot = self._manifest.load_ciphertext_ballot(filename)

        if cballot is None:
            return None

        is_valid = cballot.is_valid_encryption(
            cballot.description_hash, self._public_key, self._hash_header
        )

        if is_valid:
            return ciphertext_ballot_to_dict(cballot)
        else:
            return None

    def reduce(self, ptallies: List[Optional[TALLY_TYPE]]) -> Optional[TALLY_TYPE]:
        num_ptallies = len(ptallies)

        if None in ptallies:
            return None

        if num_ptallies > 0:
            assert isinstance(
                ptallies[0], Dict
            ), "type failure: we were expecting a dict (TALLY_TYPE), not an objectref"

        return sequential_tally(ptallies)

    def zero(self) -> Optional[TALLY_TYPE]:
        return {}  # an empty tally dict

    def __init__(
        self,
        public_key: ElementModP,
        hash_header: ElementModQ,
        root_dir: str,
    ):
        self._public_key = public_key
        self._hash_header = hash_header
        self._root_dir = root_dir

        manifest = None  # TODO: make_existing_manifest(root_dir)
        if manifest is None:
            raise RuntimeError("unexpected failure to make a manifest!")
        else:
            self._manifest = manifest  # TODO


@ray.remote
def r_verify_tally_selection_proofs(
    public_key: ElementModP,
    hash_header: ElementModQ,
    *selections: SelectionInfo,
) -> bool:  # pragma: no cover
    """
    Given a list of tally selections, verifies that every one's internal proof is correct.
    """
    try:
        results = [s.is_valid_proof(public_key, hash_header) for s in selections]
        return all(results)
    except Exception as e:
        log_and_print(
            f"Unexpected exception in r_verify_tally_selection_proofs: {e}", True
        )
        return False


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
            self.get_encrypted_ballot(name) for name in self.cvr_metadata["BallotId"]
        ]

        # We're just going to skip over the None values, which is what happens
        # if a hash is invalid or a file is missing, even though we might
        # perhaps want to scream a bit about it.
        return [b for b in result if b is not None]

    def get_encrypted_ballot(
        self, ballot_id: str
    ) -> Optional[CiphertextAcceptedBallot]:
        if self.manifest is None:
            return None
        else:
            return self.manifest.load_ciphertext_ballot(ballot_id)

    def equivalent(
        self,
        other: "RayTallyEverythingResults",
        keys: ElGamalKeyPair,
        pool: Optional[Pool] = None,
    ) -> bool:
        """
        Returns whether the two tallies are "equivalent". This might be very
        slow for large numbers of ballots. Currently does not take any advantage
        of Ray for speed, although does support `multiprocessing` pools.
        """
        return self.to_fast_tally().equivalent(other.to_fast_tally(), keys, pool)

    def all_proofs_valid(
        self,
        verbose: bool = False,
        recheck_ballots_and_tallies: bool = False,
        use_progressbar: bool = True,
    ) -> bool:
        """
        Checks all the proofs used in this tally, returns True if everything is good.
        Any errors found will be logged. Normally, this only checks the proofs associated
        with the totals. If you want to also recompute the tally (i.e., tabulate the
        encrypted ballots) and verify every individual ballot proof, then set
        `recheck_ballots_and_tallies` to True.
        """

        ray_wait_for_workers(min_workers=2)

        log_and_print("Verifying proofs.", verbose)

        r_public_key = ray.put(self.context.elgamal_public_key)
        r_hash_header = ray.put(self.context.crypto_extended_base_hash)

        start = timer()
        selections = self.tally.map.values()
        num_selections = len(selections)
        sharded_selections = shard_iterable_uniform(
            selections, 2, num_inputs=num_selections
        )

        # parallelizing this is overkill, but why not?
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

            ballot_ids = self.cvr_metadata["BallotId"]

            # List[ObjectRef[Optional[TALLY_TYPE]]]
            recomputed_tallies: List[ObjectRef] = []

            ballot_start = timer()

            bvc = BallotVerifyContext(
                self.context.elgamal_public_key,
                self.context.crypto_extended_base_hash,
                self.manifest.root_dir,
            )
            rmr = RayMapReducer(
                context=bvc,
                use_progressbar=use_progressbar,
                input_description="Ballots",
                reduction_description="Tallies",
                max_tasks=MAX_CONCURRENT_TASKS,
                map_shard_size=BALLOTS_PER_SHARD,
                reduce_shard_size=PARTIAL_TALLIES_PER_SHARD,
            )
            recomputed_tally = rmr.map_reduce_list(ballot_ids)

            if not recomputed_tally:
                return False

            ballot_end = timer()

            log_and_print(
                f"Ballot verification rate: {num_ballots / (ballot_end - ballot_start): .3f} ballot/sec",
                True,
            )

            tally_success = tallies_match(self.tally.to_tally_map(), recomputed_tally)

            if not tally_success:
                return False

        return True

    def to_fast_tally(self) -> FastTallyEverythingResults:
        """
        Converts from the "Ray" tally result to the "Fast" tally result used elsewhere. This will create
        make it possible to access individual ballots, but they're only read on-demand. This method
        should return quickly, even though reading the ballots later could be quite slow.
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
