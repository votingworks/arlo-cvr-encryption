import shutil
import unittest
from datetime import timedelta
from typing import List

import ray
from electionguard.election import InternalElectionDescription, ElectionConstants
from electionguard.elgamal import elgamal_keypair_from_secret
from hypothesis import given
from hypothesis import settings, HealthCheck, Phase

from arlo_e2e.admin import ElectionAdmin
from arlo_e2e.arlo_audit import get_ballot_ids_from_imprint_ids
from arlo_e2e.decrypt import (
    decrypt_and_write,
    load_proven_ballot,
    exists_proven_ballot,
    r_verify_proven_ballot_proofs,
)
from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.ray_tally import ray_tally_everything
from arlo_e2e.utils import mkdir_helper
from arlo_e2e_testing.dominion_hypothesis import (
    ballots_and_context,
    DominionBallotsAndContext,
)

_encrypted_ballot_dir = "audit_encrypted_ballots"
_decrypted_ballot_dir = "audit_decrypted_ballots"


class EncryptionAndDecryption(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(_encrypted_ballot_dir, ignore_errors=True)
        shutil.rmtree(_decrypted_ballot_dir, ignore_errors=True)
        ray_init_localhost()

    def tearDown(self) -> None:
        shutil.rmtree(_encrypted_ballot_dir, ignore_errors=True)
        shutil.rmtree(_decrypted_ballot_dir, ignore_errors=True)
        ray.shutdown()

    @given(ballots_and_context(max_rows=20))
    @settings(
        deadline=timedelta(milliseconds=50000),
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=2,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_everything(self, input: DominionBallotsAndContext) -> None:
        # Something of an end-to-end test of an election RLA!

        mkdir_helper(_encrypted_ballot_dir)
        mkdir_helper(_decrypted_ballot_dir)

        cvrs, ed, secret_key, id_map, cec, ballots = input
        ied = InternalElectionDescription(ed)

        tally = ray_tally_everything(
            cvrs,
            secret_key=secret_key,
            use_progressbar=False,
            root_dir=_encrypted_ballot_dir,
        ).to_fast_tally()
        extended_base_hash = tally.context.crypto_extended_base_hash
        keypair = elgamal_keypair_from_secret(secret_key)

        # exercise code from arlo_audit, while we've got the chance
        imprint_ids: List[str] = list(cvrs.data["ImprintedId"])
        bids_from_tally = get_ballot_ids_from_imprint_ids(tally, imprint_ids)

        # verifies there are no duplicate bids
        self.assertEqual(len(ballots), len(set(bids_from_tally)))

        election_admin = ElectionAdmin(keypair, ElectionConstants())
        self.assertTrue(election_admin.is_valid())

        decrypt_and_write(election_admin, tally, bids_from_tally, _decrypted_ballot_dir)

        exists_ballots = [
            exists_proven_ballot(bid, _decrypted_ballot_dir) for bid in bids_from_tally
        ]
        not_exists_ballots = [
            exists_proven_ballot(f"xxx={bid}", _decrypted_ballot_dir)
            for bid in bids_from_tally
        ]

        self.assertTrue(all(exists_ballots))
        self.assertFalse(any(not_exists_ballots))

        # Now, read them all back in again!
        proven_ballots = [
            load_proven_ballot(bid, _decrypted_ballot_dir) for bid in bids_from_tally
        ]
        proven_ballots_not_none = [x for x in proven_ballots if x is not None]
        self.assertEqual(len(proven_ballots), len(proven_ballots_not_none))

        proven_ballots_by_bid = {x.ballot.object_id: x for x in proven_ballots_not_none}

        verifications = [
            r_verify_proven_ballot_proofs.remote(
                extended_base_hash,
                keypair.public_key,
                tally.get_encrypted_ballot(bid),
                proven_ballots_by_bid[bid],
            )
            for bid in bids_from_tally
        ]
        self.assertTrue(all(verifications))

        shutil.rmtree(_encrypted_ballot_dir, ignore_errors=True)
        shutil.rmtree(_decrypted_ballot_dir, ignore_errors=True)
