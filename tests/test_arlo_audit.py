import shutil
import unittest
from datetime import timedelta
from io import StringIO
from typing import List, Dict

import ray
from electionguard.ballot import PlaintextBallot
from electionguard.decrypt_with_secrets import (
    plaintext_ballot_to_dict,
    ProvenPlaintextBallot,
)
from electionguard.election import InternalElectionDescription, ElectionConstants
from electionguard.elgamal import elgamal_keypair_from_secret
from hypothesis import given
from hypothesis import settings, HealthCheck, Phase

from arlo_cvre.admin import ElectionAdmin
from arlo_cvre.arlo_audit import (
    get_ballot_ids_from_imprint_ids,
    get_imprint_to_ballot_id_map,
    compare_audit_ballots,
    get_decrypted_ballots_with_proofs_from_imprint_ids,
    validate_plaintext_and_encrypted_ballot,
    get_imprint_ids_from_ballot_retrieval_csv,
)
from arlo_cvre.arlo_audit_report import (
    ArloSampledBallot,
    _dominion_iid_str,
    _audit_result,
    _cvr_result,
    _discrepancy,
    _audit_iid_str,
)
from arlo_cvre.decrypt import (
    decrypt_and_write,
)
from arlo_cvre.ray_helpers import ray_init_localhost
from arlo_cvre.ray_tally import ray_tally_everything
from arlo_cvre.tally import FastTallyEverythingResults
from arlo_cvre_testing.dominion_hypothesis import (
    ballots_and_context,
    DominionBallotsAndContext,
)

_encrypted_ballot_dir = "audit_encrypted_ballots"
_decrypted_ballot_dir = "audit_decrypted_ballots"


# this is similar to test_decrypt, but is all about exercising the code in arlo_audit


class TestArloAudit(unittest.TestCase):
    def removeTrees(self) -> None:
        shutil.rmtree(_encrypted_ballot_dir, ignore_errors=True)
        shutil.rmtree(_decrypted_ballot_dir, ignore_errors=True)

    def setUp(self) -> None:
        self.removeTrees()
        ray_init_localhost()

    def tearDown(self) -> None:
        self.removeTrees()
        ray.shutdown()

    @given(ballots_and_context(max_rows=20))
    @settings(
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
        max_examples=4,
        # disabling the "shrink" phase, because it runs very slowly
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target],
    )
    def test_everything(self, input: DominionBallotsAndContext) -> None:
        self.removeTrees()

        cvrs, ed, secret_key, id_map, cec, ballots = input
        ied = InternalElectionDescription(ed)

        # We're not actually using any of the ciphertexts we're about to generate,
        # but we do need the tally structure. Easier to generate all this ciphertext
        # and just ignore it.

        tally = ray_tally_everything(
            cvrs,
            secret_key=secret_key,
            use_progressbar=False,
            root_dir=_encrypted_ballot_dir,
            should_verify_proofs=False,
        ).to_fast_tally()
        keypair = elgamal_keypair_from_secret(secret_key)

        imprint_ids: List[str] = list(cvrs.data["ImprintedId"])
        bids_from_tally = get_ballot_ids_from_imprint_ids(tally, imprint_ids)

        election_admin = ElectionAdmin(keypair, ElectionConstants())
        self.assertTrue(election_admin.is_valid())

        decrypt_and_write(election_admin, tally, bids_from_tally, _decrypted_ballot_dir)

        decrypted_ballots_with_proofs: Dict[
            str, ProvenPlaintextBallot
        ] = get_decrypted_ballots_with_proofs_from_imprint_ids(
            tally, imprint_ids, _decrypted_ballot_dir
        )
        decrypted_ballots: Dict[str, PlaintextBallot] = {
            x: decrypted_ballots_with_proofs[x].ballot
            for x in decrypted_ballots_with_proofs.keys()
        }

        fake_audit_ballots: List[
            ArloSampledBallot
        ] = plaintext_ballots_to_arlo_sampled_ballots(tally, input.ballots)

        comparison_result = compare_audit_ballots(
            tally, decrypted_ballots, fake_audit_ballots
        )
        self.assertEqual([], comparison_result)

        iid_to_audit_ballot_map: Dict[str, ArloSampledBallot] = {
            x.imprintedId: x for x in fake_audit_ballots
        }

        iid_to_bid_map = get_imprint_to_ballot_id_map(tally, imprint_ids)
        for iid in imprint_ids:
            plaintext = decrypted_ballots_with_proofs[iid]
            encrypted = tally.get_encrypted_ballot(iid_to_bid_map[iid])
            self.assertTrue(
                validate_plaintext_and_encrypted_ballot(
                    tally,
                    plaintext,
                    encrypted,
                    verbose=True,
                    arlo_sample=iid_to_audit_ballot_map[iid],
                )
            )

        self.removeTrees()

    # TODO: flip votes in the audit report, the comparison should fail

    def test_audit_ballot_manifest_reader(self) -> None:
        input = StringIO(
            """
Container,Tabulator,Batch Name,Ballot Number,Imprinted ID,Ticket Numbers,Already Audited,Audit Board
101,2,40,26,2-40-26,0.031076785376728041,N,Audit Board #1
101,2,40,28,2-40-28,0.021826135722965789,N,Audit Board #1
101,2,40,45,2-40-45,0.034623708282185027,N,Audit Board #1
101,2,40,49,2-40-49,0.090637005933095012,N,Audit Board #1
101,2,40,53,2-40-53,0.049162872653210574,N,Audit Board #1
101,2,40,59,2-40-59,0.081861274452917595,N,Audit Board #1
101,2,40,61,2-40-61,0.073496959644001595,N,Audit Board #1
101,2,40,72,2-40-72,0.078147659105285294,N,Audit Board #1
101,2,40,86,2-40-86,0.063680993788903031,N,Audit Board #1
"""
        )
        expected = {
            "2-40-26",
            "2-40-28",
            "2-40-45",
            "2-40-49",
            "2-40-53",
            "2-40-59",
            "2-40-61",
            "2-40-72",
            "2-40-86",
        }

        result = set(get_imprint_ids_from_ballot_retrieval_csv(input))

        self.assertEqual(expected, result)


def plaintext_ballots_to_arlo_sampled_ballots(
    tally: FastTallyEverythingResults, plaintexts: List[PlaintextBallot]
) -> List[ArloSampledBallot]:
    iids = list(tally.cvr_metadata[_dominion_iid_str])
    iid_to_bid_map = get_imprint_to_ballot_id_map(tally, iids)
    bid_to_iid_map = {iid_to_bid_map[iid]: iid for iid in iids}
    eg_ballot_style_to_normal_ballot_style: Dict[str, str] = {
        tally.metadata.ballot_types[k]: k for k in tally.metadata.ballot_types.keys()
    }

    return [
        plaintext_ballot_to_arlo_sampled_ballot(
            tally, p, bid_to_iid_map, eg_ballot_style_to_normal_ballot_style
        )
        for p in plaintexts
    ]


def plaintext_ballot_to_arlo_sampled_ballot(
    tally: FastTallyEverythingResults,
    plaintext: PlaintextBallot,
    bid_to_iid_map: Dict[str, str],
    eg_ballot_style_to_normal_ballot_style: Dict[str, str],
) -> ArloSampledBallot:
    bid = plaintext.object_id
    iid = bid_to_iid_map[bid]
    ballot_type = plaintext.ballot_style
    contests = tally.metadata.style_map[
        eg_ballot_style_to_normal_ballot_style[ballot_type]
    ]
    plaintext_dict = plaintext_ballot_to_dict(plaintext)

    row: Dict[str, str] = {_audit_iid_str: iid}

    for c in tally.metadata.contest_name_order:
        if c not in contests:
            row[_audit_result + c] = "CONTEST_NOT_ON_BALLOT"
            row[_cvr_result + c] = "CONTEST_NOT_ON_BALLOT"
            row[_discrepancy + c] = "CONTEST_NOT_ON_BALLOT"
        else:
            included_results: List[str] = []
            for s in sorted(
                tally.metadata.contest_map[c], key=lambda m: m.sequence_number
            ):
                if s.object_id in plaintext_dict:
                    plaintext_selection = plaintext_dict[s.object_id]
                    plaintext_int = plaintext_selection.to_int()
                    if plaintext_int == 1:
                        included_results.append(s.choice_name)
                result_str = ", ".join(sorted(included_results))
                row[_audit_result + c] = result_str
                row[_cvr_result + c] = result_str
                row[_discrepancy + c] = None

    return ArloSampledBallot(row)
