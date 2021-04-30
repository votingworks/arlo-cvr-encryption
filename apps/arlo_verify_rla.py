import argparse
from sys import exit
from typing import Optional, Dict, List

from electionguard.ballot import PlaintextBallot
from electionguard.decrypt_with_secrets import ProvenPlaintextBallot
from electionguard.logs import log_info
from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.arlo_audit import (
    compare_audit_ballots,
    validate_plaintext_and_encrypted_ballot,
    get_imprint_to_ballot_id_map,
    get_decrypted_ballots_with_proofs_from_imprint_ids,
)
from arlo_e2e.arlo_audit_report import (
    ArloSampledBallot,
    arlo_audit_report_to_sampled_ballots,
)
from arlo_e2e.eg_helpers import log_nothing_to_stdout
from arlo_e2e.io import validate_directory_input
from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults

if __name__ == "__main__":
    set_serializers()
    set_deserializers()
    log_nothing_to_stdout()

    parser = argparse.ArgumentParser(
        description="Reads an arlo-e2e tally, decrypted ballots, and Arlo audit report; verifies everything matches"
    )

    parser.add_argument(
        "-t",
        "--tallies",
        type=str,
        default="tally_output",
        help="directory name for where the tally artifacts can be found (default: tally_output)",
    )

    parser.add_argument(
        "-d",
        "--decrypted",
        type=str,
        default="decrypted_ballots",
        help="directory name for where decrypted ballots can be found (default: decrypted_ballots)",
    )

    parser.add_argument(
        "-v",
        "--validate",
        action="store_true",
        help="validates the decrypted ballots are consistent with the original encrypted versions (default: False)",
    )

    parser.add_argument(
        "-r",
        "--root-hash",
        "--root_hash",
        type=str,
        default=None,
        help="optional root hash for the tally directory; if the manifest is tampered, an error is indicated",
    )

    parser.add_argument(
        "audit_report",
        type=str,
        nargs=1,
        help="Arlo audit report (in CSV format) to compare to the tally",
    )

    args = parser.parse_args()

    tally_dir = validate_directory_input(args.tallies, "tally", error_if_absent=True)
    decrypted_dir = validate_directory_input(
        args.decrypted, "decryption", error_if_absent=True
    )
    audit_report_filename = args.audit_report[0]
    validate_decryptions = args.validate
    root_hash = args.root_hash

    print(f"Loading tallies from {tally_dir}.")
    tally: Optional[FastTallyEverythingResults] = load_fast_tally(
        tally_dir, check_proofs=False, root_hash=root_hash
    )

    if tally is None:
        print(f"Failed to load results from {tally_dir}")
        exit(1)

    sampled_ballots: Optional[
        List[ArloSampledBallot]
    ] = arlo_audit_report_to_sampled_ballots(audit_report_filename)
    if sampled_ballots is None:
        print(f"Failed to load audit report from {audit_report_filename}")
        exit(1)

    iid_to_sample = {x.imprintedId: x for x in sampled_ballots}

    iids = [x.imprintedId for x in sampled_ballots]

    decrypted_ballots_with_proofs: Dict[
        str, ProvenPlaintextBallot
    ] = get_decrypted_ballots_with_proofs_from_imprint_ids(
        tally, [x.imprintedId for x in sampled_ballots], decrypted_dir
    )

    decrypted_ballots: Dict[str, PlaintextBallot] = {
        iid: decrypted_ballots_with_proofs[iid].ballot
        for iid in decrypted_ballots_with_proofs.keys()
    }

    failed_iids = compare_audit_ballots(tally, decrypted_ballots, sampled_ballots)

    if failed_iids is None:
        print("Audit failed, exiting.")
        exit(1)
    elif len(failed_iids) > 0:
        print(f"Failed audit for {len(failed_iids)} ballots.")
    else:
        print(
            f"Successful audit for {len(sampled_ballots)} ballots, zero discrepancies!"
        )

    # normally, we only want to print something about the ballots where something went wrong
    printed_iids = iids if validate_decryptions else failed_iids

    iid_to_bid_map = get_imprint_to_ballot_id_map(tally, printed_iids)
    for iid in printed_iids:
        log_info(f"Verifying imprint id: iid")
        plaintext = decrypted_ballots_with_proofs[iid]
        encrypted = tally.get_encrypted_ballot(iid_to_bid_map[iid])
        validate_plaintext_and_encrypted_ballot(
            tally, plaintext, encrypted, verbose=True, arlo_sample=iid_to_sample[iid]
        )
