from io import StringIO
from typing import List, Union, Dict, Optional, Set

import pandas as pd
from electionguard.ballot import (
    PlaintextBallot,
    PlaintextBallotSelection,
    CiphertextAcceptedBallot,
)
from electionguard.decrypt_with_secrets import (
    ProvenPlaintextBallot,
    plaintext_ballot_to_dict,
    ciphertext_ballot_to_dict,
)
from electionguard.logs import log_error, log_info

from arlo_cvre.arlo_audit_report import (
    ArloSampledBallot,
    _audit_iid_str,
    _dominion_iid_str,
)
from arlo_cvre.decrypt import load_proven_ballot, verify_proven_ballot_proofs
from arlo_cvre.eg_helpers import log_and_print
from arlo_cvre.tally import FastTallyEverythingResults


def get_imprint_to_ballot_id_map(
    tally: FastTallyEverythingResults,
    imprint_ids: List[str],
) -> Dict[str, str]:
    rows = tally.cvr_metadata.loc[
        tally.cvr_metadata[_dominion_iid_str].isin(imprint_ids)
    ]
    pairs: List[Dict[str, str]] = rows[["ImprintedId", "BallotId"]].to_dict(
        orient="records"
    )
    return {d["ImprintedId"]: d["BallotId"] for d in pairs}


def get_ballot_ids_from_imprint_ids(
    tally: FastTallyEverythingResults,
    imprint_ids: List[str],
) -> List[str]:
    """
    Given a set of election results and a list of Dominion imprint-ids, returns a list of arlo-e2e
    ballot-ids. (Arlo-e2e ballot-ids are used throughout the encryption and decryption process, and
    map one-to-one with the Dominion imprint ids.)
    """
    map: Dict[str, str] = get_imprint_to_ballot_id_map(tally, imprint_ids)
    return sorted(map.values())


def get_imprint_ids_from_ballot_retrieval_csv(file: Union[str, StringIO]) -> List[str]:
    """
    Given a filename of an Arlo ballot retrieval manifest, returns the list of imprint ids.
    If something goes wrong, it will be logged and an empty list is returned.
    """
    # When an RLA is going on, we'll get a CSV file that looks like this:

    # Container,Tabulator,Batch Name,Ballot Number,Imprinted ID,Ticket Numbers,Already Audited,Audit Board
    # 101,2,40,26,2-40-26,0.031076785376728041,N,Audit Board #1
    # 101,2,40,28,2-40-28,0.021826135722965789,N,Audit Board #1
    # 101,2,40,45,2-40-45,0.034623708282185027,N,Audit Board #1
    # 101,2,40,49,2-40-49,0.090637005933095012,N,Audit Board #1
    # 101,2,40,53,2-40-53,0.049162872653210574,N,Audit Board #1
    # 101,2,40,59,2-40-59,0.081861274452917595,N,Audit Board #1
    # 101,2,40,61,2-40-61,0.073496959644001595,N,Audit Board #1
    # 101,2,40,72,2-40-72,0.078147659105285294,N,Audit Board #1
    # 101,2,40,86,2-40-86,0.063680993788903031,N,Audit Board #1

    # (This particular "Ticket Numbers" column was corrupted by Excel's failure at recognizing dates, but
    # luckily we don't need it.)

    # In particular, we're going to use the "Imprinted ID" field, which should be unique within the election
    # and which appears both here and in the original CSV files.

    try:
        df = pd.read_csv(file, sep=",")
    except FileNotFoundError:
        log_and_print(f"file not found: {file}")
        return []
    except pd.errors.ParserError:
        log_and_print(f"CSV parsing error: {file}")
        return []

    iids = df[_audit_iid_str]
    return list(iids)


def get_decrypted_ballots_with_proofs_from_imprint_ids(
    tally: FastTallyEverythingResults, imprint_ids: List[str], decrypted_dir: str
) -> Dict[str, ProvenPlaintextBallot]:
    """
    Given a list of imprint-ids, locates and loads the decrypted ballots from disk, if they exist, returning
    a dictionary from imprint-ids to the ProvenPlaintextBallot data. Any missing file will also be absent
    from the dictionary, so double-check the number of keys if you want to check for file errors.
    """
    iid_map = get_imprint_to_ballot_id_map(tally, imprint_ids)

    proven_ballots: Dict[str, Optional[ProvenPlaintextBallot]] = {
        iid: load_proven_ballot(
            ballot_object_id=iid_map[iid], decrypted_dir=decrypted_dir
        )
        for iid in iid_map.keys()
    }

    # For our audit purposes, we don't care about the Chaum-Pedersen proofs, so we're just
    # pulling out the ballot data.
    proven_ballots_not_none: Dict[str, ProvenPlaintextBallot] = {
        iid: proven_ballots[iid]  # type: ignore
        for iid in proven_ballots.keys()
        if proven_ballots[iid] is not None
    }

    return proven_ballots_not_none


def get_decrypted_ballots_from_imprint_ids(
    tally: FastTallyEverythingResults, imprint_ids: List[str], decrypted_dir: str
) -> Dict[str, PlaintextBallot]:
    """
    Given a list of imprint-ids, locates and loads the decrypted ballots from disk, if they exist, returning
    a dictionary from imprint-ids to the PlaintextBallot data. Any missing file will also be absent
    from the dictionary, so double-check the number of keys if you want to check for file errors.
    """
    proven_ballots_not_none = get_decrypted_ballots_with_proofs_from_imprint_ids(
        tally, imprint_ids, decrypted_dir
    )

    proven_ballots_not_none_no_proofs: Dict[str, PlaintextBallot] = {
        iid: proven_ballots_not_none[iid].ballot
        for iid in proven_ballots_not_none.keys()
    }

    return proven_ballots_not_none_no_proofs


def compare_audit_ballots(
    tally: FastTallyEverythingResults,
    decrypted_ballots: Dict[str, PlaintextBallot],
    audit_data: List[ArloSampledBallot],
) -> Optional[List[str]]:
    """
    Returns a list of ballot imprint-ids where the validation failed. An empty-list
    implies everything went perfectly. If any sort of internal error occurred, None
    is returned.

    (So, to check for success, check for the empty list, don't rely on the falsiness
    of empty-list, since None is also falsy. Gotta love Python.)
    """
    all_iids = [ballot.imprintedId for ballot in audit_data]
    all_iids_present = all([iid in decrypted_ballots for iid in all_iids])

    if not all_iids_present:
        missing_iids = [iid for iid in all_iids if iid not in decrypted_ballots]
        log_and_print(
            f"{len(missing_iids)} imprint-ids are missing from the decrypted ballots: {missing_iids}"
        )
        return None

    all_audits = {
        audit_ballot.imprintedId: compare_audit_ballot(
            tally, decrypted_ballots[audit_ballot.imprintedId], audit_ballot
        )
        for audit_ballot in audit_data
    }

    return [iid for iid in sorted(all_iids) if not all_audits[iid]]


def compare_audit_ballot(
    tally: FastTallyEverythingResults,
    ballot: PlaintextBallot,
    audit_ballot: ArloSampledBallot,
) -> bool:
    """
    Returns True if everything matches up. Otherwise, returns False. Errors will also be
    printed and logged.
    """
    iid = audit_ballot.imprintedId
    bid = ballot.object_id
    metadata_row = tally.cvr_metadata[tally.cvr_metadata[_dominion_iid_str] == iid]

    log_info(f"Auditing ballot: {iid}, bid: {bid}")

    if len(metadata_row) == 0:
        err_str = f"imprint-id {iid} not found in election metadata!"
        log_error(err_str)
        return False
    elif len(metadata_row) > 1:
        err_str = f"imprint-id {iid} not uniquely in election metadata!"
        log_error(err_str)
        return False

    metadata_bid = metadata_row["BallotId"].values[0]
    if metadata_bid != bid:
        err_str = f"imprint-id {iid} has bid {bid} in the decrypted ballot versus {metadata_bid} in election metadata!"
        log_error(err_str)
        return False

    # we're going to ignore the "cvr_result" and the "discrepancy" field and just
    # focus on the "audit_result" field, to keep things simple

    audit_result = audit_ballot.audit_result
    bdict: Dict[str, PlaintextBallotSelection] = plaintext_ballot_to_dict(ballot)

    failed_contests: List[str] = []
    for contest_name in audit_result.keys():
        log_info(f"  Auditing contest: {contest_name}")
        winner_str = audit_result[contest_name]

        # skip contests that aren't on the ballot or weren't audited
        if winner_str == "CONTEST_NOT_ON_BALLOT" or winner_str is None:
            continue

        # If there are multiple winners, as in a "vote 3 of n" sort of contest,
        # then they'll be separated by exactly a comma and a space. Notable
        # exception to this is that Arlo reports "BLANK" when there are no
        # choices, so we have to deal with that specially.
        winners = set(winner_str.split(", ")) if winner_str != "BLANK" else {}
        log_info(f"  Winners (audit): {winners if len(winners) > 0 else '<BLANK>'}")

        # now we need to figure out the right key for plaintext ballot
        selection_metadata = tally.metadata.contest_map[contest_name]

        if selection_metadata is None:
            err_str = f"no selection metadata found for contest {contest_name}"
            log_error(err_str)
            return False

        winning_selection_object_ids = [
            s.object_id
            for s in selection_metadata
            if s.contest_name == contest_name and s.choice_name in winners
        ]

        # Arlo-e2e will generate selection names like "Write-in" and "Write-in (2)", but
        # during the RLA those are completely ignored, so we need to filter them out here.
        empty_selection_object_ids = [
            s.object_id
            for s in selection_metadata
            if s.contest_name == contest_name
            and s.choice_name not in winners
            and not s.choice_name.startswith("Write-in")
        ]

        empty_choice_names = [
            s.choice_name
            for s in selection_metadata
            if s.contest_name == contest_name
            and s.choice_name not in winners
            and not s.choice_name.startswith("Write-in")
        ]

        # finally, we can go look inside the ElectionGuard plaintext-ballot structures
        winning_selections: List[PlaintextBallotSelection] = [
            bdict[id] for id in winning_selection_object_ids
        ]

        # weird case: sometimes Arlo-e2e thinks a race is completely absent from a ballot but Arlo
        # thinks that it's present with no selections. This could be a consequence of problems with
        # how Arlo-e2e infers what contests are present on which ballots. Anyway, the workaround is
        # is that little "if id in bdict" bit at the end, which means that in this particular case,
        # we have fewer empties than we think, but the verification will still continue.
        # TODO: go over this with the Arlo people, see in particular ballot #9045 (bid b0009044)
        #   in the Inyo CVR file

        empty_selections: List[PlaintextBallotSelection] = [
            bdict[id] for id in empty_selection_object_ids if id in bdict
        ]

        winners_good = all([s.to_int() == 1 for s in winning_selections])
        empties_good = all([s.to_int() == 0 for s in empty_selections])

        log_info(
            f"  Winners (decrypted): {winners_good}; Empties (decrypted): {empties_good}"
        )

        if not winners_good and empties_good:
            err_str = f"inconsistent audit result for contest ({contest_name})!"
            log_error(err_str)
            failed_contests.append(contest_name)

    if failed_contests:
        log_error(f"Contests with failed audits: {failed_contests}")
        return False
    else:
        return True


def validate_plaintext_and_encrypted_ballot(
    results: FastTallyEverythingResults,
    plaintext: Optional[ProvenPlaintextBallot],
    encrypted: Optional[CiphertextAcceptedBallot],
    verbose: bool = True,
    arlo_sample: Optional[ArloSampledBallot] = None,
) -> bool:
    """
    Returns true if the "proven" plaintext ballot corresponds to the given encrypted ballot.
    False if something didn't match.

    Optional `verbose` flag to print things about the ballots. The Arlo sampled ballot is
    printed alongside the other ballots during this verbose printing process, but is ignored
    during the validation.
    """
    if encrypted is None:
        # this would have been caught earlier, will never happen here
        return False

    bid = encrypted.object_id
    if bid in results.metadata.ballot_id_to_ballot_type:
        ballot_type = results.metadata.ballot_id_to_ballot_type[bid]
    else:
        if verbose:
            print(f"Ballot: {bid}, Unknown ballot style!")
        return False

    if plaintext is None:
        decryption_status = "No decryption available"
    else:
        decryption_status = (
            "Verified"
            if verify_proven_ballot_proofs(
                results.context.crypto_extended_base_hash,
                results.context.elgamal_public_key,
                encrypted,
                plaintext,
            )
            else "INVALID"
        )

    if verbose:
        print(f"Ballot: {bid}, Style: {ballot_type}, Decryption: {decryption_status}")

    encrypted_ballot_dict = ciphertext_ballot_to_dict(encrypted)
    contests = sorted(results.metadata.style_map[ballot_type])

    success = True

    for c in results.metadata.contest_name_order:
        if c not in contests:
            continue
        arlo_expected_winner_set: Optional[Set[str]] = None
        if arlo_sample is not None and c in arlo_sample.audit_result:
            arlo_expected_winner_str: Optional[str] = arlo_sample.audit_result[c]
            if arlo_expected_winner_str is None:
                arlo_expected_winner_set = None
            elif arlo_expected_winner_str == "CONTEST_NOT_ON_BALLOT":
                log_and_print(
                    f"Unexpected to find contest {c} for imprint-id {arlo_sample.imprintedId}"
                )
                arlo_expected_winner_set = None
            elif arlo_expected_winner_str == "BLANK":
                arlo_expected_winner_set = set()
            else:
                arlo_expected_winner_set = set(arlo_expected_winner_str.split(", "))

        if verbose:
            print(f"    {c}")
        if plaintext is not None:
            plaintext_dict = plaintext_ballot_to_dict(plaintext.ballot)
            proofs = plaintext.proofs
            for s in sorted(
                results.metadata.contest_map[c], key=lambda m: m.sequence_number
            ):
                selection_proof_status = ""
                if s.object_id in plaintext_dict:
                    plaintext_selection = plaintext_dict[s.object_id]
                    plaintext_int = plaintext_selection.to_int()
                    if decryption_status == "INVALID":
                        success = False
                        if (
                            s.object_id in proofs
                            and s.object_id in encrypted_ballot_dict
                        ):
                            valid = proofs[s.object_id].is_valid(
                                plaintext_int,
                                encrypted_ballot_dict[s.object_id],
                                results.context.elgamal_public_key,
                                results.context.crypto_extended_base_hash,
                            )
                            selection_proof_status = "" if valid else "(INVALID PROOF)"
                        else:
                            selection_proof_status = "(MISSING PROOF)"
                    if verbose:
                        if arlo_expected_winner_set is not None:
                            arlo_result = (
                                " (RLA Verified)"
                                if (
                                    plaintext_int == 1
                                    and s.choice_name in arlo_expected_winner_set
                                )
                                or (
                                    plaintext_int == 0
                                    and s.choice_name not in arlo_expected_winner_set
                                )
                                else f" (RLA Inconsistency! Expected winners: {arlo_expected_winner_set} ({len(arlo_expected_winner_set)}))"
                            )

                        else:
                            arlo_result = ""
                        print(
                            f"        {s.to_string_no_contest():30}: {plaintext_int} {selection_proof_status}{arlo_result}"
                        )
                else:
                    success = False
                    if verbose:
                        print(
                            f"        {s.to_string_no_contest():30}: MISSING PLAINTEXT"
                        )
    return success
