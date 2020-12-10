from io import StringIO
from typing import List, Union, Dict, Optional, cast

import pandas as pd
from electionguard.ballot import PlaintextBallot, PlaintextBallotSelection
from electionguard.decrypt_with_secrets import (
    ProvenPlaintextBallot,
    plaintext_ballot_to_dict,
)
from electionguard.logs import log_error, log_info

from arlo_e2e.arlo_audit_report import (
    ArloSampledBallot,
    _audit_iid_str,
    _dominion_iid_str,
)
from arlo_e2e.decrypt import load_proven_ballot
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.tally import FastTallyEverythingResults


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


def get_decrypted_ballots_from_imprint_ids(
    tally: FastTallyEverythingResults, imprint_ids: List[str], decrypted_dir: str
) -> Dict[str, PlaintextBallot]:
    """
    Given a list of imprint-ids, locates and loads the decrypted ballots from disk, if they exist, returning
    a dictionary from imprint-ids to the PlaintextBallot data. Any missing file will also be absent
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
    proven_ballots_not_none_no_proofs = cast(
        Dict[str, PlaintextBallot],
        {
            iid: proven_ballots[iid].ballot  # type: ignore
            for iid in proven_ballots.keys()
            if proven_ballots[iid] is not None
        },
    )

    return proven_ballots_not_none_no_proofs


def compare_audit_ballots(
    tally: FastTallyEverythingResults,
    decrypted_ballots: Dict[str, PlaintextBallot],
    audit_data: List[ArloSampledBallot],
) -> bool:
    """
    Returns true if the audit data and the decrypted plaintext ballots correspond.
    """
    all_iids = [ballot.imprintedId for ballot in audit_data]
    all_iids_present = all([iid in decrypted_ballots for iid in all_iids])

    if not all_iids_present:
        missing_iids = [iid for iid in all_iids if iid not in decrypted_ballots]
        log_and_print(
            f"One or more imprint-ids are missing from the decrypted ballots: {missing_iids}"
        )
        return False

    all_audit_ballots = {
        audit_ballot.imprintedId: audit_ballot for audit_ballot in audit_data
    }

    all_audits = {
        audit_ballot.imprintedId: compare_audit_ballot(
            tally, decrypted_ballots[audit_ballot.imprintedId], audit_ballot
        )
        for audit_ballot in audit_data
    }

    if all(all_audits.values()):
        return True

    # Otherwise, time to print everything that failed
    for iid in sorted(all_iids):
        if not all_audits[iid]:
            print(f"FAILED AUDIT FOR ImprintId {iid}, decrypted ballot is:")
            joined_str = {
                "\n  ".join(
                    plaintext_ballot_to_printable(tally, decrypted_ballots[iid])
                )
            }
            print(f"  {joined_str}\n")
    return False


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

    metadata_bid = metadata_row.get_value(0, "BallotId")
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

        # skip contests that aren't on the ballot
        if winner_str is None:
            continue

        # if there are multiple winners, as in a "vote 3 of n" sort of contest,
        # then they'll be separated by exactly a comma and a space
        winners = set(winner_str.split(", "))
        log_info(f"  Winners (audit): {winners}")

        # now we need to figure out the right key for plaintext ballot
        selection_metadata = tally.metadata.contest_map[contest_name]

        if selection_metadata is None:
            err_str = f"no selection metadata found for contest {contest_name}"
            log_error(err_str)
            return False

        # note special case handling for the audit reporting the the winners are "BLANK",
        # which implies that
        winning_selection_object_ids = (
            [
                s.object_id
                for s in selection_metadata
                if s.contest_name == contest_name and s.choice_name in winners
            ]
            if winners != {"BLANK"}
            else []
        )

        empty_selection_object_ids = [
            s.object_id
            for s in selection_metadata
            if s.contest_name == contest_name and s.choice_name not in winners
        ]

        # finally, we can go look inside the ElectionGuard plaintext-ballot structures
        winning_selections: List[PlaintextBallotSelection] = [
            bdict[id] for id in winning_selection_object_ids
        ]
        empty_selections: List[PlaintextBallotSelection] = [
            bdict[id] for id in empty_selection_object_ids
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


def plaintext_ballot_to_printable(
    tally: FastTallyEverythingResults, plaintext: PlaintextBallot
) -> List[str]:
    """
    Utility function: decodes a PlaintextBallot to human-readable format, returns a list of strings.
    """

    # this code is simplified from arlo_decode_ballots

    # TODO: refactor this into a separate pretty-printers module, since ElectionGuard has no such thing

    result_strs: List[str] = []

    bid = plaintext.object_id
    ballot_type = plaintext.ballot_style

    result_strs.append(f"Ballot: {bid}, Style: {ballot_type}")

    contests = sorted(tally.metadata.style_map[ballot_type])
    for c in tally.metadata.contest_name_order:
        if c not in contests:
            continue

        result_strs.append(f"    {c}")
        plaintext_dict = plaintext_ballot_to_dict(plaintext)
        for s in sorted(tally.metadata.contest_map[c], key=lambda m: m.sequence_number):
            if s.object_id in plaintext_dict:
                plaintext_selection = plaintext_dict[s.object_id]
                plaintext_int = plaintext_selection.to_int()
                result_strs.append(
                    f"        {s.to_string_no_contest():30}: {plaintext_int}"
                )
            else:
                result_strs.append(f"        {s.to_string_no_contest():30}: MISSING")

    return result_strs
