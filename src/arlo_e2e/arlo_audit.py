from io import StringIO
from typing import List, Union, Dict

import pandas as pd
from electionguard.ballot import PlaintextBallot

from arlo_e2e.decrypt import load_proven_ballot
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.ray_tally import RayTallyEverythingResults
from arlo_e2e.tally import FastTallyEverythingResults

_dominion_iid_str = "ImprintedId"
_audit_iid_str = "Imprinted ID"


def get_ballot_ids_from_imprint_ids(
    tally: Union[RayTallyEverythingResults, FastTallyEverythingResults],
    imprint_ids: List[str],
) -> List[str]:
    """
    Given a set of election results and a list of Dominion imprint-ids, returns a list of arlo-e2e
    ballot-ids. (Arlo ballot-ids are used throughout the encryption and decryption process, and
    map one-to-one with the Dominion imprint ids.)
    """
    rows = tally.cvr_metadata.loc[
        tally.cvr_metadata[_dominion_iid_str].isin(imprint_ids)
    ]
    bids = rows["BallotId"]
    return sorted(list(bids))


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


def get_decrypted_ballots_from_ballot_ids(
    bids: List[str], decrypted_dir: str
) -> Dict[str, PlaintextBallot]:
    """
    Given a list ballot ids, loads the decrypted ballots from disk, if they exist, returning
    a dictionary from bids to the PlaintextBallot data. Any missing file will also be absent
    from the dictionary.
    """
    proven_ballots = {
        bid: load_proven_ballot(ballot_object_id=bid, decrypted_dir=decrypted_dir)
        for bid in bids
    }

    return {
        bid: proven_ballots[bid].ballot
        for bid in proven_ballots.keys()
        if proven_ballots[bid] is not None
    }
