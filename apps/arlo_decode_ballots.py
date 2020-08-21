import argparse
from typing import Optional
from sys import exit

from electionguard.decrypt_with_secrets import (
    plaintext_ballot_to_dict,
    ciphertext_ballot_to_dict,
)
from electionguard.election import InternalElectionDescription
from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.decrypt import (
    load_proven_ballot,
    verify_proven_ballot_proofs,
)
from arlo_e2e.eg_helpers import log_nothing_to_stdout
from arlo_e2e.publish import load_fast_tally
from arlo_e2e.tally import FastTallyEverythingResults

if __name__ == "__main__":
    set_serializers()
    set_deserializers()
    log_nothing_to_stdout()

    parser = argparse.ArgumentParser(
        description="Validates plaintext ballots and decodes to human-readable form"
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
        "ballot_id", type=str, nargs="+", help="ballot identifiers to decode",
    )

    args = parser.parse_args()
    tally_dir = args.tallies
    decrypted_dir = args.decrypted
    ballot_ids = args.ballot_id

    print(f"Loading tallies from {tally_dir}.")
    results: Optional[FastTallyEverythingResults] = load_fast_tally(
        tally_dir, check_proofs=False
    )

    if results is None:
        print(f"Failed to load results from {tally_dir}")
        exit(1)

    for bid in ballot_ids:
        if bid not in results.metadata.ballot_id_to_ballot_type:
            print(f"Ballot id {bid} is not part of the tally")

    encrypted_ballots = [results.get_encrypted_ballot(bid) for bid in ballot_ids]
    if None in encrypted_ballots:
        print("Missing files on disk. Exiting.")
        exit(1)

    plaintext_ballots = [load_proven_ballot(bid, decrypted_dir) for bid in ballot_ids]

    ied = InternalElectionDescription(results.election_description)
    extended_base_hash = results.context.crypto_extended_base_hash

    for encrypted, plaintext in zip(encrypted_ballots, plaintext_ballots):
        if encrypted is None:
            # this would have been caught earlier, will never happen here
            continue

        bid = encrypted.object_id
        if bid in results.metadata.ballot_id_to_ballot_type:
            ballot_type = results.metadata.ballot_id_to_ballot_type[bid]
        else:
            print(f"Ballot: {bid}, Unknown ballot style!")
            continue

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

        print(f"Ballot: {bid}, Style: {ballot_type}, Decryption: {decryption_status}")

        encrypted_ballot_dict = ciphertext_ballot_to_dict(encrypted)
        contests = sorted(results.metadata.style_map[ballot_type])
        for c in results.metadata.contest_name_order:
            if c not in contests:
                continue

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
                                selection_proof_status = (
                                    "" if valid else "(INVALID PROOF)"
                                )
                            else:
                                selection_proof_status = "(MISSING PROOF)"
                        print(
                            f"        {s.to_string_no_contest():30}: {plaintext_int} {selection_proof_status}"
                        )
                    else:
                        print(
                            f"        {s.to_string_no_contest():30}: MISSING PLAINTEXT"
                        )
