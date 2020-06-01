from typing import NamedTuple, List, Optional

from electionguard.contest import (
    ContestDescription,
    PlaintextVotedContest,
    EncryptedVotedContest,
)
from electionguard.elgamal import ElGamalKeyPair
from electionguard.group import ElementModQ, ElementModP
from electionguard.hash import hash_elems, CryptoHashable
from electionguard.logs import log_error
from electionguard.nonces import Nonces


class ElectionDescription(NamedTuple, CryptoHashable):
    title: str
    contests: List[ContestDescription]

    def crypto_hash(self) -> ElementModQ:
        return hash_elems(self.title, self.contests)


class PlaintextBallot(NamedTuple):
    seed: ElementModQ
    contests: List[PlaintextVotedContest]

    def encrypt(
        self,
        ed: ElectionDescription,
        public_key: ElementModP,
        suppress_validity_check: bool = False,
    ) -> Optional["EncryptedBallot"]:
        """
        Given a PlaintextBallot, the necessary key material, and the corresponding election description,
        encrypts each contest and returns an `EncryptedBallot`.

        :param ed: The election description corresponding to this ballot
        :param public_key: Public key for the election
        :param suppress_validity_check: If true, suppresses validity checks during encryption; only use for testing!
        :return an encrypted ballot, or `None` if something in the input was invalid
        """
        if len(ed.contests) != len(self.contests):
            log_error(
                f"encrypting a ballot: mismatching lengths ({len(ed.contests)} vs. {len(self.contests)}"
            )
            return None

        nonces = Nonces(self.seed)[0:len(ed.contests)]
        ciphertexts = [
            t[1].encrypt(t[0], public_key, t[2], suppress_validity_check)
            for t in zip(ed.contests, self.contests, nonces)
        ]

        if None in ciphertexts:
            return None
        else:
            return EncryptedBallot(self.seed, ciphertexts)


class EncryptedBallot(NamedTuple, CryptoHashable):
    seed: ElementModQ
    contests: List[EncryptedVotedContest]

    def crypto_hash(self) -> ElementModQ:
        return hash_elems(self.seed, self.contests)

    def is_valid(self, ed: ElectionDescription, public_key: ElementModP) -> bool:
        if len(ed.contests) != len(self.contests):
            log_error(
                f"validating an encrypted ballot: mismatching lengths ({len(ed.contests)} vs. {len(self.contests)}"
            )
            return False

        for t in zip(ed.contests, self.contests):
            description = t[0]
            ciphertext = t[1]
            if not ciphertext.is_valid(description, public_key):
                return False
        return True

    def decrypt(
        self,
        ed: ElectionDescription,
        keypair: ElGamalKeyPair,
        suppress_validity_check: bool = False,
    ) -> Optional[PlaintextBallot]:
        if len(ed.contests) != len(self.contests):
            log_error(
                f"decrypting a ballot: mismatching lengths ({len(ed.contests)} vs. {len(self.contests)}"
            )
            return None

        plaintexts = [
            t[1].decrypt(t[0], keypair, suppress_validity_check)
            for t in zip(ed.contests, self.contests)
        ]
        if None in plaintexts:
            return None
        else:
            return PlaintextBallot(self.seed, plaintexts)
