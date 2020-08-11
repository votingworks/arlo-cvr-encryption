from dataclasses import dataclass
from secrets import randbelow

from electionguard.election import ElectionConstants
from electionguard.elgamal import (
    ElGamalKeyPair,
    elgamal_keypair_random,
    elgamal_encrypt,
    elgamal_add,
)
from electionguard.group import rand_q
from electionguard.logs import log_error
from electionguard.serializable import Serializable
from electionguard.utils import get_optional


@dataclass
class ElectionAdmin(Serializable):
    """
    This data structure represents the secret state, used by election officials. When written
    to disk, it should be treated as highly sensitive.
    """

    # TODO: add some sort of passphrase-based crypto around the on-disk storage
    keypair: ElGamalKeyPair
    constants: ElectionConstants

    def is_valid(self) -> bool:
        if self.constants != ElectionConstants():
            log_error("Mismatching election constants!")
            return False

        # super-cheesy unit test to make sure keypair works

        m1 = randbelow(5)
        m2 = randbelow(5)
        nonce1 = rand_q()
        nonce2 = rand_q()

        c1 = get_optional(elgamal_encrypt(m1, nonce1, self.keypair.public_key))
        c2 = get_optional(elgamal_encrypt(m2, nonce2, self.keypair.public_key))
        csum = elgamal_add(c1, c2)

        sum = csum.decrypt(self.keypair.secret_key)

        if sum != m1 + m2:
            log_error("The given keypair didn't work for basic ElGamal math")
            return False

        return True


def make_fresh_election_admin() -> ElectionAdmin:
    return ElectionAdmin(elgamal_keypair_random(), ElectionConstants())
