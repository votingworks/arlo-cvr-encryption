# this file is a placeholder, just to make sure everything is working
from typing import Optional

from electionguard.elgamal import (
    elgamal_encrypt,
    elgamal_add,
    ElGamalCiphertext,
)
from electionguard.group import ElementModP, ElementModQ, flatmap_optional


def fib(n: int) -> int:
    """
    Why not use the Fibonacci sequence for our hello world tests?
    """
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def elgamal_reencrypt(
    public_key: ElementModP, nonce: ElementModQ, ciphertext: ElGamalCiphertext
) -> Optional[ElGamalCiphertext]:
    """
    Make sure the imports all work by building a function that uses a lot of things from ElectionGuard.
    """
    return flatmap_optional(
        elgamal_encrypt(0, nonce, public_key),
        lambda zero: elgamal_add(zero, ciphertext),
    )
