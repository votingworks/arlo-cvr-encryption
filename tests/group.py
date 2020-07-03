# This file contains code that reduces the parameter strength of ElectionGuard, such that everything
# is a 16 or 17-bit number, rather than the cryptographically strong 256 or 4096-bit numbers. Needless
# to say, this makes modular exponentiation a *lot* faster.
from typing import Dict

from electionguard import group, dlog
from electionguard.group import ElementModP, ONE_MOD_P
from electionguard.logs import log_warning


def electionguard_crypto_weak_params():
    log_warning(
        "CRYPTO IS NO LONGER PRODUCTION KEY LENGTH! (But tests will run much faster)"
    )
    group.P = 65543
    group.Q = 32771
    group.R = ((group.P - 1) * pow(group.Q, -1, group.P)) % group.P
    group.G = pow(2, group.R, group.P)
    group.G_INV = pow(group.G, -1, group.P)
    group.Q_MINUS_ONE = group.Q - 1
    dlog.__dlog_cache: Dict[ElementModP, int] = {ONE_MOD_P: 0}
    dlog.__dlog_max_elem = ONE_MOD_P
    dlog.__dlog_max_exp = 0
    log_warning(f"P = {group.P}, Q = {group.Q}")
