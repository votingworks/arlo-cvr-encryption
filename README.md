# Arlo E2E Tooling

This repository contains a set of standalone tools that can be
used alongside an [Arlo RLA audit](https://voting.works/risk-limiting-audits/).

## Table of contents

- [Why E2E?](#why-e2e?)
- [Command-line tools](#command-line-tools)
- [Implementation thoughts](#implementation-thoughts)

## Why E2E?

In a risk limiting audit, the risk limit itself is a function of the margin of victory. In the hypothetical where an attacker can compromise the tallying process, the attacker can announce a total with a huge margin, and the RLA will then require very few samples. If the real margin was small, the number of samples should have been much larger.

The fix to this is to require the election official to:
- *commit* to the set of all ballots such that they cannot tamper with the set of electronic ballot records afterward (i.e., any request from the RLA, i.e., "give me ballot N" can be proven consistent with the commitment)
- *prove* that the election tally and margins of victory are consistent with this commitment (this is where all the e2e machinery comes into play)

With these, the RLA now has proof that it's working from the right starting point.

This idea, in various forms, has been around for several years. A recent paper from Benaloh, Stark, and Teague ([slides](https://www.e-vote-id.org/wp-content/uploads/2019/10/VAULT.pdf)) has exactly the idea that we want to build.

A nice property of this design is that it's completely "on the side" of the regular RLA process. You can do a RLA without the e2e part, and you still get something useful. The e2e just makes things more robust. You can layer it on to an existing RLA process without requiring changes to how votes are cast and tabulated.

And, of course, if you *do* happen to have voting machines that generate e2e ciphertexts, now those fit in very nicely here, so this scheme provides something of a stepping stone toward e2e technologies that allow voters to verify their votes were correctly tallied.

## Command-line tools

*(None of these exist yet. This is where we think we're going.)*

`arlo_initialize_keys`: Creates a public/private key pair for the election administrator. The private key could eventually be built with secret sharing across trustees, but for version 1 that's irrelevant, since the election administrator already has the plaintext CVRs.

`arlo_encrypt_cvrs`: Input is a file full of CVRs, probably in CSV format along with the public key of the election. Output is a directory full of JSON files, using the serialization support of ElectionGuard. This would include tallies and Chaum-Pedersen proofs. The
output of this tool could be posted on a web site, making it a realization of the _public bulletin board_ concept
that appears widely in the cryptographic voting literature.

`arlo_verify_proofs`: Input is one or more JSON files (the `arlo_encrypt_cvrs` format) along with the public key of the election. Quietly churns through all the Chaum-Pederson proofs and generates a loud warning if any of the proofs aren't verifiable.

`arlo_decrypt_ballots`: Input is one or more JSON files (the `arlo_encrypt_cvrs` format), the *private* key of the election, and the identifier(s) for the ballot(s) to be decrypted. Output is some sort of JSON format containing the plaintext plus the decryption proof.

## Implementation status

The current `arlo-e2e` codebase has detailed unit tests built with [Hypothesis](https://hypothesis.readthedocs.io/en/latest/).
It can generate a random set of CVRs for a random election (varying the number
of contests, candidates per contest, and so forth), encrypt them and generate
all the proofs, serialize them to disk, read them all back in again,
and verify all the proofs.

On top of this, we'll build the command-line tools, as above.

At some point really soon, we'll need to support cluster parallelism
for the encryption/tallying process, so we can scale to large elections.

Not in version 1 but on the future wishlist:
- Some sort of binary file format or use of a real database to store all the encrypted CVRs. JSON is inefficient,
  although it's at least amenable to compression.
- Some sort of integration with Arlo, rather than running as a standalone tool.
- Some sort of web frontend, rather than the command-line tools.

This code builds on the [ElectionGuard Python Implementation](https://github.com/microsoft/ElectionGuard-Python),
which itself was built to be a complete implementation of the 
[ElectionGuard spec](https://github.com/microsoft/ElectionGuard-SDK-Specification),
including many features that we're not using here, such as threshold cryptography.

Other libraries that we're *not* using, but ostensibly could at some point:
- [Verificatum](https://www.verificatum.org/)
  - Code is all MIT licensed
  - Core libraries in C
  - Alternative implementations in Java (server-side) and JavaScript (for embedding in a hypothetical voting machine)