# Arlo E2E Tooling

This repository contains a set of scripts to let one implement VAULT in conjunction with the [risk-limiting audit](https://voting.works/risk-limiting-audits/) application [Arlo](https://github.com/votingworks/arlo).

VAULT (Verifiable Audits Using Limited Transparency) is a method of conducting ballot-comparison risk-limiting audits that lets the public validate the audit, but without having to publish the full list of cast vote records. The original paper describing VAULT can be found [here](https://www.stat.berkeley.edu/~stark/Preprints/vault19.pdf), and slides describing it can be found [here](https://www.e-vote-id.org/wp-content/uploads/2019/10/VAULT.pdf).

## Table of contents

- [Why E2E?](#why-e2e?)
- [Command-line tools](#command-line-tools)
- [Implementation thoughts](#implementation-thoughts)
- [Amazon AWS details](#amazon-aws-s3-ec2-iam-details)

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

`arlo_initialize_keys`: Creates a public/private key pair for the election administrator. 
For future versions of arlo-e2e, threshold cryptography would be interesting because it
reduces the downside risk of the election administrator's key material being stolen.
Of course, that same election administrator already has the plaintext ballots.

`arlo_tally_ballots`: Input is a file full of CVRs, in Dominion format, and the key file
from `arlo_initialize_keys`. Output is a directory full of JSON files, including the 
encrypted ballots, their associated proofs, the tallies, and other useful metadata.
This directory could be posted on a web site, making it a realization of the _public bulletin board_ concept
that appears widely in the cryptographic voting literature. The election official might then
distribute the SHA256 hash of `MANIFEST.json`, which contains SHA256 hashes of
every other JSON file in the directory, allowing for incremental integrity checking
of files as they're read.

`arlo_verify_tally`: Input is a tally directory (the output of `arlo_tally_ballots`). The 
election private key is not needed. This tool verifies that the tally is consistent with all the
encrypted ballots, and that all the proofs verify correctly. This process is something that
a third-party observer would conduct against the public bulletin board.

`arlo_verify_rla`: Input is a tally directory, a decrypted ballots directory, and the
CSV audit file written out by Arlo. Verifies that the proper ballots were decrypted correctly,
and that they match up with what the auditors saw during the RLA.

`arlo_ballot_style_summary`: Input is a tally directory, output is a summary of all the
contests and ballot styles. Demonstrates how to work with the metadata included
in a tally directory.

`arlo_all_ballots_for_contest`: Input is a tally directory, output is a list of every ballot id
for ballots having the desired contest.  Demonstrates how to work with the metadata included
in a tally directory.

`arlo_decrypt_ballots`: Input is one or more ballot identifiers (same as the ballot file names, but without the `.json` suffix), 
the *private* key of the election, and the identifier(s) for the ballot(s) to be decrypted. Output ballots are written
to a separate directory, including both the plaintext and proofs of the plaintext's correctness. These are the ballots
that a "ballot-level comparison audit" would be considering.

`arlo_decrypt_ballots_batch`: Input is an Arlo ballot retrieval manifest, in CSV format, the tally directory, and the decrypted ballot directory. The
ballots from the manifest are decrypted and written out, as with the regular `arlo_decrypt_ballots` command.

`arlo_decode_ballots`: Given some ballot identifiers (as above), prints everything we know about those ballots. If they
were previously decrypted, this will print their decryptions and verify their equivalence proofs. If the proofs don't
check out, this tool flags the issue. (Like `arlo_verify_tally`, this tool would be used by a third-party observer
of an election audit.) For ballots that were never decrypted, we at least print the available metadata for the ballot.

`arlo_write_root_hash`: Given a tally directory, computes the hash of `MANIFEST.json` and
writes that along with any other desired metadata into `root_hash.html` and
a corresponding QRcode embedded as a PNG file. This is something an election official might 
print and give out to members of the press. Many of the above commands take a `--root-hash` argument,
where this root-hash value, if specified, will be checked against the manifest file, thus
verifying the integrity of every file hashed inside the manifest.

## Implementation status

The current `arlo-e2e` codebase has detailed unit tests built with [Hypothesis](https://hypothesis.readthedocs.io/en/latest/).
It can generate a random set of CVRs for a random election (varying the number
of contests, candidates per contest, and so forth), encrypt them and generate
all the proofs, serialize them to disk, read them all back in again,
and verify all the proofs.

On top of this, we have all the command-line tools, listed above, and we have code
that can use all of the cores of a multicore computer to accelerate the process
on a single computer. In progress as well is code to support cluster parallelism
for the encryption/tallying process, so we can scale to large elections.
We already have prototyped tallying code using [Ray](https://ray.io/).

Not in version 1 but on the future wishlist:
- Some sort of binary file format or use of a real database to store all the encrypted CVRs. Gzip on our
  encrypted ballots reduces them to 2/3 or their original human-readable size.
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
  
## Amazon AWS (S3, EC2, IAM) details

This part of the implementation is currently in need of some generalization
and cleanup. But how it works right now.

There are two YAML configurations for Ray in the `cloud` directory. One
of them `aws-config.yaml` works and is tested regular. The other,
`azure-config.yaml` is more of a work in progress and should not
be assumed to be ready to go.

A third file, `iam-policy-summary.txt` is something of an outline of how
we had to specify the AWS security policies (IAM) in order to ensure that
our EC2 virtual machines can speak to our S3 buckets. These almost certainly
are far less than optimal. When in doubt, when making a new S3 bucket,
you'll be dorking with the IAM policies until you can get everything
mounted correctly with `s3fs`.

Within `aws-config.yaml`:
- The current specified `max_workers` and `initial_workers` are the biggest
  we could run without triggering a weird crashy behavior in Ray. They're
  working on it. Other Ray loads use far more nodes than arlo-e2e, so it's
  something about the way we stress the cluster that's different.
  
- The "worker" nodes we're currently using are `c5a.16xlarge` (beefy 64 vCPU AMD machines),
  with `c5.16xlarge` (similarly beefy Intel machines as an alternate), with a
  `m5a.xlarge` (four vCPUs but lots more memory) that we use for our "head" node.
  
- The Ray autoscaler does all the work of creating and destroying our nodes on EC2.
  It starts from an Ubuntu 20.04 VM, pre-installed with dependencies that we need
  (you'll see the various `ami-` strings in there). Once it launches each VM, it then
  executes the various `setup_commands`. Mostly these just install more things, but
  they also set up `s3fs`, which mounts the S3 storage bucket we want onto `/mnt/arlo-data`.
  
- If you want to try to make your own Ubuntu virtual machine, you may find the script
  in `ubuntu-setup.sh` to be helpful. This probably could/should be done just as well
  with Docker, but we get where we need to be without it, for now.

- Right now, everything is kinda arbitrarily set up to work in Amazon's `us-east-2`
  datacenter. There's no reason you couldn't run this elsewhere. Just make sure the
  S3 storage and EC2 compute nodes are in the same data center to avoid crazy data
  charges.