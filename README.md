# Arlo CVR Encryption
![check](https://github.com/votingworks/arlo-cvr-encryption/actions/workflows/check.yml/badge.svg)


This repository contains a set of standalone tools that can be
used alongside an [Arlo RLA audit](https://voting.works/risk-limiting-audits/)
with the goal of increasing the *transparency* of a risk-limiting audit.

## Table of contents

- [Why CVR Encryption?](#why-cvr-encryption)
- [Command-line tools](#command-line-tools)
  - [arlo_initialize_keys](#arlo_initialize_keys)
  - [arlo_tally_ballots](#arlo_tally_ballots)
  - [arlo_decrypt_ballots](#arlo_decrypt_ballots)
  - [arlo_decrypt_ballot_batch](#arlo_decrypt_ballot_batch)
  - [arlo_verify_tally](#arlo_verify_tally)
  - [arlo_verify_rla](#arlo_verify_rla)
  - [arlo_all_ballots_for_contest](#arlo_all_ballots_for_contest)
  - [arlo_ballot_style_summary](#arlo_ballot_style_summary)
  - [arlo_decode_ballots](#arlo_decode_ballots)
  - [arlo_write_root_hash](#arlo_write_root_hash)
  - [Benchmarks](#benchmarks)
- [Implementation status](#implementation-status)
- [Installation](#installation)
- [Amazon AWS details](#amazon-aws-s3-ec2-iam-details)

## Why CVR encryption?

In a risk limiting audit, the risk limit itself is a function of the margin of victory.
In the hypothetical where an attacker can compromise the tallying process, the attacker
can announce a total with a huge margin, and the RLA will then require very few samples.

If the real margin was small, the number of samples should have been much larger.

The fix to this is to require the election official to:
- *Commit* to the set of all ballots such that they cannot tamper with the set of
  electronic ballot records afterward (i.e., any request from the RLA, i.e., "give
  me ballot N" can be proven consistent with the commitment).
- *Prove* that the election tally and margins of victory are consistent with this
  commitment (this is where all the cryptographic machinery comes into play).

With these, the RLA now has proof that it's working from the right starting point.

This idea, in various forms, has been around for several years. A recent paper
from Benaloh, Stark, and Teague ([slides](https://www.e-vote-id.org/wp-content/uploads/2019/10/VAULT.pdf))
has exactly the idea that we want to build.

A nice property of this design is that it's completely "on the side" of the regular
RLA process. You can do a RLA without the encryption part, and you still get something useful.
The encryption just makes things more robust. You can layer it on to an existing RLA process
without requiring changes to how votes are cast and tabulated.

And, of course, if you *do* happen to have "end to end" (e2e) cryptographic voting machines,
that generate these same kinds of ciphertexts,
now those fit in very nicely. This scheme provides something of a stepping stone
towards bridging the world of e2e voting machines and risk-limiting audits.

## Command-line tools

All of these tools, in the `apps` directory, can generally be executed by
first entering the appropriate Python virtual environment (e.g., running `pipenv shell`),
and then running `python apps/command.py` with the appropriate arguments.

### arlo_initialize_keys

```
usage: arlo_initialize_keys.py [-h] [-k KEYS]

Initialize the election public/private key material.

optional arguments:
  -h, --help            show this help message and exit
  -k KEYS, --keys KEYS  file name for where the information is written (default:
                        secret_election_keys.json)
```
This command randomly generates a public/private keypair that the election official
will use for subsequent arlo-cvre computation. The resulting file, by default
`secret_election_keys.json` should be treated as sensitive data. The public
key will be separately included in the public tally results.


### arlo_tally_ballots

```
usage: arlo_tally_ballots.py [-h] [-k KEYS] [-t TALLIES] [--cluster] cvr_file

Load a Dominion-style ballot CVR file and write out an Arlo-cvr-encryption tally

positional arguments:
  cvr_file              filename for the Dominion-style ballot CVR file

optional arguments:
  -h, --help            show this help message and exit
  -k KEYS, --keys KEYS  file name for the election official's key materials (default:
                        secret_election_keys.json)
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally is written (default: tally_output)
  --cluster             uses a Ray cluster for distributed computation
```
This command reads a Dominion-style CVR file and ultimately writes out a directory full
of encrypted ballots and their associated proofs, as well as the tallies and their
decryptions.

- A single encrypted ballot can easily become a megabyte of JSON. We write each ballot
  out to its own file. We map the ballot's unique id to a file within a subdirectory
  of prefixes (e.g., `b0001283` becomes `ballots/b0001/b0001283.json`). This ensures
  that we have no more than 1000 ballots per subdirectory. This sort of structure
  generally improves performance, because subdirectories with large numbers of entries
  can be slow to traverse.

- Each subdirectory also includes a file called `MANIFEST.json`, which includes the SHA256 hashes
  of its contents, as well as the hash of every subdirectory's manifest. This creates a Merkle-tree
  structure. Subsequent tools take the hash of the root manifest as an input and can then use
  this to validate any file read from this directory.
  
- Each subdirectory also includes an `index.html` file, making this directory structure
  suitable for use on a static web service or CDN.
  
- The root directory also includes `root_hash.html`, which includes the SHA256 hash of
  the root manifest file and other useful metadata, both in human-readable form and as
  a QRcode. An election official might load this web page, print it, and make copies
  for any election observers.
  
- The `--cluster` argument says to use Ray on a compute cluster (more on Ray below). If you're
  running on a single machine, Ray will still be used, but in "local" mode, allowing all of the
  available CPU cores on that single machine to be utilized for increased performance.
  
- The tally directory can be specified as a local filename or as an S3-style URL 
  (e.g., `s3://bucket-name/directory-name`). When writing to S3, all the appropriate
  metadata are set for each object to work properly with 
  [S3's static web service](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteHosting.html).
  
Note that the ability to write output to S3, versus a local filesystem, can be exercised whether
running the tally locally, or running the tally on an AWS EC2 cluster. For relatively small
elections, you could run all the computation locally, but still publish the results to S3.
  
### arlo_decrypt_ballots
```
usage: arlo_decrypt_ballots.py [-h] [--cluster] [-t TALLIES] [-k KEYS] [-d DECRYPTED]
                               [-r ROOT_HASH]
                               ballot_id [ballot_id ...]

Decrypts a list of ballots

positional arguments:
  ballot_id             ballot identifiers for ballots to be decrypted

optional arguments:
  -h, --help            show this help message and exit
  --cluster             uses a Ray cluster for distributed computation
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally artifacts can be found (default:
                        tally_output)
  -k KEYS, --keys KEYS  file name for where the information is written (default:
                        secret_election_keys.json)
  -d DECRYPTED, --decrypted DECRYPTED
                        directory name for where decrypted ballots will be written (default:
                        decrypted_ballots)
  -r ROOT_HASH, --root-hash ROOT_HASH, --root_hash ROOT_HASH
                        optional root hash for the tally directory; if the manifest is
                        tampered, an error is indicated
```
This commands allows a desired set of ballots to be decrypted, and then written into
a separate subdirectory. This is something an election official would do after ballots
have been selected for an audit. The `ballot_id` names are the internal names used by
arlo-cvr-encryption (e.g., `b0001283` for the ballot in `ballots/b0001/b0001283.json`).

### arlo_decrypt_ballot_batch
```
usage: arlo_decrypt_ballot_batch.py [-h] [--cluster] [-t TALLIES] [-r ROOT_HASH] [-k KEYS]
                                    [-d DECRYPTED]
                                    batch_file

Decrypts a batch of ballots based on a ballot retrieval CSV file

positional arguments:
  batch_file            filename for the ballot retrieval CSV file (no default)

optional arguments:
  -h, --help            show this help message and exit
  --cluster             uses a Ray cluster for distributed computation
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally artifacts can be found (default:
                        tally_output)
  -r ROOT_HASH, --root-hash ROOT_HASH, --root_hash ROOT_HASH
                        optional root hash for the tally directory; if the manifest is
                        tampered, an error is indicated
  -k KEYS, --keys KEYS  file name for where the information is written (default:
                        secret_election_keys.json)
  -d DECRYPTED, --decrypted DECRYPTED
                        directory name for where decrypted ballots will be written (default:
                        decrypted_ballots)
```
This command is similar to arlo_decrypt_ballots, except that its input is a "batch file"
written out by the Arlo auditing system, which includes Dominion-style
ballot "imprinted id" strings (e.g., `2-1-48`), which are then translated into 
arlo-cvr-encryption identifiers.
  
### arlo_verify_tally
```
usage: arlo_verify_tally.py [-h] [-t TALLIES] [-r ROOT_HASH] [--totals] [--cluster]

Reads an arlo-cvr-encryption tally and verifies all the cryptographic artifacts

optional arguments:
  -h, --help            show this help message and exit
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally artifacts can be found (default:
                        tally_output)
  -r ROOT_HASH, --root-hash ROOT_HASH, --root_hash ROOT_HASH
                        optional root hash for the tally directory; if the manifest is
                        tampered, an error is indicated
  --totals              prints the verified totals for every race
  --cluster             uses a Ray cluster for distributed computation
```
This command reads the output of `arlo_tally_ballots`. The optional root hash can be specified
and then every file will be verified as untampered before its processed. As with `arlo_tally_ballots`,
for larger elections you'll want to run this on a large cluster for improved performance,
but it will use all of the available parallelism on a local computer as well. 

### arlo_verify_rla
```
usage: arlo_verify_rla.py [-h] [-t TALLIES] [-d DECRYPTED] [-v] [-r ROOT_HASH] audit_report

Reads an arlo-cvr-encryption tally, decrypted ballots, and Arlo audit report; verifies everything matches

positional arguments:
  audit_report          Arlo audit report (in CSV format) to compare to the tally

optional arguments:
  -h, --help            show this help message and exit
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally artifacts can be found (default:
                        tally_output)
  -d DECRYPTED, --decrypted DECRYPTED
                        directory name for where decrypted ballots can be found (default:
                        decrypted_ballots)
  -v, --validate        validates the decrypted ballots are consistent with the original
                        encrypted versions (default: False)
  -r ROOT_HASH, --root-hash ROOT_HASH, --root_hash ROOT_HASH
                        optional root hash for the tally directory; if the manifest is
                        tampered, an error is indicated
```
This command verifies the result of `arlo_decrypt_ballot_batch` against an Arlo audit report,
which includes the audit board's determinations for the interpretation of each ballot.
If you want to also validate that the decrypted ballots and the original encrypted ballots
match up, use the `--validate` option. This only validates the specific ballots considered
by the audit, not the full election of encrypted ballots. For that, you'd use `arlo_verify_tally`.

### arlo_all_ballots_for_contest
```
usage: arlo_all_ballots_for_contest.py [-h] [-t TALLIES] contest [contest ...]

Prints ballot-ids for all ballots having the desired contest(s)

positional arguments:
  contest               text prefix(es) for contest

optional arguments:
  -h, --help            show this help message and exit
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally artifacts can be found (default:
                        tally_output)
```
A useful utility for identifying the subset of ballots containing a particular contest.

### arlo_ballot_style_summary
```
usage: arlo_ballot_style_summary.py [-h] [-t TALLIES]

Reads an arlo-cvr-encryption tally and prints statistics about ballot styles and contests

optional arguments:
  -h, --help            show this help message and exit
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally artifacts can be found (default:
                        tally_output)
```
A useful utility to generate summaries about how many ballots there are of each ballot style.


### arlo_decode_ballots
```
usage: arlo_decode_ballots.py [-h] [-t TALLIES] [-d DECRYPTED] [-r ROOT_HASH]
                              ballot_id [ballot_id ...]

Validates plaintext ballots and decodes to human-readable form

positional arguments:
  ballot_id             ballot identifiers to decode

optional arguments:
  -h, --help            show this help message and exit
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally artifacts can be found (default:
                        tally_output)
  -d DECRYPTED, --decrypted DECRYPTED
                        directory name for where decrypted ballots can be found (default:
                        decrypted_ballots)
  -r ROOT_HASH, --root-hash ROOT_HASH, --root_hash ROOT_HASH
                        optional root hash for the tally directory; if the manifest is
                        tampered, an error is indicated
```

Given some arlo-cvr-encryption ballot identifiers (e.g., `b0001283` for the ballot in `ballots/b0001/b0001283.json`),
prints everything we know about those ballots. If they were previously decrypted, this will print their
decryptions and verify their equivalence proofs. If the proofs don't check out, this tool flags the issue.
For ballots that were never decrypted, we at least print the available metadata for the ballot.

### arlo_write_root_hash
```
usage: arlo_write_root_hash.py [-h] [--index_html] [--election_name ELECTION_NAME]
                               [-t TALLIES]
                               [KEY=VALUE [KEY=VALUE ...]]

Writes out a file (root_hash.html) suitable for printing and handing out as the root hash of
the election.

positional arguments:
  KEY=VALUE             Set a number of metadata key-value pairs (do not put spaces before or
                        after the = sign). If a value contains spaces, you should define it
                        with double quotes: foo="this is a sentence". Note that values are
                        always treated as strings.

optional arguments:
  -h, --help            show this help message and exit
  --index_html, --index-html, -i
                        generates (or regenerates) the index.html files as well as the root
                        hash QRcode
  --election_name ELECTION_NAME, --election-name ELECTION_NAME, -n ELECTION_NAME
                        Human-readable name of the election, e.g., `Harris County General
                        Election, November 2020`
  -t TALLIES, --tallies TALLIES
                        directory name for where the tally and MANIFEST.json are written
                        (default: tally_output)
```

The behavior of this command is built into `arlo_tally_ballots`. You'd only use this
if you wanted to customize the set of key/value pairs represented in `root_hash.html`
and the included QRcode.

### Benchmarks

For benchmarking and testing purposes, there are several additional programs 
(`io_test`, `manifest_bench`, and `ray_remote_bench`) in the `apps` directory.


## Implementation status

The current `arlo-cvr-encryption` codebase has detailed unit tests built with [Hypothesis](https://hypothesis.readthedocs.io/en/latest/).
It can generate a random set of CVRs for a random election (varying the number
of contests, candidates per contest, and so forth), encrypt them and generate
all the proofs, serialize them to disk, read them all back in again,
and verify all the proofs.

On top of this, we have all the command-line tools, listed above, and we have code
that can use all of the cores of a multicore computer to accelerate the process
on a single computer. We also support large clusters of computers, via
[Ray](https://ray.io/). Our internal benchmark of encrypting a large election, 
using 1200 Amazon "c5a" virtual CPUs, can encrypt roughly 120 ballots per second,
or roughly a million ballots in two hours. These are typical municipal ballots
with many contests. Simpler ballots would be faster to encrypt.

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
  
## Installation

To install `arlo-cvr-encryption` on your computer to play with it, you'll need
to do a few other things first. **If you're on a Mac**:
- Install the XCode tools (`xcode-select --install`)
- Install [Homebrew](https://brew.sh/)
  - This typically also installs Python 3.8, but doesn't put it in your path. You can run `brew install python@3.8` 
    if you're not sure.
  - Make sure you have `/usr/local/opt/python@3.8/bin` early in your directory path.
- Make sure you've got an updated `pip` (`python -m pip install --upgrade pip`)
- Then, you can just run `make`, which will create a Python virtual environment, install
  the external dependencies, and eventually even run the unit tests. If they all pass,
  you're good to go.
  
**If you're on a Linux machine**:
- Make sure you've got Python 3.8 properly installed ([Ubuntu instructions](https://linuxize.com/post/how-to-install-python-3-8-on-ubuntu-18-04/))
- Make sure you've got an updated `pip` (`python -m pip install --upgrade pip`)
- Then, you can just run `make`, which will create a Python virtual environment, install
  the external dependencies, and eventually even run the unit tests. If they all pass,
  you're good to go.
  
## Amazon AWS (S3, EC2, IAM) details

To generate or verify tallies of large elections, one desktop computer does not
have sufficient power to do the computation in a reasonable amount of time.
The solution is to run on a large cluster. We've primarily developed and tested
against Amazon EC2 clusters.

To set this up for yourself, you'll need an AWS account. By default, AWS accounts
have a limited number of concurrent virtual machines. You have to email them to
explicitly ask for as many as we're using. 

Configuring Ray: There are two YAML configurations for Ray in the `cloud` 
directory. One of them `aws-config.yaml` works and is execised regularly. The other,
`azure-config.yaml` is more of a work in progress and should not
be assumed to be ready to go. If you want to run on a cluster other than AWS,
you'll have some extra work to do.

A third file, `iam-policy-summary.txt` is something of an outline of how
we had to specify the AWS security policies (IAM) in order to ensure that
our EC2 virtual machines can speak to our S3 buckets. These almost certainly
are far less than optimal. When in doubt, when making a new S3 bucket,
you'll be dorking with the IAM policies. If you're just reading the results
of somebody else's election, then their S3 bucket should be world-readable.
You just specify the S3-style URL.

Within `aws-config.yaml`:
- The current specified `max_workers` and `min_workers` are designed to
  allocate the maximum number of nodes, right away. Note that these are
  counts of the number of "virtual computers", each of which may have
  multiple "virtual CPUs." Ray seems to work well with up to 64 vCPUs per virtual machine. 
  After that, the Linux kernel seems to run out of resources.
  
- The "worker" nodes we're currently using are `c5a.16xlarge` (beefy 64 vCPU AMD machines),
  with `c5.12xlarge` (similarly beefy 48vCPU Intel machines) as an alternate, and with a
  `m5a.xlarge` (four vCPUs but lots more memory) that we use for our "head" node.
  (Ray uses a singular "head" node to control all of its "workers". By giving it
  a ton of memory and forbidding worker tasks from running on the head node, we
  seem to be able to run at our desired scale.)
  
- The Ray autoscaler does all the work of creating and destroying our nodes on EC2.
  It starts from an Ubuntu 20.04 VM, pre-installed with dependencies that we need
  (you'll see the various `ami-` strings in there). Once it launches each VM, it then
  executes the various `setup_commands`. Mostly these just install the various
  Python packages we need.
  
- If you want to try to make your own Ubuntu virtual machine, you may find the script
  in `ubuntu-setup.sh` to be helpful. This probably could/should be done just as well
  with Docker, but we get where we need to be without it, for now.

- Right now, everything is arbitrarily set up to work in Amazon's `us-east-2`
  datacenter. There's no reason you couldn't run this elsewhere.
  
Running Ray on a big cluster:
- On your local machine, in your `pipenv shell` virtual environment, it's very simple.
  Assuming you've got your AWS credentials set up in your home directory, all you have
  to do is run `ray up cloud/aws-config.yaml`. This starts the Ray cluster. Similarly,
  `ray down cloud/aws-config.yaml` destroys the cluster. Don't forget to do this, since
  you're paying for keeping these nodes alive.
  
- You use `ray rsync-up cloud/aws-config.yaml localfile remotefile` to copy files
  to the Ray head node. You'll typically copy the Dominion-style CSV file as well
  as the secret key file.
  
- You use `ray submit cloud/aws-config.yaml apps/arlo_tally_ballots.py args` to
  run the tally process on the cluster. Don't forget the `--cluster` argument.
  Otherwise, the command-line arguments are exactly the same as when running
  the command locally.
  
- You can run `ray dashboard cloud/aws-config`, which starts a localhost webserver
  that lets you see the CPU and memory usage within your Ray cluster.
  
- It's useful to also bring up the AWS EC2 dashboard in a separate tab, so you can
  double check that you're not accidentally keeping all these nodes running after
  your computation is complete.
  
If you want to learn more about the engineering effort it took to get arlo-cvr-encryption running
at scale, you may enjoy this [YouTube talk](https://www.youtube.com/watch?v=m7r33EuN6Zw)
with many of the details.