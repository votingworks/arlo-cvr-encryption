from typing import Final

# When we're writing ballots out to disk, we'll carve out this many characters and
# use that as a directory name. This avoids directories with insane numbers of files
# that take forever to list or interact with.
BALLOT_FILENAME_PREFIX_DIGITS = 5

# When we're writing files to s3fs, we'll rarely see failures, but with enough files, it's a certainty.
# This is how many times we'll retry each write until it works.
NUM_WRITE_RETRIES: Final = 10

# These constants define how we shard up the ballot processing for the map-reduce pipeline
MAX_CONCURRENT_TASKS: Final = 3000
BALLOTS_PER_SHARD: Final = 20
PARTIAL_TALLIES_PER_SHARD: Final = 50

# These define the various filenames that we're going to use when writing out an election
ELECTION_METADATA: Final[str] = "election_metadata.json"
CVR_METADATA: Final[str] = "cvr_metadata.csv"
ELECTION_DESCRIPTION: Final[str] = "election_description.json"
ENCRYPTED_TALLY: Final[str] = "encrypted_tally.json"
CRYPTO_CONSTANTS: Final[str] = "constants.json"
CRYPTO_CONTEXT: Final[str] = "cryptographic_context.json"
MANIFEST_FILE: Final[str] = "MANIFEST.json"
