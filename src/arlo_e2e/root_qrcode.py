import json
from io import BytesIO
from typing import Dict

import PIL.Image
import qrcode

# centering is awful: https://css-tricks.com/centering-a-div-that-maintains-aspect-ratio-when-theres-body-margin/
from arlo_e2e.constants import NUM_WRITE_RETRIES
from arlo_e2e.eg_helpers import log_and_print
from arlo_e2e.io import FileRef
from arlo_e2e.manifest import load_existing_manifest

root_start_text = """<!DOCTYPE html>
<html>
<style>
.qrcode {{
    width: 50%;
    height: 50%;
    margin: auto;
    display: flex;
    flex: 0 1 auto;
    align-items: center;
    justify-content: center;
}}
.qrcode img {{
    max-width: 100%;
    max-height:100%;
}}
</style>
<head><title>{title_text}: Root Hash</title></head>
<body>
    <h2>{title_text}: Root Hash</h2>
    <p>This information comprises the "root hash" of this election. Additional useful metadata
    about the election may appear here as well, such as URLs or cloud storage bucket names.
    
    <p>This data helps validate the integrity of a risk limiting audit and is means to be
    read by software like <a href="https://voting.works">VotingWorks</a>'s 
    <a href="https://github.com/votingworks/arlo-e2e">arlo-e2e</a> system.</p>
    <hr>
    <ul>
"""

root_end_text = """
    </ul>
    <div class=qrcode>
    <img src="root_hash_qrcode.png" alt="QRcode with the same information as above">
    </div>
    </center>
</body>
</html>
"""


def gen_root_qrcode(
    election_name: str,
    tally_dir_ref: FileRef,
    metadata: Dict[str, str],
    num_retry_attempts: int = NUM_WRITE_RETRIES,
    verbose: bool = False,
) -> None:
    """
    Creates and writes a file, `root_hash.html` and its associated image files,
    in the `tally_dir` folder. Assumes that `MANIFEST.json` has already been
    written to the same directory and will compute its hash.

    The `metadata` field allows you to specify arbitrary keys and values to be written
    out with the QRcode as well as the human-readable portion. Recommended fields:

    * s3_host
    * s3_bucket
    * s3_region
    * s3_directory
    * s3_directory_decrypted
    * web_prefix
    * web_prefix_decrypted

    See also, `load_existing_manifest` has an optional `expected_root_hash` field,
    used by `load_ray_tally` and `load_fast_tally`.

    :param election_name: Human-readable name of the election, e.g., `Harris County General Election, November 2020`.
      If this is missing, the election name is taken from the election itself, assuming there's an `election_name`
      field in the election metadata, which is normally there.
    :param tally_dir_ref: FileRef to the directory where `MANIFEST.json` can be found and where results will be written
    :param metadata: dictionary mapping strings to values, rendered out to the QRcode as-is
    :param num_retry_attempts: number of times to attempt a write if it fails
    :param verbose: if true, logging details go to stdout as well
    """
    log_and_print(
        f"Generating root QR hash for for {str(tally_dir_ref)}", verbose=verbose
    )

    manifest = load_existing_manifest(root_dir_ref=tally_dir_ref)
    if manifest is None:
        log_and_print("MANIFEST.json file not found, cannot generate QRcode")
        return

    data_hash = manifest.manifest_hash.hash
    qr_headers = {
        "election_name": election_name,
        "tally_location": str(tally_dir_ref),
        "root_hash": data_hash,
    }

    log_and_print(f"QR headers: {str(qr_headers)}", verbose=verbose)

    # goofy Python syntax to merge two dictionaries
    qr_data = {**qr_headers, **metadata}

    bullet_text = ""
    for k in sorted(qr_data.keys()):
        if qr_data[k].startswith("http"):
            value_data = f'<a href="{qr_data[k]}f">{qr_data[k]}</a>'
        else:
            value_data = qr_data[k]

        bullet_text += (
            f"        <li><code><b>{k}:</b></code> <code>{value_data}</code></li>\n"
        )

    qr_img: PIL.Image = qrcode.make(json.dumps(qr_data)).get_image()
    qr_byteio = BytesIO()
    qr_img.save(qr_byteio, "PNG")
    qr_bytes: bytes = qr_byteio.getvalue()
    (tally_dir_ref + "root_hash_qrcode.png").write(
        qr_bytes, num_attempts=num_retry_attempts
    )

    html_text = (
        root_start_text.format(title_text=election_name) + bullet_text + root_end_text
    )
    (tally_dir_ref + "root_hash.html").write(
        html_text,
        num_attempts=num_retry_attempts,
    )
