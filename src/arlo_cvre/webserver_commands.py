import os
from dataclasses import dataclass
from pathlib import Path
from shutil import copyfileobj
from typing import List, Tuple

import flask
from electionguard.serializable import set_serializers, set_deserializers
from flask import get_flashed_messages, flash, Flask
from typing.io import BinaryIO
from werkzeug.utils import secure_filename

from arlo_cvre.admin import make_fresh_election_admin, ElectionAdmin

# File extensions that we'll allow for file uploads
from arlo_cvre.constants import NUM_WRITE_RETRIES
from arlo_cvre.io import make_file_ref

# Design note: we're putting as much of the web functionality here, without the actual Flask web
# server present, to make these methods easier to test. We're prefixing all of these methods
# with "w_" so they're a bit easier to integrate with the web server itself.

ALLOWED_EXTENSIONS = {"csv"}

# File holding the election's public/private keypair
DEFAULT_ADMIN_STATE_FILENAME = "secret_election_keys.json"

# Location where administration state is written on the local filesystem
DEFAULT_ADMIN_DIRECTORY = "."

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = f"{DEFAULT_ADMIN_DIRECTORY}/uploads"


def _initializate_everything() -> None:
    set_serializers()
    set_deserializers()
    Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)


def flash_info(text: str) -> None:
    """
    Front-end for Flask's "flash" infrastructure, which gathers "flash" messages to be delivered
    to the web front-end for later use.
    """
    flash(text, category="info")


def flash_error(text: str) -> None:
    """
    Front-end for Flask's "flash" infrastructure, which gathers "flash" messages to be delivered
    to the web front-end for later use.
    """
    flash(text, category="error")


def _allowed_file(filename: str) -> bool:
    # code borrowed from the Flask documentation
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@dataclass(frozen=True)
class SimpleResponse:
    """
    A wrapper around the return type from all our webserver functions. The `to_json` method
    notably will consume any "flashed" messages waiting in the Flask web server.
    """

    success: bool
    body: str = ""

    def to_json(self) -> str:
        """
        Converts the simple response to JSON text, suitable for sending to the client.
        Also consumes any pending "flash" messages. The result is a JSON object with
        two keys: "success" and "messages". The former maps to a boolean. The latter
        maps to a list of two-element lists, where the first one is the category
        ("info" or "error") and the latter is a general-purpose string that might
        be displayed to the user.
        """
        flashed_messages: List[Tuple[str, str]] = get_flashed_messages(
            with_categories=True
        )

        return_me = {
            "success": self.success,
            "body": self.body,
            "messages": flashed_messages,
        }
        return str(flask.json.jsonify(return_me))


def w_initialize_keys(
    keyfile_name: str = "secret_election_keys.json",
) -> SimpleResponse:
    """
    Creates a fresh ElectionAdmin structure with random keys and writes it to disk,
    in the current working directory, with the given filename.
    """

    _initializate_everything()

    # This ultimately bottoms out at secrets.randbelow(), which claims to be cryptographically strong.
    admin_state = make_fresh_election_admin()
    admin_fr = make_file_ref(
        file_name=keyfile_name,
        root_dir=DEFAULT_ADMIN_DIRECTORY,
    )

    admin_fr.write_json(content_obj=admin_state, num_attempts=NUM_WRITE_RETRIES)

    # Read it back in, just to make sure we're all good.
    admin_state2 = admin_fr.read_json(ElectionAdmin)

    if admin_state2 != admin_state:
        flash_error(f"Something went wrong writing to {keyfile_name}")
        return SimpleResponse(False)

    if not admin_state2.is_valid():
        flash_error(f"Admin state wasn't valid (shouldn't ever happen!)")
        return SimpleResponse(False)

    flash_info(f"Admin state written to {keyfile_name}")
    return SimpleResponse(True)


def w_get_admin_state(
    keyfile_name: str = DEFAULT_ADMIN_STATE_FILENAME,
) -> SimpleResponse:
    pass


def w_upload_cvrs(stream: BinaryIO, filename: str) -> SimpleResponse:
    """
    Given CVR data, writes it to a local file of the given name, suitable for
    subsequent tallying.
    """

    _initializate_everything()

    if not _allowed_file(filename):
        flash_error(f"file {filename} not allowed")
        return SimpleResponse(False)

    filename_secure = secure_filename(filename)
    dst_path = os.path.join(app.config["UPLOAD_FOLDER"], filename_secure)

    # We're not going to worry about retries, since this is to the "local" filesystem
    # and not S3.
    dst = open(dst_path, "wb")
    try:
        copyfileobj(stream, dst)
    except Exception as e:
        flash_error(f"failed to store {dst_path}: {str(e)}")
    finally:
        dst.close()

    flash_info(f"{filename_secure} written successfully")
    return SimpleResponse(True)
