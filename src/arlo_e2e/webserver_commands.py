import os
from typing import List, Tuple, Dict

import flask
from dataclasses import dataclass
from pathlib import PurePath

from electionguard.serializable import set_serializers, set_deserializers
from flask import get_flashed_messages, flash, Flask
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from arlo_e2e.admin import make_fresh_election_admin, ElectionAdmin
from arlo_e2e.utils import write_json_helper, load_json_helper, write_file_with_retries


# Design note: we're putting as much of the web functionality here, without the actual Flask web
# server present, to make these methods easier to test. We're prefixing all of these methods
# with "w_" so they're a bit easier to integrate with the web server itself.

# How many times we'll attempt to write a file before we give up. Works around transient AWS S3 failures.
FILE_WRITE_RETRIES = 10

# File extensions that we'll allow for file uploads
ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'


def _initializate_everything() -> None:
    set_serializers()
    set_deserializers()


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
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@dataclass(frozen=True)
class SimpleResponse:
    success: bool

    def to_json(self) -> str:
        """
        Converts the simple response to JSON text, suitable for sending to the client.
        Also consumes any pending "flash" messages. The result is a JSON object with
        two keys: "success" and "messages". The former maps to a boolean. The latter
        maps to a dictionary where the keys are message categories ("info" and "error")
        and the values are lists of strings generated since the last time a web response
        was sent.
        """
        flashed_messages: List[Tuple[str, str]] = get_flashed_messages(with_categories=True)
        result_messages: Dict[str, List[str]] = {}
        for c, m in flashed_messages:
            if not result_messages[c]:
                result_messages[c] = [m]
            else:
                result_messages[c].append(m)

        return_me = {
            'success': self.success,
            'messages': result_messages
        }
        return flask.json.jsonify(return_me)


def w_initialize_keys(keyfile_name="secret_election_keys.json") -> SimpleResponse:
    """
    Creates a fresh ElectionAdmin structure with random keys and writes it to disk,
    in the current working directory, with the given filename.
    """

    _initializate_everything()
    filename = PurePath(keyfile_name)

    # This ultimately bottoms out at secrets.randbelow(), which claims to be cryptographically strong.
    admin_state = make_fresh_election_admin()
    write_json_helper(".", filename, admin_state, num_retries=FILE_WRITE_RETRIES)

    # Read it back in, just to make sure we're all good.
    admin_state2 = load_json_helper(".", filename, ElectionAdmin)

    if admin_state2 != admin_state:
        flash_error(f"Something went wrong writing to {filename}")
        return SimpleResponse(False)

    if not admin_state2.is_valid():
        flash_error(f"Admin state wasn't valid (shouldn't ever happen!)")
        return SimpleResponse(False)

    flash_info(f"Admin state written to {filename}")
    return SimpleResponse(True)


def w_upload_cvrs(file: FileStorage, filename: str) -> SimpleResponse:
    """
    Given CVR data, writes it to a local file of the given name, suitable for
    subsequent tallying.
    """

    _initializate_everything()
    if _allowed_file(file.filename):
        filename_secure = secure_filename(filename)

        # We're not going to worry about retries, since this is to the "local" filesystem
        # and not S3.
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename_secure))
        flash_info(f"{filename} written successfully")
        return SimpleResponse(True)
    else:
        flash_error(f"file {file.filename} -> {filename} not allowed")
        return SimpleResponse(False)
