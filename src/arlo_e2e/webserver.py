from flask import Flask, request
from werkzeug.datastructures import FileStorage

from arlo_e2e.webserver_commands import w_initialize_keys, flash_error, SimpleResponse, w_upload_cvrs

app = Flask(__name__)


@app.route("/initialize-keys")
def initialize_keys() -> str:
    return w_initialize_keys().to_json()


@app.route('/upload-cvrs/<filename>', methods=['POST'])
def upload_cvrs(filename: str) -> str:
    if 'file' not in request.files:
        flash_error('No file part')
        return SimpleResponse(False).to_json()
    file: FileStorage = request.files['file']
    if not file or file.filename == '':
        flash_error('No selected file')
        return SimpleResponse(False).to_json()
    return w_upload_cvrs(file, filename).to_json()
