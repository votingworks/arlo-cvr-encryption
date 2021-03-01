# inspiration: https://github.com/byjokese/Generate-Index-Files/blob/master/generate-index.py

import os
from stat import S_ISDIR

from arlo_e2e.ray_io import ray_write_file_with_retries

index_start_text = """<!DOCTYPE html>
<html>
<head><title>{title_text}: {path}</title></head>
<body>
    <h2>{title_text}: {path}</h2>
    This directory has data associated with <a href="https://voting.works">VotingWorks</a>'s arlo-e2e auditing system.
    These files are meant to be read by software, like <a href="https://github.com/votingworks/arlo-e2e">arlo-e2e</a>,
    which is auditing the correct outcome of an election. These files describe an <i>encrypted</i> version of every
    ballot. For more information about how arlo-e2e works, see the 
    <a href="https://github.com/votingworks/arlo-e2e">arlo-e2e page</a>.
    <hr>
    <ul>
"""

index_end_text = """
    </ul>
</body>
</html>
"""


def generate_index_html_files(
    title_text: str, directory_name: str, num_retries: int = 1
) -> None:
    """
    Creates index.html files at every level of the directory. Note that this doesn't cause
    anything to be added to the manifest. That's not necessary, and could be messy.
    """
    files = os.listdir(directory_name)
    index_text = index_start_text.format(
        title_text=title_text, path=directory_name if directory_name != "." else "/"
    )

    for file in sorted(files):
        full_path = os.path.join(directory_name, file)

        if file == "index.html":
            os.unlink(full_path)  # remove the file, which we'll then regenerate later
            continue

        stats = os.stat(full_path)
        is_dir = S_ISDIR(stats.st_mode)
        num_bytes = stats.st_size if not is_dir else 0
        additional_text = (
            f"<i>{num_bytes} bytes</i>" if not is_dir else "<b>directory</b>"
        )

        index_text += (
            f"        <li><a href='{file}'>{file}</a> - {additional_text}</li>\n"
        )

        if is_dir:
            generate_index_html_files(title_text, full_path, num_retries=num_retries)

    index_text += index_end_text

    ray_write_file_with_retries(
        "index.html",
        index_text,
        root_dir=directory_name,
        subdirectories=[],
        num_attempts=num_retries,
    )
