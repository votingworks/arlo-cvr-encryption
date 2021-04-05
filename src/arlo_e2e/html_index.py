# inspiration: https://github.com/byjokese/Generate-Index-Files/blob/master/generate-index.py

from typing import List

from arlo_e2e.io import make_file_ref, FileRef

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
    title_text: str,
    root_dir: str,
    subdirectories: List[str] = None,
    num_attempts: int = 1,
) -> None:
    """
    Creates index.html files at every level of the directory. Note that this doesn't cause
    anything to be added to the manifest. That's not necessary, and could be messy.
    """
    if subdirectories is None:
        subdirectories = []

    dir_fr = make_file_ref(
        file_name="", root_dir=root_dir, subdirectories=subdirectories
    )
    scan = dir_fr.scandir()

    index_text = index_start_text.format(
        title_text=title_text, path="/" + "/".join(subdirectories)
    )

    files_and_dirs: List[FileRef] = sorted(
        list(scan.files.values()) + list(scan.subdirs.values()), key=lambda fn: str(fn)
    )
    for fn in files_and_dirs:
        if fn.file_name == "index.html":
            fn.unlink()  # remove the file, which we'll then regenerate later
            continue

        num_bytes = fn.size()
        is_dir = fn.is_dir()

        additional_text = (
            f"<i>{num_bytes} bytes</i>" if not is_dir else "<b>directory</b>"
        )

        file_name_plus_slash = f"{fn.file_name}{'/' if is_dir else ''}"

        index_text += f"        <li><a href='{file_name_plus_slash}'>{file_name_plus_slash}</a> - {additional_text}</li>\n"

        if is_dir:
            generate_index_html_files(
                title_text, root_dir, fn.subdirectories, num_attempts=num_attempts
            )

    index_text += index_end_text

    dir_fr.update(new_file_name="index.html").write(
        index_text, num_attempts=num_attempts
    )
