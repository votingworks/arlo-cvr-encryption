# inspiration: https://github.com/byjokese/Generate-Index-Files/blob/master/generate-index.py

from typing import List

from arlo_e2e.io import FileRef

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

redirect_template = """<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="refresh" content="0; url={redirect_path}">
    </head>
    <body>
    </body>
</html>
"""


def generate_index_html_files(
    title_text: str,
    dir_ref: FileRef,
    num_attempts: int = 1,
) -> None:
    """
    Creates index.html files at every level of the directory. Note that this doesn't cause
    anything to be added to the manifest. That's not necessary, and could be messy.
    """
    scan = dir_ref.scandir()

    index_text = index_start_text.format(title_text=title_text, path=str(dir_ref))

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

        if is_dir:
            file_or_dir_name = f"{fn.subdirectories[-1]}/"
        else:
            file_or_dir_name = f"{fn.file_name}"

        index_text += f"        <li><a href='{file_or_dir_name}'>{file_or_dir_name}</a> - {additional_text}</li>\n"

        if is_dir:
            generate_index_html_files(title_text, fn, num_attempts=num_attempts)

    index_text += index_end_text

    (dir_ref + "index.html").write(index_text, num_attempts=num_attempts)

    if not dir_ref.is_local():
        # S3 static web hosting doesn't behave like a normal web server, which will redirect
        # URLs that end in a slash to the corresponding index.html file. To work around this,
        # we're going to use a hack: https://stackoverflow.com/a/56597839/4048276

        # Example: if the dir_ref = s3://bucket-name/dir1/dir2/
        # then the code above just wrote out s3://bucket-name/dir1/dir2/index.html
        # and we're now going to write s3://bucket-name/dir1/dir2 and s3://bucket-name/dir1/dir2/
        # as plain files (!) instructing the browser to redirect to the index.html file.

        redirect_txt = redirect_template.format(
            redirect_path=f"/{'/'.join(dir_ref.subdirectories)}/index.html"
        )
        redirect_file_ref1 = dir_ref.update(
            new_file_name=dir_ref.subdirectories[-1],
            new_subdirs=dir_ref.subdirectories[:-1],
        )
        redirect_file_ref2 = redirect_file_ref1.update(
            new_file_name=redirect_file_ref1.file_name + "/",
        )
        redirect_file_ref1.write(
            redirect_txt, num_attempts=num_attempts, force_content_type="text/html"
        )
        redirect_file_ref2.write(
            redirect_txt, num_attempts=num_attempts, force_content_type="text/html"
        )
