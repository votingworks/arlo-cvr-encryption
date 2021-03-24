from typing import NamedTuple, List

from electionguardtest.election import _DrawType
from hypothesis.strategies import composite, characters, text, lists

from arlo_e2e.io import make_file_name


class FileNameAndContents(NamedTuple):
    file_name: str
    file_path: List[str]
    file_contents: str

    def write(self, root_dir: str) -> None:
        """Write these file contents under the given root directory."""
        fn = make_file_name(
            file_name=self.file_name, root_dir=root_dir, subdirectories=self.file_path
        )
        fn.write(self.file_contents)

    def overwrite(self, root_dir: str, contents: str) -> None:
        """Overwrites the file, under the given root directory."""
        fn = make_file_name(
            file_name=self.file_name, root_dir=root_dir, subdirectories=self.file_path
        )
        fn.unlink()
        fn.write(contents)


@composite
def file_name_and_contents(
    draw: _DrawType, file_name_prefix: str = "", file_contents_prefix: str = ""
):
    """
    Generates `FileNameAndContents` instances, suitable for stuffing into a Manifest. The optional
    `file_name_prefix` and `file_contents_prefix` arguments are handy for ensuring the outputs
    are different from other generated values.
    """
    atoz = characters(min_codepoint=ord("a"), max_codepoint=ord("z"))
    file_contents = file_contents_prefix + draw(
        text(min_size=1, max_size=100, alphabet=atoz)
    )

    # make our lives easier: file path elements are 2 or 3 chars, while file names are 4 or 5 chars

    file_path = draw(
        lists(
            min_size=0,
            max_size=3,
            elements=text(min_size=2, max_size=3, alphabet=atoz),
        )
    )

    file_name = file_name_prefix + draw(
        text(min_size=4, max_size=5, alphabet=atoz),
    )
    return FileNameAndContents(file_name, file_path, file_contents)


@composite
def list_file_names_contents(draw: _DrawType, length: int, file_name_prefix: str = ""):
    """
    Generates a `List[FileNameAndContents]`, of the desired length, suitable for stuffing into a
    Manifest. The optional `file_name_prefix` is handy for ensuring that the outputs have unique
    filenames. This strategy also guarantees that the file names and contents for each file are unique.
    """
    return [
        draw(
            file_name_and_contents(
                file_name_prefix=file_name_prefix + f"{n:010d}",
                file_contents_prefix=f"{n:010d}",
            )
        )
        for n in range(0, length)
    ]
