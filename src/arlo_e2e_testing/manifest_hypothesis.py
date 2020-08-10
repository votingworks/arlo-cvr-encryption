from typing import NamedTuple, List
from electionguardtest.election import _DrawType
from hypothesis.strategies import composite, characters, text, lists


class FileNameAndContents(NamedTuple):
    file_name: str
    file_path: List[str]
    file_contents: str


@composite
def file_name_and_contents(draw: _DrawType):
    atoz = characters(min_codepoint=ord("a"), max_codepoint=ord("z"))
    file_contents = draw(text(min_size=1, max_size=100, alphabet=atoz))

    # make our lives easier: file path elements are 2 or 3 chars, while file names are 4 or 5 chars

    file_path = draw(
        lists(
            min_size=0,
            max_size=3,
            elements=text(min_size=2, max_size=3, alphabet=atoz),
        )
    )

    file_name = draw(text(min_size=4, max_size=5, alphabet=atoz),)
    return FileNameAndContents(file_name, file_path, file_contents)
