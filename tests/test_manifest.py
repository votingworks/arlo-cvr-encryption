import shutil
import unittest

from electionguard.logs import log_warning

MANIFEST_TESTING_DIR = "manifest_testing"


class TestManifestPublishing(unittest.TestCase):
    def removeTree(self) -> None:
        try:
            shutil.rmtree(MANIFEST_TESTING_DIR, ignore_errors=True)
        except FileNotFoundError:
            # okay if it's not there
            pass

    def tearDown(self) -> None:
        self.removeTree()

    def setUp(self) -> None:
        log_warning("EXPECT MANY ERRORS TO BE LOGGED. THIS IS NORMAL.")
        self.removeTree()
