import os
import unittest

from electionguard.serializable import set_serializers, set_deserializers

from arlo_cvre.admin import make_fresh_election_admin, ElectionAdmin
from arlo_cvre.io import make_file_ref


class TestAdmin(unittest.TestCase):
    admin_file = "test_admin_writing.json"

    def setUp(self) -> None:
        set_serializers()
        set_deserializers()

    def tearDown(self) -> None:
        os.remove(self.admin_file)

    def test_write_fresh_state(self) -> None:
        admin_state = make_fresh_election_admin()
        fr = make_file_ref(file_name=self.admin_file, root_dir=".")
        fr.write_json(admin_state)
        admin_state2 = fr.read_json(ElectionAdmin)

        self.assertEqual(admin_state2, admin_state)
        self.assertTrue(admin_state2.is_valid())
