import unittest
import os
from pathlib import PurePath

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.admin import make_fresh_election_admin, ElectionAdmin
from arlo_e2e.ray_io import write_json_helper, load_json_helper


class TestAdmin(unittest.TestCase):
    admin_file = PurePath("test_admin_writing.json")

    def setUp(self) -> None:
        set_serializers()
        set_deserializers()

    def tearDown(self) -> None:
        os.remove(self.admin_file)

    def test_write_fresh_state(self) -> None:
        admin_state = make_fresh_election_admin()
        write_json_helper(".", self.admin_file, admin_state)
        admin_state2 = load_json_helper(".", self.admin_file, ElectionAdmin)

        self.assertEqual(admin_state2, admin_state)
        self.assertTrue(admin_state2.is_valid())
