import os
import unittest

from electionguard.serializable import set_serializers, set_deserializers

from arlo_e2e.admin import make_fresh_election_admin, ElectionAdmin
from arlo_e2e.ray_io import ray_load_json_file, ray_write_json_file


class TestAdmin(unittest.TestCase):
    admin_file = "test_admin_writing.json"

    def setUp(self) -> None:
        set_serializers()
        set_deserializers()

    def tearDown(self) -> None:
        os.remove(self.admin_file)

    def test_write_fresh_state(self) -> None:
        admin_state = make_fresh_election_admin()
        ray_write_json_file(
            file_name=self.admin_file, content_obj=admin_state, root_dir="."
        )
        admin_state2 = ray_load_json_file(".", self.admin_file, ElectionAdmin)

        self.assertEqual(admin_state2, admin_state)
        self.assertTrue(admin_state2.is_valid())
