import shutil
import unittest
from dataclasses import dataclass
from os import cpu_count

import ray
from electionguard.serializable import Serializable

from arlo_e2e.io import (
    set_failure_probability_for_testing,
    wait_for_zero_pending_writes,
    reset_status_actor,
    make_file_ref,
    FileRef,
    make_file_ref_from_path,
)
from arlo_e2e.ray_helpers import ray_init_localhost
from arlo_e2e.utils import sha256_hash

poop_emoji = "💩"  # used for testing encoding/decoding


@dataclass(eq=True)
class TestSerializable(Serializable):
    text: str
    emoji: str


@ray.remote
def r_write_file(fr: FileRef, contents: Serializable, num_attempts: int) -> bool:
    return fr.write_json(contents, num_attempts=num_attempts)


def write_all_files(num_files: int, num_attempts: int = 10) -> int:
    """Returns the number of failures."""
    file_refs = [
        make_file_ref(file_name=f"file{f:03d}", root_dir="write_output")
        for f in range(0, num_files)
    ]

    if ray.is_initialized():
        # Engineering note: we could skip the call to ray.get here, launching all
        # these write calls asynchronously without bothering to wait for them
        # to complete, but this seems to break the calls to wait_for_zero_pending_writes().
        # Perhaps this is a consequence of having no pending writes that have even
        # *started* by the time control flow gets there. Our workaround here is to
        # wait for all the remote writes to complete. We'll need a separate test to
        # really exercise the concurrency aspects of wait_for_zero_pending_writes().

        results = ray.get(
            [
                r_write_file.remote(
                    fr, TestSerializable(fr.file_name, poop_emoji), num_attempts
                )
                for fr in file_refs
            ]
        )
    else:
        results = [
            fr.write_json(
                TestSerializable(fr.file_name, poop_emoji), num_attempts=num_attempts
            )
            for fr in file_refs
        ]

    # True = success, False = failure, so we want to sum up the failures
    return sum([not x for x in results])


def verify_all_files(num_files: int) -> bool:
    failure = False
    for f in range(0, num_files):
        name = f"file{f:03d}"
        fr = make_file_ref(name, "write_output")
        expected_contents = TestSerializable(name, poop_emoji)
        expected_json = expected_contents.to_json()
        contents = fr.read_json(
            TestSerializable, expected_sha256_hash=sha256_hash(expected_json)
        )
        length = fr.size()
        exists = fr.file_exists()
        if not exists or contents is None:
            print(f"file {name} is missing!")
            failure = True
            continue
        if contents != expected_contents:
            print(f"file {name} has wrong contents: {contents}!")
            failure = True
            continue
        if len(contents.to_json().encode("utf-8")) != length:
            print(f"file {name} has wrong length: {length}!")
            failure = True
            continue

    return not failure


def cleanup_between_tests() -> None:
    if ray.is_initialized():
        ray.shutdown()
        reset_status_actor()

    try:
        shutil.rmtree("write_output", ignore_errors=True)
    except FileNotFoundError:
        # okay if it's not there
        pass


class TestBasicReadsAndWrites(unittest.TestCase):
    def setUp(self) -> None:
        cleanup_between_tests()

    def tearDown(self) -> None:
        cleanup_between_tests()

    def test_file_sizes(self) -> None:
        fr = make_file_ref("testfile", root_dir="write_output")
        fr.write("123456789")
        self.assertEqual(9, fr.size())
        self.assertTrue(fr.file_exists())
        fr.unlink()
        self.assertFalse(fr.file_exists())

        self.assertEqual(0, make_file_ref("testfile2", root_dir="write_output").size())
        self.assertEqual(0, make_file_ref("", root_dir="write_output").size())
        self.assertEqual(0, make_file_ref("", root_dir="write_output2").size())
        cleanup_between_tests()

    def test_basics(self) -> None:
        self.assertEquals(0, write_all_files(10))
        self.assertTrue(verify_all_files(10))

        dir_info = make_file_ref("", "write_output").scandir()
        self.assertEqual(10, len(dir_info.files))
        self.assertEqual(0, len(dir_info.subdirs))
        cleanup_between_tests()

    def test_hash_verification(self) -> None:
        fr = make_file_ref("test1", "write_output")
        fr.write("test contents")
        self.assertEqual("test contents", fr.read())
        self.assertEqual(
            "test contents", fr.read(expected_sha256_hash=sha256_hash("test contents"))
        )
        self.assertIsNone(fr.read(expected_sha256_hash=sha256_hash("wrong contents")))
        cleanup_between_tests()

    def test_make_file_ref_from_path_local(self) -> None:
        f0 = make_file_ref_from_path("foo/bar.txt")
        self.assertEqual("file:./foo/bar.txt", str(f0))
        self.assertTrue(f0.is_file())
        self.assertTrue(f0.is_local())

        f1 = make_file_ref_from_path("foo.txt")
        self.assertEqual("file:./foo.txt", str(f1))
        self.assertTrue(f1.is_file())
        self.assertTrue(f1.is_local())

        f2 = make_file_ref_from_path("./foo.txt")
        self.assertEqual("file:./foo.txt", str(f2))
        self.assertTrue(f2.is_file())
        self.assertTrue(f2.is_local())

        f3 = make_file_ref_from_path("foo/bar/baz.txt")
        self.assertEqual("file:./foo/bar/baz.txt", str(f3))
        self.assertEqual(".", f3.root_dir)
        self.assertEqual(["foo", "bar"], f3.subdirectories)
        self.assertEqual("baz.txt", f3.file_name)
        self.assertTrue(f3.is_file())
        self.assertTrue(f3.is_local())

        f4 = make_file_ref_from_path("/foo/bar/baz.txt")
        self.assertEqual("file:/foo/bar/baz.txt", str(f4))
        self.assertEqual("/", f4.root_dir)
        self.assertEqual(["foo", "bar"], f4.subdirectories)
        self.assertEqual("baz.txt", f4.file_name)
        self.assertTrue(f4.is_file())
        self.assertTrue(f4.is_local())

        f5 = make_file_ref_from_path("/")
        self.assertEqual("file:/", str(f5))
        self.assertEqual("/", f5.root_dir)
        self.assertEqual([], f5.subdirectories)
        self.assertEqual("", f5.file_name)
        self.assertTrue(f5.is_dir())
        self.assertTrue(f5.is_local())

        f6 = make_file_ref_from_path(".")
        self.assertEqual("file:./", str(f6))
        self.assertEqual(".", f6.root_dir)
        self.assertEqual([], f6.subdirectories)
        self.assertEqual("", f6.file_name)
        self.assertTrue(f6.is_dir())
        self.assertTrue(f6.is_local())

        f7 = make_file_ref_from_path("../../")
        # we're using .. because it will succeed the internal is_dir checks
        self.assertEqual("file:./../../", str(f7))
        self.assertEqual(".", f7.root_dir)
        self.assertEqual(["..", ".."], f7.subdirectories)
        self.assertEqual("", f7.file_name)
        self.assertTrue(f7.is_dir())
        self.assertTrue(f7.is_local())

    def test_make_file_ref_from_path_s3(self) -> None:
        f0 = make_file_ref_from_path("s3://foo/bar.txt")
        self.assertEqual("s3://foo/bar.txt", str(f0))
        self.assertEqual("foo", f0.s3_bucket())
        self.assertEqual("bar.txt", f0.s3_key_name())
        self.assertTrue(f0.is_file())
        self.assertFalse(f0.is_local())

        f1 = make_file_ref_from_path("s3://foo/bar/baz.txt")
        self.assertEqual("s3://foo/bar/baz.txt", str(f1))
        self.assertEqual("foo", f1.root_dir)
        self.assertEqual(["bar"], f1.subdirectories)
        self.assertEqual("baz.txt", f1.file_name)
        self.assertEqual("foo", f1.s3_bucket())
        self.assertEqual("bar/baz.txt", f1.s3_key_name())
        self.assertTrue(f1.is_file())
        self.assertFalse(f1.is_local())

        d1 = make_file_ref_from_path("s3://foo/bar/baz/")
        self.assertEqual("s3://foo/bar/baz/", str(d1))
        self.assertEqual("foo", d1.root_dir)
        self.assertEqual(["bar", "baz"], d1.subdirectories)
        self.assertEqual("", d1.file_name)
        self.assertEqual("foo", d1.s3_bucket())
        self.assertEqual("bar/baz/", d1.s3_key_name())
        self.assertTrue(d1.is_dir())
        self.assertFalse(d1.is_local())

        d2 = make_file_ref_from_path("s3://foo")
        self.assertEqual("s3://foo/", str(d2))
        self.assertEqual("foo", d2.root_dir)
        self.assertEqual([], d2.subdirectories)
        self.assertEqual("", d2.file_name)
        self.assertEqual("foo", d2.s3_bucket())
        self.assertEqual("/", d2.s3_key_name())
        self.assertTrue(d2.is_dir())
        self.assertFalse(d2.is_local())


class TestRayWriteRetry(unittest.TestCase):
    def setUp(self) -> None:
        cleanup_between_tests()

    def tearDown(self) -> None:
        cleanup_between_tests()

    def test_zero_failures(self) -> None:
        cleanup_between_tests()
        ray_init_localhost(num_cpus=cpu_count())
        set_failure_probability_for_testing(0.0)

        self.assertEqual(0, write_all_files(10))
        num_failures = wait_for_zero_pending_writes()
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(10))
        cleanup_between_tests()

    def test_huge_failures(self) -> None:
        cleanup_between_tests()
        ray_init_localhost(num_cpus=cpu_count())
        set_failure_probability_for_testing(0.2)

        # up to 100 retries, driving odds of total failure to zero
        self.assertEqual(0, write_all_files(100, 100))
        num_failures = wait_for_zero_pending_writes()

        # our retries should guarantee everything succeeds!
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(100))
        cleanup_between_tests()

    def test_without_ray(self) -> None:
        cleanup_between_tests()
        set_failure_probability_for_testing(0.2)

        # up to 100 retries, driving odds of total failure to zero
        self.assertEquals(0, write_all_files(20, 100))
        num_failures = wait_for_zero_pending_writes()

        # our retries should guarantee everything succeeds!
        self.assertEqual(num_failures, 0)
        self.assertTrue(verify_all_files(20))
        cleanup_between_tests()
