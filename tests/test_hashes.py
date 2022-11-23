# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import os
import luigi
import tempfile
from unittest.mock import patch

import luisy
from luisy.hashes import (
    HashMapping,
    get_upstream_tasks,
    TaskHashEntry,
    HashSynchronizer,
)
from luisy.config import (
    remove_working_dir,
    Config
)

from luisy.testing import create_testing_config

from luisy.code_inspection import (
    get_requirements_path,
    get_requirements_dict,
)
from luisy.testing import create_file


@luisy.raw
class ToyLowestTask(luisy.Task):
    def run(self):
        self.write({'A': 2})


@luisy.interim
@luisy.requires(ToyLowestTask)
class ToyTask(luisy.Task):
    a = luigi.IntParameter()
    b = luigi.IntParameter(default=2)

    def run(self):
        data = {'A': self.a, 'B': self.b}
        self.write(data)


class TestHashMapping(unittest.TestCase):

    def setUp(self):
        self.hashes = {
            '/some/file/a.csv': "hash_a",
            '/some/file/b.csv': "hash_b",
            '/some/file/c.csv': "hash_c",
        }

        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_constructor(self):

        mapping = HashMapping(
            project_name="some",
            hashes=self.hashes,
        )
        self.assertDictEqual(
            self.hashes,
            mapping.hashes)

    def test_clean_up(self):

        def exists(self, filepath):

            if 'a.csv' in filepath:
                return True
            else:
                return False

        with patch("luisy.hashes.HashMapping.exists", exists):

            mapping = HashMapping(
                project_name="some",
                hashes=self.hashes,
            )
            mapping.clean_up()
            self.assertDictEqual(
                {
                    '/some/file/a.csv': "hash_a",
                },
                mapping.hashes
            )

    def test_compute_from_tasks(self):
        req_path = get_requirements_path(ToyTask)
        return_value = get_requirements_dict(req_path)
        return_value["luisy"] = "luisy==42.7"

        with patch("luisy.code_inspection.get_requirements_dict", return_value=return_value):
            task = ToyTask(a=2)
            tasks = get_upstream_tasks(task)
            mapping = HashMapping.compute_from_tasks(tasks, "some_project")
            self.assertIn('/tests/interim/ToyTask_a=2_b=2.pkl', mapping.hashes)

    def test_local_save(self):
        mapping = HashMapping(
            project_name="my_project",
            hashes=self.hashes,
            local=True,
        )
        mapping.save()
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir.name, 'my_project', '.luisy.hash')))

    def test_compute_from_task_same_class(self):
        req_path = get_requirements_path(ToyTask)
        return_value = get_requirements_dict(req_path)
        return_value["luisy"] = "luisy==42.7"

        with patch("luisy.code_inspection.get_requirements_dict", return_value=return_value):
            tasks = get_upstream_tasks(ToyTask(a=2))
            mapping = HashMapping.compute_from_tasks(tasks, "some_project")
            other_tasks = get_upstream_tasks(ToyTask(a=1))
            other_mapping = HashMapping.compute_from_tasks(other_tasks, 'some_project')
            self.assertEqual(
                mapping.hashes['/tests/interim/ToyTask_a=2_b=2.pkl'],
                other_mapping.hashes['/tests/interim/ToyTask_a=1_b=2.pkl']
            )

    def test_compute_from_task_duplicate_class(self):
        req_path = get_requirements_path(ToyTask)
        return_value = get_requirements_dict(req_path)
        return_value["luisy"] = "luisy==42.7"

        with patch("luisy.code_inspection.get_requirements_dict", return_value=return_value):
            # One mapping for both
            tasks_a = get_upstream_tasks(ToyTask(a=2))
            tasks_b = get_upstream_tasks(ToyTask(a=1))

            all_tasks = {}
            all_tasks.update(tasks_a)
            all_tasks.update(tasks_b)

            mapping = HashMapping.compute_from_tasks(all_tasks, "some_project")

            unique_hashes = set(mapping.hashes.values())
            unique_files = set(mapping.hashes.keys())

            # ToyLowestTask is the same -> ToyLowestTask() + ToyTask(a=2) + ToyTask(a=1)
            self.assertEqual(len(mapping.hashes), 3)
            # dont hash ToyTask twice -> ToyLowestTask + ToyTask
            self.assertEqual(len(unique_hashes), 2)
            self.assertEqual(len(unique_files), 3)


class TestTaskHashEntry(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)
        self.task = ToyTask(a=1)
        self.entry_different = TaskHashEntry(
            hash_new='new',
            hash_local='local',
            hash_cloud='cloud',
            task=self.task,
            filename=self.task.output().path
        )
        self.entry_same = TaskHashEntry(
            hash_new='hash',
            hash_local='hash',
            hash_cloud='hash',
            task=self.task,
            filename=self.task.output().path
        )
        self.entry_missing = TaskHashEntry(
            hash_new='hash',
            hash_local=None,
            hash_cloud=None,
            task=self.task,
            filename=self.task.output().path
        )

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_in_cloud(self):
        self.assertTrue(self.entry_different.in_cloud())
        self.assertFalse(self.entry_missing.in_cloud())

    def test_has_outdated_cloud_hash(self):
        self.assertTrue(self.entry_different.has_outdated_cloud_hash())
        self.assertFalse(self.entry_same.has_outdated_cloud_hash())

    def test_in_local(self):
        self.assertTrue(self.entry_different.in_local())
        self.assertFalse(self.entry_missing.in_local())

    def test_is_downloadable(self):
        self.assertFalse(self.entry_different.is_downloadable())
        self.assertTrue(self.entry_same.is_downloadable())

    def test_has_outdated_local_hash(self):
        self.assertTrue(self.entry_different.has_outdated_local_hash())
        self.assertFalse(self.entry_same.has_outdated_local_hash())

    def test_needs_local_execution(self):
        needs_local_execution = TaskHashEntry(
            hash_new='new',
            hash_local=None,
            hash_cloud='outdated',
            task=self.task,
            filename=self.task.output().path
        )
        self.assertTrue(needs_local_execution.needs_local_execution())
        self.assertFalse(self.entry_same.needs_local_execution())

    def test_needs_to_be_downloaded(self):
        needs_download = TaskHashEntry(
            hash_new='new',
            hash_local=None,
            hash_cloud='new',
            task=self.task,
            filename=self.task.output().path
        )
        self.assertTrue(needs_download.needs_to_be_downloaded())


@patch("luisy.hashes.HashSynchronizer._initialize_hashmappings", autospec=True)
class TestHashSynchronization(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)
        self.task = ToyTask(a=1)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def get_hashes(self, files):
        return {
            f: 'data' for f in files
        }

    def mock_initialize_hashmapping(self, func, hashes_new, hashes_local=None,
                                    hashes_cloud=None):
        def initialize(self):
            self.hashmapping_new = HashMapping(hashes_new)
            self.hashmapping_local = HashMapping(hashes=hashes_local)
            self.hashmapping_cloud = HashMapping(hashes=hashes_cloud)
            self.hashmapping_cloud.save = lambda *args: None
            self.hashmapping_local.save = lambda *args: None

        func.side_effect = initialize
        hash_sync = HashSynchronizer(
            tasks={remove_working_dir(self.task.output().path): self.task},
            root_task=self.task
        )
        hash_sync.initialize()
        return hash_sync

    def test_initialization(self, _initialize_hashmappings):
        hash_sync = self.mock_initialize_hashmapping(
            hashes_new={'/tests/interim/ToyTask_a=1_b=2.pkl': 'A'},
            hashes_local={'/tests/interim/ToyTask_a=1_b=2.pkl': 'A', 'b': 'B'},
            hashes_cloud={'c': 'C'},
            func=_initialize_hashmappings
        )
        self.assertEqual(len(hash_sync.hash_entries), 1)
        self.assertEqual(hash_sync.hash_entries[0].filename, '/tests/interim/ToyTask_a=1_b=2.pkl')
        self.assertEqual(hash_sync.hash_entries[0].hash_local, 'A')
        self.assertEqual(hash_sync.hash_entries[0].hash_cloud, None)

    def test_clear_outdated_tasks(self, _initialize_hashmappings):
        create_file(self.task.output().path, {})
        hash_sync = self.mock_initialize_hashmapping(
            hashes_new={'/tests/interim/ToyTask_a=1_b=2.pkl': 'new'},
            hashes_local={'/tests/interim/ToyTask_a=1_b=2.pkl': 'old'},
            hashes_cloud={},
            func=_initialize_hashmappings
        )
        self.assertTrue(os.path.exists(self.task.output().path))
        hash_sync.clear_outdated_tasks()
        self.assertFalse(os.path.exists(self.task.output().path))

    def test_files_to_download(self, _initialize_hashmappings):
        hash_sync = self.mock_initialize_hashmapping(
            hashes_new={'/tests/interim/ToyTask_a=1_b=2.pkl': 'new'},
            hashes_local={'/tests/interim/ToyTask_a=1_b=2.pkl': 'old'},
            hashes_cloud={'/tests/interim/ToyTask_a=1_b=2.pkl': 'new'},
            func=_initialize_hashmappings
        )
        hash_sync.set_files_to_download()
        self.assertListEqual(
            [remove_working_dir(file) for file in Config()._files_to_download],
            ['/tests/interim/ToyTask_a=1_b=2.pkl']
        )

    def test_synchronize_local_hashes(self, _initialize_hashmappings):
        hash_sync = self.mock_initialize_hashmapping(
            hashes_new={'/tests/interim/ToyTask_a=1_b=2.pkl': 'new'},
            hashes_local={'/tests/interim/ToyTask_a=1_b=2.pkl': 'old', 'another_task': 'new'},
            hashes_cloud={'/tests/interim/ToyTask_a=1_b=2.pkl': 'new', 'another_task': 'new'},
            func=_initialize_hashmappings
        )
        hash_sync.synchronize_local_hashes(failed_tasks=[self.task])
        self.assertEqual(
            hash_sync.hashmapping_local.hashes['/tests/interim/ToyTask_a=1_b=2.pkl'],
            'old'
        )
        hash_sync.synchronize_local_hashes(failed_tasks=[])
        self.assertEqual(
            hash_sync.hashmapping_local.hashes['/tests/interim/ToyTask_a=1_b=2.pkl'],
            'new'
        )

    def test_synchronize_cloud_hashes(self, _initialize_hashmappings):
        hash_sync = self.mock_initialize_hashmapping(
            hashes_new={'/tests/interim/ToyTask_a=1_b=2.pkl': 'new'},
            hashes_local={'/tests/interim/ToyTask_a=1_b=2.pkl': 'new', 'another_task': 'new'},
            hashes_cloud={'/tests/interim/ToyTask_a=1_b=2.pkl': 'old', 'another_task': 'new'},
            func=_initialize_hashmappings
        )
        with patch("luisy.tasks.base.Task.upload") as upload:
            upload.return_value = None
            hash_sync.synchronize_cloud_hashes(failed_tasks=[self.task])
            self.assertEqual(
                hash_sync.hashmapping_cloud.hashes['/tests/interim/ToyTask_a=1_b=2.pkl'],
                'old'
            )
            hash_sync.synchronize_cloud_hashes(failed_tasks=[])
            self.assertEqual(
                hash_sync.hashmapping_cloud.hashes['/tests/interim/ToyTask_a=1_b=2.pkl'],
                'new'
            )
