# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import patch
from luisy.testing import create_testing_config
import json
import tempfile
import os
import pickle

from luisy.hashes import HashMapping
from luisy import (
    Config,
    Task,
    ExternalTask,
    requires,
    WrapperTask,
)
from luisy.testing import create_file
from luisy.cli import build
from luisy.decorators import (
    raw,
    interim,
    final,
    project_name,
)


@project_name('my_project')
@raw
class ExternalA(ExternalTask):
    pass


@project_name('my_project')
@raw
class ExternalB(ExternalTask):
    pass


@project_name('my_project')
@interim
@requires(ExternalA)
class ToyTaskA(Task):

    def run(self):
        data = self.input().read()
        data['ToyTaskA'] = 'run_locally'
        self.write(data)


@project_name('my_project')
@interim
@requires(ExternalB)
class ToyTaskB(Task):

    def run(self):
        data = self.input().read()
        data['ToyTaskB'] = "run_locally"
        self.write(data)


@project_name("my_project")
@final
@requires(ToyTaskA, ToyTaskB, as_dict=True)
class FinalTask(Task):

    def run(self):
        self.write(
            {
                'A': self.input()['ToyTaskA'].read(),
                'B': self.input()['ToyTaskB'].read(),
            }
        )


@project_name("my_project")
class ToyWrapperTask(WrapperTask):
    def requires(self):
        return {
            "ToyTaskA": ToyTaskA(),
            "ToyTaskB": ToyTaskB(),
        }


class AzureContainerMock(object):
    files = {
        '/my_project/.luisy.hash': {}
    }

    def __init__(self, *args, **kwargs):
        pass

    def exists(self, path):
        return path in self.files

    def _download_file(self, source, dest):
        if source in self.files:
            os.makedirs(dest.replace(os.path.basename(dest), ''), exist_ok=True)
            with open(dest, 'wb') as f:
                pickle.dump(self.files[source], f)

    def _upload_file(self, source, dest, overwrite=True):
        with open(source, 'rb') as f:
            data = pickle.load(f)
        self.files[dest] = data

    def load_json(self, _):
        return self.hashes

    def save_json(self, _, data):
        self.hashes = data


class TestWithSync(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

        self.hashes = {
            '/my_project/raw/ExternalA.pkl': 'raw_A',
            '/my_project/raw/ExternalB.pkl': 'raw_B',
            '/my_project/interim/ToyTaskA.pkl': 'interim_A',
            '/my_project/interim/ToyTaskB.pkl': 'interim_B',
            '/my_project/final/FinalTask.pkl': 'final',
        }
        create_testing_config(working_dir=self.tmpdir.name)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def get_hashes(self, files):
        return {
            f: self.hashes[f] for f in files
        }

    def prepare_cloud(self, files):
        self.fs = AzureContainerMock()

        self.fs.hashes = self.get_hashes(files)
        for filename in files:
            self.fs.files[filename] = {filename: "data"}

        Config().fs = self.fs

    @patch("luisy.hashes.compute_hashes")
    def test_downloading(self, compute_hashes):
        compute_hashes.return_value = self.get_hashes(
            [
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
            ]
        )
        self.prepare_cloud(['/my_project/raw/ExternalA.pkl'])

        task = ToyTaskA()
        coordinator = build(task=task, download=True)

        self.assertIn(task, coordinator.tasks_set['completed'])
        self.assertIn(ExternalA(), coordinator.tasks_set['already_done'])

        self.assertTrue(os.path.exists(task.requires().output().path))

        self.assertTrue(set({'ToyTaskA': "run_locally"}.items()).issubset(
            set(task.read().items())))

    @patch("luisy.hashes.compute_hashes")
    def test_uploading(self, compute_hashes):
        compute_hashes.return_value = self.get_hashes(
            [
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
            ]
        )
        self.prepare_cloud(['/my_project/raw/ExternalA.pkl'])

        task = ToyTaskA()
        build(task=task, download=True, upload=True)

        # Assert that hashes have been uploaded
        self.assertDictEqual(self.fs.hashes, compute_hashes.return_value)

        self.assertIn('/my_project/interim/ToyTaskA.pkl', self.fs.files)

        self.assertTrue(set({'ToyTaskA': "run_locally"}.items()).issubset(
            set(self.fs.files['/my_project/interim/ToyTaskA.pkl'].items())))

    @patch("luisy.hashes.compute_hashes")
    def test_upload_without_download(self, compute_hashes):
        compute_hashes.return_value = self.get_hashes(
            [
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
            ]
        )
        self.prepare_cloud([
            '/my_project/raw/ExternalA.pkl',
        ])

        create_file(ExternalA().output().path, val={})
        task = ToyTaskA()
        build(task=task, upload=True)

        self.assertIn('/my_project/interim/ToyTaskA.pkl', self.fs.files)

    @patch("luisy.hashes.HashSynchronizer._initialize_hashmappings", autospec=True)
    def test_leave_other_files_untouched_in_upload(self, compute_hashes):
        def initialize_mappings(self):
            self.hashmapping_local = HashMapping(
                hashes={
                    'A': 'a'
                },
                local=True,
                project_name='my_project'
            )
            self.hashmapping_new = HashMapping(
                hashes={
                    '/my_project/interim/ToyTaskA.pkl': 'hash_1',
                },
                local=True,
                project_name='my_project'
            )
            self.hashmapping_cloud = HashMapping(
                hashes={
                    '/my_project/interim/ToyTaskA.pkl': 'hash_2',
                    '/my_project/final/FinalTask.pkl': 'hash_3'
                },
                local=False,
                project_name='my_project'
            )
        self.prepare_cloud(['/my_project/final/FinalTask.pkl'])
        compute_hashes.side_effect = initialize_mappings
        create_file(ExternalA().output().path, val={})

        task = ToyTaskA()
        coordinator = build(task=task, upload=True)

        self.assertListEqual(
            [hash_entry.filename for hash_entry in
             coordinator.hash_synchronizer.get_upload_hash_entries()],
            ['/my_project/interim/ToyTaskA.pkl']
        )

        self.assertIn('/my_project/interim/ToyTaskA.pkl', self.fs.files)
        self.assertIn('/my_project/final/FinalTask.pkl', self.fs.files)

    @patch("luisy.hashes.compute_hashes")
    def test_uploading_wrapper_task(self, compute_hashes):
        compute_hashes.return_value = self.get_hashes(
            [
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
                '/my_project/raw/ExternalB.pkl',
                '/my_project/interim/ToyTaskB.pkl',
            ]
        )
        self.prepare_cloud(['/my_project/raw/ExternalA.pkl', '/my_project/raw/ExternalB.pkl'])

        # task = ToyTaskA()
        task = ToyWrapperTask()
        build(task=task, download=True, upload=True)

        # Assert that hashes have been uploaded
        self.assertDictEqual(self.fs.hashes, compute_hashes.return_value)

        self.assertIn('/my_project/interim/ToyTaskA.pkl', self.fs.files)
        self.assertIn('/my_project/interim/ToyTaskB.pkl', self.fs.files)

        self.assertTrue(set({'ToyTaskA': "run_locally"}.items()).issubset(
            set(self.fs.files['/my_project/interim/ToyTaskA.pkl'].items())))
        self.assertTrue(set({'ToyTaskB': "run_locally"}.items()).issubset(
            set(self.fs.files['/my_project/interim/ToyTaskB.pkl'].items())))

    @patch("luisy.hashes.compute_hashes")
    def test_downloading_wrapper_task(self, compute_hashes):
        compute_hashes.return_value = self.get_hashes(
            [
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
                '/my_project/raw/ExternalB.pkl',
                '/my_project/interim/ToyTaskB.pkl',
            ]
        )
        self.prepare_cloud(['/my_project/interim/ToyTaskA.pkl', '/my_project/interim/ToyTaskB.pkl'])

        task = ToyWrapperTask()
        coordinator = build(task=task, download=True)

        self.assertIn(ToyWrapperTask(), coordinator.tasks_set['already_done'])

        self.assertIn('/my_project/interim/ToyTaskA.pkl', self.fs.files)
        self.assertIn('/my_project/interim/ToyTaskB.pkl', self.fs.files)

    @patch("luisy.hashes.compute_hashes")
    def test_that_hashes_of_untouched_tasks_stay_untouched(self, compute_hashes):
        compute_hashes.return_value = self.get_hashes(
            [
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
            ]
        )

        # Upload more files to the cloud than needed for the run
        self.prepare_cloud(
            files=['/my_project/raw/ExternalA.pkl', '/my_project/raw/ExternalB.pkl'],
        )
        build(task=ToyTaskA(), download=True, upload=True)

        # Make sure file is still up
        self.assertIn('/my_project/raw/ExternalB.pkl', self.fs.files)
        # Make sure hash is still there
        self.assertIn('/my_project/raw/ExternalB.pkl', self.fs.hashes)

    @patch("luisy.hashes.compute_hashes")
    def test_upload_overwrite(self, compute_hashes):
        compute_hashes.return_value = self.get_hashes(
            [
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
            ]
        )

        self.prepare_cloud(
            files=['/my_project/raw/ExternalA.pkl', '/my_project/interim/ToyTaskA.pkl'],
        )
        self.fs.hashes['/my_project/interim/ToyTaskA.pkl'] = 'some_outdated_hash'
        build(task=ToyTaskA(), download=True, upload=True)

        # Assert that hash in cloud is updated
        self.assertEqual(self.fs.hashes['/my_project/interim/ToyTaskA.pkl'], 'interim_A')

        # Make sure that locally executed task is uploaded
        self.assertTrue(set({'ToyTaskA': "run_locally"}.items()).issubset(
            set(self.fs.files['/my_project/interim/ToyTaskA.pkl'].items())))

    @patch("luisy.hashes.compute_hashes")
    def test_that_files_with_old_hash_are_overwritten_locally(self, compute_hashes):
        compute_hashes.return_value = self.get_hashes(
            [
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
            ]
        )

        # Upload more files to the cloud than needed for the run
        self.prepare_cloud(
            files=[
                '/my_project/raw/ExternalA.pkl',
                '/my_project/interim/ToyTaskA.pkl',
            ],
        )
        build(task=ToyTaskA(), download=True)

        # Change content of the file in the cloud
        data = {'data': 'some_new_content'}
        self.fs.files['/my_project/interim/ToyTaskA.pkl'] = data

        # Change true hashes and cloud hashes
        compute_hashes.return_value = {
            '/my_project/interim/ToyTaskA.pkl': 'new_hash_for_a'
        }
        self.fs.hashes['/my_project/interim/ToyTaskA.pkl'] = 'new_hash_for_a'

        # Rerun with new cloud content
        build(task=ToyTaskA(), download=True)

        # Check that file has been downloaded and not executed locally
        self.assertDictEqual(
            ToyTaskA().read(),
            data
        )

        # Check that hash has been updated
        hash_file = HashMapping.get_local_hash_path("my_project")
        with open(hash_file, 'r') as f:
            hashes = json.load(f)

        self.assertEqual(
            hashes['/my_project/interim/ToyTaskA.pkl'],
            'new_hash_for_a',
        )
