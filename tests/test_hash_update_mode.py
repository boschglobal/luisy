# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import luisy
import tempfile
import os
import json
import pickle

from luisy.testing import (
    create_testing_config,
    LuisyTestCase,
)
from luisy import Config
from luisy.cli import build


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


@luisy.raw
class SomeTask(luisy.Task):
    def run(self):
        self.write(['some_output'])

    def get_file_name(self):
        return 'EXT'


@luisy.interim
@luisy.requires(SomeTask)
class SomeInterimTask(luisy.Task):

    def get_file_name(self):
        return 'ABC'

    def run(self):
        print('OneTask')
        self.write(['some_output'])


@luisy.interim
@luisy.requires(SomeTask)
class SomeInterimTask2(luisy.Task):

    def get_file_name(self):
        return 'ABC'

    def run(self):
        print('DifferentTask')
        self.write(['some_other_output'])


class TestHashUpdate(LuisyTestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        create_testing_config(working_dir=self.tmpdir.name)

        self.requirements_file = os.path.join(self.tmpdir.name, 'requirements.txt')
        with open(self.requirements_file, 'w') as f:
            f.write('luisy==1.2.3\n')
            f.write('luigi==3.2.1\n')
            f.write('pandas==100.0.0\n')

    def tearDown(self):
        self.tmpdir.cleanup()

    def prepare_cloud(self, files):
        self.fs = AzureContainerMock()

        self.fs.hashes = {
            '/tests/interim/ABC.pkl': 'hash0',
            '/tests/raw/EXT.pkl': 'hash1'}
        for filename in files:
            self.fs.files[filename] = {filename: "data"}

        create_testing_config(working_dir=self.tmpdir.name)
        Config().fs = self.fs

    def assert_hash_changes(self, hash_1, hash_2):
        self.assertEqual(hash_1.keys(), hash_2.keys())
        # hash changed
        self.assertNotEqual(
            hash_2['/tests/interim/ABC.pkl'],
            hash_1['/tests/interim/ABC.pkl']
        )

    def load_hashes(self):
        return json.load(open(
            os.path.join(
                self.tmpdir.name,
                'tests/.luisy.hash'),
            'r')
        )

    def test_force_hash_update(self):
        task_path = os.path.join(self.tmpdir.name, 'tests/interim/ABC.pkl')
        build(
            task=SomeInterimTask(),
            working_dir=self.tmpdir.name,
            requirements_path=self.requirements_file
        )
        hashes_initial = self.load_hashes()
        task_output_initial = pickle.load(open(task_path, 'rb'))
        self.assertEqual(len(hashes_initial), 2)

        # Run SomeTask2 with same filename but different code -> different hash, force-hash-update
        # activated
        build(
            task=SomeInterimTask2(),
            working_dir=self.tmpdir.name,
            requirements_path=self.requirements_file,
            hash_update_mode=True
        )
        hashes_new = self.load_hashes()
        task_output_new = pickle.load(
            open(task_path, 'rb'))

        self.assertEqual(len(hashes_new), 2)
        self.assert_hash_changes(hashes_initial, hashes_new)
        # task did not change
        self.assertEqual(task_output_initial, task_output_new)

        build(
            task=SomeInterimTask(),
            working_dir=self.tmpdir.name,
            requirements_path=self.requirements_file
        )
        hashes_new = self.load_hashes()
        self.assertEqual(
            hashes_new['/tests/interim/ABC.pkl'],
            hashes_initial['/tests/interim/ABC.pkl']
        )

    def test_uploading(self):
        self.prepare_cloud(files=[])
        build(
            task=SomeInterimTask(),
            working_dir=self.tmpdir.name,
            requirements_path=self.requirements_file,
            upload=True,
            hash_update_mode=True
        )
        self.assertNotEqual(self.fs.hashes['/tests/interim/ABC.pkl'], 'hash0')
        self.assertNotEqual(self.fs.hashes['/tests/raw/EXT.pkl'], 'hash1')
        hashes_old = self.fs.hashes
        build(
            task=SomeInterimTask2(),
            working_dir=self.tmpdir.name,
            requirements_path=self.requirements_file,
            upload=True,
            hash_update_mode=True
        )
        self.assertNotEqual(
            self.fs.hashes['/tests/interim/ABC.pkl'],
            hashes_old['/tests/interim/ABC.pkl']
        )
        self.assertEqual(
            self.fs.hashes['/tests/raw/EXT.pkl'],
            hashes_old['/tests/raw/EXT.pkl']
        )
